package scanner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"s3-dedup/internal/cache"
	"s3-dedup/internal/config"
	"s3-dedup/internal/hashing"
	"s3-dedup/internal/pointer"
	"s3-dedup/internal/report"
	"strings"
	"sync"
	"sync/atomic"

	//"s3-dedup/internal/s3"
	"strconv"
	"time"

	"github.com/minio/minio-go/v6"
)

type S3Client interface {
	ListObjects(
		ctx context.Context,
		bucket string,
		prefix string,
		recursive bool,
		fn func(minio.ObjectInfo) error,
	) error

	GetObject(
		ctx context.Context,
		bucket string,
		key string,
	) (io.ReadCloser, error)

	StatObject(
		ctx context.Context,
		bucket string,
		objectName string,
	) (minio.ObjectInfo, error)

	PutObject(
		ctx context.Context,
		bucket string,
		objectName string,
		reader io.Reader,
		size int64,
		contentType string,
	) (int64, error)

	RemoveObjects(
		ctx context.Context,
		bucket string,
		keys []string,
	) ([]string, error)
}

type Scanner struct {
	s3Client S3Client
	store    cache.Store
	config   *config.Config
}

type objectJob struct {
	buket  string
	info   minio.ObjectInfo
	scanID string
}

func NewScanner(s3Client S3Client, store cache.Store, config *config.Config) *Scanner {
	return &Scanner{
		s3Client: s3Client,
		store:    store,
		config:   config,
	}
}

func (s *Scanner) ScanOnce(ctx context.Context) (scanReport report.Report, resErr error) {
	scanReport.ScanStarted = time.Now().UTC()

	scanReport.Mode = s.config.Dedup.Mode
	scanID := strconv.FormatInt(scanReport.ScanStarted.UnixNano(), 10)
	workers := s.config.Schedule.Workers
	if workers <= 0 {
		workers = 1
	}
	if workers > int64(runtime.NumCPU()*2) {
		workers = int64(runtime.NumCPU() * 2)
	}

	var objectsScanned atomic.Int64
	var processErrors atomic.Int64
	var objectsRelinked atomic.Int64
	var bytesReclaimed atomic.Int64

	defer func() {
		scanReport.ScanFinished = time.Now().UTC()
		scanReport.ObjectsScanned = objectsScanned.Load()
		scanReport.Errors += processErrors.Load()
		scanReport.ObjectsRelinked = objectsRelinked.Load()
		scanReport.BytesReclaimed = bytesReclaimed.Load()
	}()

	for _, bucket := range s.config.S3.Buckets {
		jobs := make(chan objectJob, workers)
		var wg sync.WaitGroup
		for i := 0; i < int(workers); i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for job := range jobs {
					var processErr error
					switch s.config.Dedup.Mode {
					case "report_only":
						processErr = s.processObject(ctx, job.buket, job.info, job.scanID)
					case "pointer":
						var reclaimed int64
						var relinked bool
						reclaimed, relinked, processErr = s.processObjectPointer(ctx, job.buket, job.info, job.scanID)

						if s.config.Dedup.DeleteOriginals && processErr == nil {
							if relinked {
								objectsRelinked.Add(1)
							}
							if reclaimed > 0 {
								bytesReclaimed.Add(reclaimed)
							}
						}
					default:
						processErr = fmt.Errorf("Mode %q is not supported", s.config.Dedup.Mode)
					}
					if processErr != nil {
						processErrors.Add(1)
						fmt.Printf("Processing object %s/%s: %v\n", job.buket, job.info.Key, processErr)
					}
				}
			}()
		}
		err := s.s3Client.ListObjects(ctx, bucket.Name, bucket.Prefix, true,
			func(info minio.ObjectInfo) error {
				if strings.HasPrefix(info.Key, s.config.Dedup.BlobPrefix) {
					return nil
				}
				err := s.store.MarkObjectSeen(ctx, bucket.Name, info.Key, scanID)
				if err != nil {
					processErrors.Add(1)
					return fmt.Errorf("MarkObjectSeen error for %q: %w\n", info.Key, err)
				}
				if info.Size < s.config.Dedup.MinSizeBytes && info.ContentType != pointer.ContentPointerType {
					return s.store.UnregisterObject(ctx, bucket.Name, info.Key)
				}
				objectsScanned.Add(1)
				isUnchanged, err := s.store.IsObjectUnchanged(ctx, bucket.Name, info.Key, info.ETag, info.Size, info.LastModified)
				if err != nil {
					return fmt.Errorf("check cached object %q: %w", info.Key, err)
				}
				if isUnchanged {
					return nil
				}
				select {
				case jobs <- objectJob{
					buket:  bucket.Name,
					info:   info,
					scanID: scanID,
				}:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})

		close(jobs)
		wg.Wait()

		if err != nil {
			scanReport.Errors++
			return scanReport, err
		}

		_, err = s.store.FinalizeScope(ctx, bucket.Name, bucket.Prefix, scanID)
		if err != nil {
			scanReport.Errors++
			return scanReport, fmt.Errorf("FinalizeScope for %q/%q: %w", bucket.Name, bucket.Prefix, err)
		}

	}
	gcBytes, removedBlobs, err := s.collectGarbage(ctx)
	if err != nil {
		scanReport.Errors++
		return scanReport, fmt.Errorf("garbage collection: %w", err)
	}

	bytesReclaimed.Add(gcBytes)
	fmt.Printf("Blobs removed: %d, bytes reclaimed: %d\n", removedBlobs, gcBytes)
	stats, err := s.store.GetStats(ctx)
	if err != nil {
		scanReport.Errors++
		return scanReport, fmt.Errorf("GetStats error: %w", err)
	}
	scanReport.UniqueBlobs = stats.UniqueBlobs
	scanReport.DuplicatesFound = stats.DuplicatesFound
	scanReport.BytesReclaimable = stats.BytesReclaimable + gcBytes

	return scanReport, nil
}

// processObject streams a single object's content and returns error if occured.
// It is a standalone function so that the deferred Close runs after every object
// (not at the end of the whole scan), keeping open connections bounded.
func (s *Scanner) processObject(ctx context.Context, bucket string,
	info minio.ObjectInfo, scanID string) error {
	statObj, err := s.s3Client.StatObject(ctx, bucket, info.Key)
	if err != nil {
		return err
	}
	if statObj.ContentType == pointer.ContentPointerType {
		return s.processPointer(ctx, bucket, info, scanID)
	}

	obj, err := s.s3Client.GetObject(ctx, bucket, info.Key)
	if err != nil {
		return err
	}
	defer obj.Close()

	hash, err := hashing.HashReader(obj, s.config.Dedup.HashAlgo)
	if err != nil {
		return err
	}

	err = s.register(ctx, bucket, bucket, s.config.Dedup.BlobPrefix+hash, info, hash, info.Size, scanID)
	if err != nil {
		return err
	}

	return nil
}

func (s *Scanner) processObjectPointer(ctx context.Context, bucket string, info minio.ObjectInfo, scanID string) (int64, bool, error) {
	statObj, err := s.s3Client.StatObject(ctx, bucket, info.Key)
	if err != nil {
		return 0, false, err
	}
	if statObj.ContentType == pointer.ContentPointerType {
		return 0, false, s.processPointer(ctx, bucket, info, scanID)
	}

	obj, err := s.s3Client.GetObject(ctx, bucket, info.Key)
	if err != nil {
		return 0, false, err
	}
	defer obj.Close()

	temp, err := os.CreateTemp("", "s3-dedup-*")
	if err != nil {
		return 0, false, err
	}
	defer func() {
		temp.Close()
		os.Remove(temp.Name())
	}()

	tee := io.TeeReader(obj, temp)
	hash, err := hashing.HashReader(tee, s.config.Dedup.HashAlgo)
	if err != nil {
		return 0, false, err
	}

	blobKey := s.config.Dedup.BlobPrefix + hash
	statInfo, err := s.s3Client.StatObject(ctx, bucket, blobKey)
	errCode := minio.ToErrorResponse(err).Code
	var reclaimed int64
	switch {
	case err == nil:
		if statInfo.Size != info.Size {
			return 0, false, fmt.Errorf("Consistency error: Blob %q size mismatch", blobKey)
		}
	case errCode == "NoSuchKey":
		if _, err := temp.Seek(0, io.SeekStart); err != nil {
			return 0, false, err
		}
		n, err := s.s3Client.PutObject(ctx, bucket, blobKey, temp, info.Size, info.ContentType)
		if err != nil {
			return 0, false, err
		}
		if n != info.Size {
			return 0, false, fmt.Errorf("Consistency for PutObject error: Blob %q size mismatch", blobKey)
		}
		reclaimed -= n
		fmt.Printf("Blob %s of size %d was put\n", blobKey, n)
	default:
		return 0, false, fmt.Errorf("StatObject for blob %q: %w", blobKey, err)
	}

	statInfo, err = s.s3Client.StatObject(ctx, bucket, blobKey)
	if err != nil {
		return 0, false, err
	}

	isChanged, err := s.s3Client.StatObject(ctx, bucket, info.Key)
	if err != nil {
		return 0, false, err
	}

	res := statObj
	relinked := false
	if s.config.Dedup.DeleteOriginals && !isObjectChanged(statObj, isChanged) {
		res, err = s.safeDelete(ctx, bucket, info, hash)
		if err != nil {
			return 0, false, fmt.Errorf("processObjectPointer %q/%q: %w", bucket, info.Key, err)
		}
		relinked = true
	}

	err = s.register(ctx, bucket, bucket, blobKey, res, hash, info.Size, scanID)
	if err != nil {
		return 0, false, err
	}

	return info.Size - res.Size + reclaimed, relinked, nil
}

func (s *Scanner) processPointer(ctx context.Context, bucket string, info minio.ObjectInfo, scanID string) error {
	obj, err := s.s3Client.GetObject(ctx, bucket, info.Key)
	if err != nil {
		return err
	}
	defer obj.Close()

	p, err := pointer.ReadPointer(obj)
	if err != nil {
		return err
	}
	if p.BlobKey != s.config.Dedup.BlobPrefix+p.Hash {
		return fmt.Errorf("Pointer key %q does not match %q", p.BlobKey, s.config.Dedup.BlobPrefix+p.Hash)
	}

	statInfo, err := s.s3Client.StatObject(ctx, p.BlobBucket, p.BlobKey)
	if err != nil {
		return err
	}
	if !comparePointerObject(p, statInfo) {
		return fmt.Errorf("%q/%q: Pointer-object mismatch", bucket, info.Key)
	}

	err = s.register(ctx, bucket, p.BlobBucket, p.BlobKey, info, p.Hash, p.Size, scanID)
	if err != nil {
		return err
	}

	return nil
}

func (s *Scanner) safeDelete(ctx context.Context, bucket string, info minio.ObjectInfo, hash string) (minio.ObjectInfo, error) {
	p := pointer.Pointer{
		BlobBucket:  bucket,
		BlobKey:     s.config.Dedup.BlobPrefix + hash,
		Hash:        hash,
		Size:        info.Size,
		ContentType: info.ContentType,
	}
	data, err := pointer.WritePointer(p)
	if err != nil {
		return minio.ObjectInfo{}, fmt.Errorf("safeDelete %q/%q: %w", bucket, info.Key, err)
	}

	n, err := s.s3Client.PutObject(ctx, bucket, info.Key, bytes.NewReader(data), int64(len(data)), pointer.ContentPointerType)
	if err != nil {
		return minio.ObjectInfo{}, fmt.Errorf("safeDelete %q/%q: %w", bucket, info.Key, err)
	}
	if n != int64(len(data)) {
		return minio.ObjectInfo{}, fmt.Errorf("safeDelete %q/%q: %w", bucket, info.Key, err)
	}

	obj, err := s.s3Client.StatObject(ctx, bucket, info.Key)
	if obj.Size != int64(len(data)) {
		return minio.ObjectInfo{}, fmt.Errorf("safeDelete %q/%q: object put has different size", bucket, info.Key)
	}
	if obj.ContentType != pointer.ContentPointerType {
		return minio.ObjectInfo{}, fmt.Errorf("safeDelete %q/%q: object put ContentType must be %q", bucket, info.Key, pointer.ContentPointerType)
	}

	return obj, nil
}

func (s *Scanner) collectGarbage(ctx context.Context) (int64, int64, error) {
	var bytesReclaimed int64
	var blobsRemoved int64
	var errs []error

	seenBuckets := make(map[string]struct{})
	for _, configured := range s.config.S3.Buckets {
		bucket := configured.Name
		if _, exists := seenBuckets[bucket]; exists {
			continue
		}
		seenBuckets[bucket] = struct{}{}

		blobs, err := s.store.ListUnreferencedBlobs(ctx, bucket)
		if err != nil {
			errs = append(errs, fmt.Errorf(
				"list unreferenced blobs in %q: %w",
				bucket,
				err,
			))
			continue
		}
		if len(blobs) == 0 {
			continue
		}

		keys := make([]string, 0, len(blobs))
		byKey := make(map[string]cache.BlobRecord, len(blobs))
		for _, blob := range blobs {
			if _, exists := byKey[blob.Key]; exists {
				errs = append(errs, fmt.Errorf(
					"duplicate blob key %q/%q in cache",
					bucket,
					blob.Key,
				))
				continue
			}
			keys = append(keys, blob.Key)
			byKey[blob.Key] = blob
		}

		deletedKeys, removeErr := s.s3Client.RemoveObjects(
			ctx,
			bucket,
			keys,
		)

		for _, key := range deletedKeys {
			blob, exists := byKey[key]
			if !exists {
				errs = append(errs, fmt.Errorf(
					"S3 returned unknown deleted key %q/%q",
					bucket,
					key,
				))
				continue
			}

			if err := s.store.DeleteUnreferencedBlob(
				ctx,
				blob.Bucket,
				blob.Hash,
			); err != nil {
				errs = append(errs, fmt.Errorf(
					"delete blob %q/%q from cache: %w",
					blob.Bucket,
					blob.Key,
					err,
				))
				continue
			}

			blobsRemoved++
			bytesReclaimed += blob.Size
		}

		if removeErr != nil {
			errs = append(errs, fmt.Errorf(
				"remove unreferenced blobs from %q: %w",
				bucket,
				removeErr,
			))
		}
	}

	return bytesReclaimed, blobsRemoved, errors.Join(errs...)
}

func isObjectChanged(objBefore minio.ObjectInfo, objAfter minio.ObjectInfo) bool {
	if objBefore.ETag != objAfter.ETag ||
		objBefore.Size != objAfter.Size ||
		objBefore.LastModified != objAfter.LastModified {
		return true
	}
	return false
}

func comparePointerObject(pointer *pointer.Pointer, obj minio.ObjectInfo) bool {
	switch {
	case pointer.BlobKey != obj.Key:
		return false
	case pointer.Size != obj.Size:
		return false
	}

	return true
}

func (s *Scanner) register(
	ctx context.Context,
	bucket string,
	blobBucket string,
	blobKey string,
	info minio.ObjectInfo,
	hash string,
	blobSize int64,
	scanID string,
) error {
	record := cache.ObjectRecord{
		Bucket:       bucket,
		BlobBucket:   blobBucket,
		BlobKey:      blobKey,
		Key:          info.Key,
		ETag:         info.ETag,
		Size:         info.Size,
		BlobSize:     blobSize,
		LastModified: info.LastModified,
		Hash:         hash,
		LastSeenScan: scanID,
	}

	err := s.store.RegisterObject(ctx, record)
	if err != nil {
		return err
	}
	return nil
}
