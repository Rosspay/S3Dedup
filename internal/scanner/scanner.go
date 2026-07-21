package scanner

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"s3-dedup/internal/cache"
	"s3-dedup/internal/config"
	"s3-dedup/internal/hashing"
	"s3-dedup/internal/report"
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
	) (int64, error)
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

	defer func() {
		scanReport.ScanFinished = time.Now().UTC()
		scanReport.ObjectsScanned = objectsScanned.Load()
		scanReport.Errors += processErrors.Load()
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
						processErr = s.processObjectPointer(ctx, job.buket, job.info, job.scanID)
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
				if bucket.Prefix == s.config.Dedup.BlobPrefix {
					return nil
				}
				err := s.store.MarkObjectSeen(ctx, bucket.Name, info.Key, scanID)
				if err != nil {
					processErrors.Add(1)
					return fmt.Errorf("MarkObjectSeen error for %q: %w\n", info.Key, err)
				}
				if info.Size < s.config.Dedup.MinSizeBytes {
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
	stats, err := s.store.GetStats(ctx)
	if err != nil {
		scanReport.Errors++
		return scanReport, fmt.Errorf("GetStats error: %w", err)
	}
	scanReport.UniqueBlobs = stats.UniqueBlobs
	scanReport.DuplicatesFound = stats.DuplicatesFound
	scanReport.BytesReclaimable = stats.BytesReclaimable

	return scanReport, nil
}

// processObject streams a single object's content and returns error if occured.
// It is a standalone function so that the deferred Close runs after every object
// (not at the end of the whole scan), keeping open connections bounded.
func (s *Scanner) processObject(ctx context.Context, bucket string,
	info minio.ObjectInfo, scanID string) error {
	obj, err := s.s3Client.GetObject(ctx, bucket, info.Key)
	if err != nil {
		return err
	}
	defer obj.Close()

	hash, err := hashing.HashReader(obj, s.config.Dedup.HashAlgo)
	if err != nil {
		return err
	}

	err = s.register(ctx, bucket, info, hash, scanID)
	if err != nil {
		return err
	}

	return nil
}

func (s *Scanner) processObjectPointer(ctx context.Context, bucket string, info minio.ObjectInfo, scanID string) error {
	obj, err := s.s3Client.GetObject(ctx, bucket, info.Key)
	if err != nil {
		return err
	}
	defer obj.Close()

	temp, err := os.CreateTemp("", "s3-dedup-*")
	if err != nil {
		return err
	}
	defer func() {
		temp.Close()
		os.Remove(temp.Name())
	}()

	tee := io.TeeReader(obj, temp)
	hash, err := hashing.HashReader(tee, s.config.Dedup.HashAlgo)
	if err != nil {
		return err
	}

	blobKey := s.config.Dedup.BlobPrefix + hash
	statInfo, err := s.s3Client.StatObject(ctx, bucket, blobKey)
	errCode := minio.ToErrorResponse(err).Code
	switch {
	case err == nil:
		if statInfo.Size != info.Size {
			return fmt.Errorf("Consistency error: Blob %q size mismatch", blobKey)
		}
	case errCode == "NoSuchKey":
		if _, err := temp.Seek(0, io.SeekStart); err != nil {
			return err
		}
		n, err := s.s3Client.PutObject(ctx, bucket, blobKey, temp, info.Size)
		if err != nil {
			return err
		}
		if n != info.Size {
			return fmt.Errorf("Consistency for PutObject error: Blob %q size mismatch", blobKey)
		}
		fmt.Printf("Blob %s of size %d was put\n", blobKey, n)
	default:
		return fmt.Errorf("StatObject for blob %q: %w", blobKey, err)
	}

	err = s.register(ctx, bucket, info, hash, scanID)
	if err != nil {
		return err
	}

	return nil
}

func (s *Scanner) register(ctx context.Context, bucket string, info minio.ObjectInfo, hash string, scanID string) error {
	record := cache.ObjectRecord{
		Bucket:       bucket,
		Key:          info.Key,
		ETag:         info.ETag,
		Size:         info.Size,
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
