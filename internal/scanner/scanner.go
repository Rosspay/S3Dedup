package scanner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"s3-dedup/internal/cache"
	"s3-dedup/internal/config"
	"s3-dedup/internal/hashing"
	"s3-dedup/internal/logger"
	"s3-dedup/internal/pointer"
	"s3-dedup/internal/report"
	"s3-dedup/internal/tempfiles"

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
	s3Client  S3Client
	store     cache.Store
	config    *config.Config
	logging   *logger.Logger
	blobLocks sync.Map
	tempFiles sync.Map
	tempDir   string
}

type objectJob struct {
	buket  string
	info   minio.ObjectInfo
	scanID string
}

func NewScanner(s3Client S3Client, store cache.Store, config *config.Config, logging *logger.Logger) *Scanner {
	return &Scanner{
		s3Client: s3Client,
		store:    store,
		config:   config,
		logging:  logging,
		tempDir:  os.TempDir(),
	}
}

type atomicReportPart struct {
	objectsScanned  atomic.Int64
	processErrors   atomic.Int64
	objectsRelinked atomic.Int64
	bytesReclaimed  atomic.Int64
}

const scannerTempMaxAge = 24 * time.Hour

func (s *Scanner) ScanOnce(ctx context.Context) (scanReport report.Report, resErr error) {
	scanReport.ScanStarted = time.Now().UTC()
	removedTempFiles, cleanupErr := s.CleanupTempFiles(scannerTempMaxAge)
	if cleanupErr != nil {
		s.logging.Warnf("cleanup scanner temporary files: %v\n", cleanupErr)
	} else if removedTempFiles > 0 {
		s.logging.Infof("Scanner temporary files removed: %d\n", removedTempFiles)
	}

	scanReport.Mode = s.config.Dedup.Mode
	scanID := strconv.FormatInt(scanReport.ScanStarted.UnixNano(), 10)
	workers := s.config.Schedule.Workers
	if workers <= 0 {
		workers = 1
	}

	var atomics atomicReportPart
	defer func() {
		scanReport.ScanFinished = time.Now().UTC()
		fmt.Printf("Scan %s finished at %s in %f\n", scanID, scanReport.ScanFinished, time.Since(scanReport.ScanStarted).Seconds())
		scanReport.ObjectsScanned = atomics.objectsScanned.Load()
		scanReport.Errors += atomics.processErrors.Load()
		scanReport.ObjectsRelinked = atomics.objectsRelinked.Load()
		scanReport.BytesReclaimed = atomics.bytesReclaimed.Load()
	}()

	fmt.Printf("Scan %s started at %s\n", scanID, scanReport.ScanStarted)
	s.logging.Infof("Scan %s started at %s\n", scanID, scanReport.ScanStarted)

	switch s.config.Dedup.Mode {
	case "report_only":
		if err := s.scanReportOnly(ctx, &scanReport, &atomics, workers); err != nil {
			scanReport.Errors++
			return scanReport, err
		}
	case "pointer":
		if err := s.scanPointer(ctx, &scanReport, &atomics, workers, scanID); err != nil {
			scanReport.Errors++
			return scanReport, err
		}
	default:
		scanReport.Errors++
		return scanReport, fmt.Errorf("mode %q is not supported", s.config.Dedup.Mode)
	}
	return scanReport, nil
}

func (s *Scanner) discoverObject(
	ctx context.Context,
	bucket string,
	listedInfo minio.ObjectInfo,
	scanID string,
) (cache.ObjectRecord, error) {
	before, err := s.s3Client.StatObject(ctx, bucket, listedInfo.Key)
	if err != nil {
		return cache.ObjectRecord{}, err
	}
	if isObjectChanged(listedInfo, before) {
		return cache.ObjectRecord{}, fmt.Errorf("object changed after listing")
	}
	if before.ContentType == pointer.ContentPointerType {
		s.logging.Debugf("Object %s/%s is a pointer\n", bucket, before.Key)
		return s.discoverPointer(ctx, bucket, before, scanID)
	}

	obj, err := s.s3Client.GetObject(ctx, bucket, before.Key)
	if err != nil {
		return cache.ObjectRecord{}, err
	}
	hash, hashErr := hashing.HashReader(obj, s.config.Dedup.HashAlgo)
	closeErr := obj.Close()
	if hashErr != nil {
		return cache.ObjectRecord{}, hashErr
	}
	if closeErr != nil {
		return cache.ObjectRecord{}, closeErr
	}

	return newObjectRecord(
		bucket,
		s.config.Dedup.BlobBucket,
		s.config.Dedup.BlobPrefix+hash,
		before,
		hash,
		s.config.Dedup.HashAlgo,
		before.Size,
		scanID,
		cache.ObjectStateReported,
	), nil
}

func (s *Scanner) processObject(
	ctx context.Context,
	bucket string,
	info minio.ObjectInfo,
	scanID string,
) error {
	record, err := s.discoverObject(ctx, bucket, info, scanID)
	if err != nil {
		return err
	}
	return s.store.RegisterObject(ctx, record)
}

func (s *Scanner) blobMutex(blobBucket, blobKey string) *sync.Mutex {
	key := blobBucket + blobKey
	mu, _ := s.blobLocks.LoadOrStore(key, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func (s *Scanner) createTempFile() (*os.File, error) {
	temp, err := tempfiles.Create(s.tempDir, tempfiles.ScannerPattern)
	if err != nil {
		return nil, err
	}
	s.tempFiles.Store(temp.Name(), struct{}{})
	return temp, nil
}

func (s *Scanner) removeTempFile(temp *os.File) {
	if temp == nil {
		return
	}
	name := temp.Name()
	temp.Close()
	os.Remove(name)
	s.tempFiles.Delete(name)
}

func (s *Scanner) CleanupTempFiles(maxAge time.Duration) (int, error) {
	return tempfiles.RemoveStale(
		s.tempDir,
		tempfiles.ScannerPattern,
		maxAge,
		func(path string) bool {
			_, active := s.tempFiles.Load(path)
			return active
		},
	)
}

func (s *Scanner) processObjectPointer(
	ctx context.Context,
	candidate cache.DedupCandidate,
	scanID string,
	blobs *dedupBlobCoordinator,
) (pointerDedupResult, error) {
	discovered := minio.ObjectInfo{
		Key:          candidate.Key,
		ETag:         candidate.ETag,
		Size:         candidate.Size,
		LastModified: candidate.LastModified,
	}
	statObj, err := s.s3Client.StatObject(ctx, candidate.Bucket, candidate.Key)
	if err != nil {
		return pointerDedupResult{}, err
	}
	if isObjectChanged(discovered, statObj) {
		s.logging.Debugf(
			"Object %s/%s changed after discovery and will be retried on the next scan\n",
			candidate.Bucket,
			candidate.Key,
		)
		return pointerDedupResult{}, nil
	}
	if statObj.ContentType == pointer.ContentPointerType {
		record, err := s.discoverPointer(ctx, candidate.Bucket, statObj, scanID)
		if err != nil {
			return pointerDedupResult{}, err
		}
		return pointerDedupResult{record: &record}, nil
	}
	if candidate.HashAlgo != s.config.Dedup.HashAlgo {
		return pointerDedupResult{}, fmt.Errorf(
			"dedup candidate %q/%q hash algorithm is %q, expected %q",
			candidate.Bucket,
			candidate.Key,
			candidate.HashAlgo,
			s.config.Dedup.HashAlgo,
		)
	}
	if candidate.BlobSize != statObj.Size {
		return pointerDedupResult{}, fmt.Errorf(
			"dedup candidate %q/%q size %d does not match blob size %d",
			candidate.Bucket,
			candidate.Key,
			statObj.Size,
			candidate.BlobSize,
		)
	}

	blobBucket := s.config.Dedup.BlobBucket
	blobKey := s.config.Dedup.BlobPrefix + candidate.Hash
	reclaimed, stable, err := s.materializeCandidateBlob(
		ctx,
		candidate,
		statObj,
		blobBucket,
		blobKey,
		blobs,
	)
	if err != nil {
		return pointerDedupResult{}, err
	}
	result := pointerDedupResult{reclaimed: reclaimed}
	if !stable {
		return result, nil
	}

	current, err := s.s3Client.StatObject(ctx, candidate.Bucket, candidate.Key)
	if err != nil {
		return pointerDedupResult{}, err
	}
	if isObjectChanged(statObj, current) {
		s.logging.Debugf(
			"Object %s/%s changed during deduplication and will be retried on the next scan\n",
			candidate.Bucket,
			candidate.Key,
		)
		return result, nil
	}

	res := statObj
	if s.config.Dedup.DeleteOriginals {
		s.logging.Debugf("Replacing object %s/%s with pointer", candidate.Bucket, candidate.Key)
		res, err = s.safeReplace(
			ctx,
			candidate.Bucket,
			blobBucket,
			statObj,
			candidate.Hash,
		)
		if err != nil {
			return pointerDedupResult{}, fmt.Errorf(
				"processObjectPointer %q/%q: %w",
				candidate.Bucket,
				candidate.Key,
				err,
			)
		}
		result.relinked = true
	}
	result.reclaimed += statObj.Size - res.Size

	state := cache.ObjectStateBlobReady
	if result.relinked {
		state = cache.ObjectStatePointer
	}
	record := newObjectRecord(
		candidate.Bucket,
		blobBucket,
		blobKey,
		res,
		candidate.Hash,
		candidate.HashAlgo,
		candidate.BlobSize,
		scanID,
		state,
	)
	result.record = &record
	return result, nil
}

func (s *Scanner) materializeCandidateBlob(
	ctx context.Context,
	candidate cache.DedupCandidate,
	source minio.ObjectInfo,
	blobBucket string,
	blobKey string,
	blobs *dedupBlobCoordinator,
) (int64, bool, error) {
	if readySize, ok := blobs.readySize(blobBucket, blobKey); ok {
		if readySize != candidate.BlobSize {
			return 0, false, fmt.Errorf(
				"consistency error: blob %q size %d does not match candidate size %d",
				blobKey,
				readySize,
				candidate.BlobSize,
			)
		}
		return 0, true, nil
	}

	mu := blobs.mutex(blobBucket, blobKey)
	mu.Lock()
	defer mu.Unlock()
	if readySize, ok := blobs.readySize(blobBucket, blobKey); ok {
		if readySize != candidate.BlobSize {
			return 0, false, fmt.Errorf(
				"consistency error: blob %q size %d does not match candidate size %d",
				blobKey,
				readySize,
				candidate.BlobSize,
			)
		}
		return 0, true, nil
	}

	blobInfo, err := s.s3Client.StatObject(ctx, blobBucket, blobKey)
	switch {
	case err == nil:
		if blobInfo.Size != candidate.BlobSize {
			return 0, false, fmt.Errorf(
				"consistency error: blob %q size mismatch",
				blobKey,
			)
		}
		blobs.markReady(blobBucket, blobKey, blobInfo.Size)
		return 0, true, nil
	case minio.ToErrorResponse(err).Code != "NoSuchKey":
		return 0, false, fmt.Errorf(
			"StatObject for blob %q: %w",
			blobKey,
			err,
		)
	}

	obj, err := s.s3Client.GetObject(ctx, candidate.Bucket, candidate.Key)
	if err != nil {
		return 0, false, err
	}
	temp, err := s.createTempFile()
	if err != nil {
		obj.Close()
		return 0, false, err
	}
	defer s.removeTempFile(temp)

	hash, hashErr := hashing.HashReader(io.TeeReader(obj, temp), candidate.HashAlgo)
	closeErr := obj.Close()
	if hashErr != nil {
		return 0, false, hashErr
	}
	if closeErr != nil {
		return 0, false, closeErr
	}
	if hash != candidate.Hash {
		return 0, false, fmt.Errorf(
			"consistency error: object %q/%q hash changed without metadata change",
			candidate.Bucket,
			candidate.Key,
		)
	}
	tempInfo, err := temp.Stat()
	if err != nil {
		return 0, false, err
	}
	if tempInfo.Size() != candidate.BlobSize {
		return 0, false, fmt.Errorf(
			"consistency error: object %q/%q content size %d does not match expected size %d",
			candidate.Bucket,
			candidate.Key,
			tempInfo.Size(),
			candidate.BlobSize,
		)
	}

	current, err := s.s3Client.StatObject(ctx, candidate.Bucket, candidate.Key)
	if err != nil {
		return 0, false, err
	}
	if isObjectChanged(source, current) {
		s.logging.Debugf(
			"Object %s/%s changed while materializing its blob and will be retried on the next scan\n",
			candidate.Bucket,
			candidate.Key,
		)
		return 0, false, nil
	}
	if _, err := temp.Seek(0, io.SeekStart); err != nil {
		return 0, false, err
	}
	n, err := s.s3Client.PutObject(
		ctx,
		blobBucket,
		blobKey,
		temp,
		candidate.BlobSize,
		source.ContentType,
	)
	if err != nil {
		return 0, false, err
	}
	if n != candidate.BlobSize {
		return 0, false, fmt.Errorf(
			"consistency error: PutObject for blob %q wrote %d bytes, expected %d",
			blobKey,
			n,
			candidate.BlobSize,
		)
	}
	blobs.markReady(blobBucket, blobKey, n)
	s.logging.Debugf("Blob %s of size %d was put\n", blobKey, n)
	return -n, true, nil
}

func (s *Scanner) createBlob(ctx context.Context, hash string, temp *os.File, size int64, contentType string) (int64, error) {
	blobBucket := s.config.Dedup.BlobBucket
	blobKey := s.config.Dedup.BlobPrefix + hash
	mu := s.blobMutex(blobBucket, blobKey)

	reclaimed, err := func() (int64, error) {
		mu.Lock()
		defer mu.Unlock()
		statInfo, err := s.s3Client.StatObject(ctx, blobBucket, blobKey)
		errCode := minio.ToErrorResponse(err).Code
		var reclaimed int64
		switch {
		case err == nil:
			if statInfo.Size != size {
				return 0, fmt.Errorf("Consistency error: Blob %q size mismatch", blobKey)
			}
		case errCode == "NoSuchKey":
			if _, err := temp.Seek(0, io.SeekStart); err != nil {
				return 0, err
			}
			n, err := s.s3Client.PutObject(ctx, blobBucket, blobKey, temp, size, contentType)
			if err != nil {
				return 0, err
			}
			if n != size {
				return 0, fmt.Errorf("Consistency for PutObject error: Blob %q size mismatch", blobKey)
			}
			reclaimed -= n
			s.logging.Debugf("Blob %s of size %d was put\n", blobKey, n)
			return reclaimed, nil
		default:
			return 0, fmt.Errorf("StatObject for blob %q: %w", blobKey, err)
		}
		return 0, nil
	}()

	_, err = s.s3Client.StatObject(ctx, blobBucket, blobKey)
	if err != nil {
		return 0, err
	}
	return reclaimed, nil
}

func (s *Scanner) discoverPointer(
	ctx context.Context,
	bucket string,
	info minio.ObjectInfo,
	scanID string,
) (cache.ObjectRecord, error) {
	obj, err := s.s3Client.GetObject(ctx, bucket, info.Key)
	if err != nil {
		return cache.ObjectRecord{}, err
	}
	p, readErr := pointer.ReadPointer(obj)
	closeErr := obj.Close()
	if readErr != nil {
		return cache.ObjectRecord{}, readErr
	}
	if closeErr != nil {
		return cache.ObjectRecord{}, closeErr
	}

	logicalInfo := info
	if p.HashAlgo != s.config.Dedup.HashAlgo {
		p, logicalInfo, err = s.migratePointerHash(ctx, bucket, info, p)
		if err != nil {
			return cache.ObjectRecord{}, err
		}
	}
	if p.BlobKey != s.config.Dedup.BlobPrefix+p.Hash {
		return cache.ObjectRecord{}, fmt.Errorf("Pointer key %q does not match %q", p.BlobKey, s.config.Dedup.BlobPrefix+p.Hash)
	}

	statInfo, err := s.s3Client.StatObject(ctx, p.BlobBucket, p.BlobKey)
	if err != nil {
		return cache.ObjectRecord{}, err
	}
	if !comparePointerObject(p, statInfo) {
		return cache.ObjectRecord{}, fmt.Errorf("%q/%q: Pointer-object mismatch", bucket, info.Key)
	}

	return newObjectRecord(
		bucket,
		p.BlobBucket,
		p.BlobKey,
		logicalInfo,
		p.Hash,
		p.HashAlgo,
		p.Size,
		scanID,
		cache.ObjectStatePointer,
	), nil
}

func (s *Scanner) processPointer(ctx context.Context, bucket string, info minio.ObjectInfo, scanID string) error {
	record, err := s.discoverPointer(ctx, bucket, info, scanID)
	if err != nil {
		return err
	}
	return s.store.RegisterObject(ctx, record)
}

func (s *Scanner) migratePointerHash(ctx context.Context, bucket string, info minio.ObjectInfo, p *pointer.Pointer) (*pointer.Pointer, minio.ObjectInfo, error) {
	obj, err := s.s3Client.GetObject(ctx, p.BlobBucket, p.BlobKey)
	if err != nil {
		return nil, minio.ObjectInfo{}, err
	}
	defer obj.Close()

	temp, err := s.createTempFile()
	if err != nil {
		return nil, minio.ObjectInfo{}, err
	}
	defer s.removeTempFile(temp)

	tee := io.TeeReader(obj, temp)
	hash, err := hashing.HashReader(tee, s.config.Dedup.HashAlgo)
	if err != nil {
		return nil, minio.ObjectInfo{}, err
	}

	_, err = s.createBlob(ctx, hash, temp, p.Size, p.ContentType)
	if err != nil {
		return nil, minio.ObjectInfo{}, err
	}

	p.BlobKey = s.config.Dedup.BlobPrefix + hash
	p.Hash = hash
	p.HashAlgo = s.config.Dedup.HashAlgo
	p.BlobBucket = s.config.Dedup.BlobBucket

	data, err := pointer.WritePointer(*p)
	if err != nil {
		return nil, minio.ObjectInfo{}, fmt.Errorf("safeDelete %q/%q: %w", bucket, info.Key, err)
	}

	n, err := s.s3Client.PutObject(ctx, bucket, info.Key, bytes.NewReader(data), int64(len(data)), pointer.ContentPointerType)
	if err != nil {
		return nil, minio.ObjectInfo{}, fmt.Errorf("safeDelete %q/%q: %w", bucket, info.Key, err)
	}
	if n != int64(len(data)) {
		return nil, minio.ObjectInfo{}, fmt.Errorf("safeDelete %q/%q: PutObject size mismatch", bucket, info.Key)
	}

	pInfo, err := s.s3Client.StatObject(ctx, bucket, info.Key)
	if err != nil {
		return nil, minio.ObjectInfo{}, fmt.Errorf("safeDelete %q/%q: %w", bucket, info.Key, err)
	}
	if pInfo.Size != int64(len(data)) {
		return nil, minio.ObjectInfo{}, fmt.Errorf("safeDelete %q/%q: object put has different size", bucket, info.Key)
	}
	if pInfo.ContentType != pointer.ContentPointerType {
		return nil, minio.ObjectInfo{}, fmt.Errorf("safeDelete %q/%q: object put ContentType must be %q", bucket, info.Key, pointer.ContentPointerType)
	}

	return p, pInfo, nil
}

func (s *Scanner) safeReplace(ctx context.Context, bucket string, blobBucket string, info minio.ObjectInfo, hash string) (minio.ObjectInfo, error) {
	p := pointer.Pointer{
		BlobBucket:  blobBucket,
		BlobKey:     s.config.Dedup.BlobPrefix + hash,
		HashAlgo:    s.config.Dedup.HashAlgo,
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
		return minio.ObjectInfo{}, fmt.Errorf("safeDelete %q/%q: PutObject size mismatch", bucket, info.Key)
	}

	obj, err := s.s3Client.StatObject(ctx, bucket, info.Key)
	if err != nil {
		return minio.ObjectInfo{}, fmt.Errorf("safeDelete %q/%q: %w", bucket, info.Key, err)
	}
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

	bucket := s.config.Dedup.BlobBucket

	blobs, err := s.store.ListUnreferencedBlobs(ctx, bucket)
	if err != nil {
		return 0, 0, fmt.Errorf("list unreferenced blobs in blobBucket %q: %w", bucket, err)
	}
	if len(blobs) == 0 {
		return 0, 0, nil
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

	return bytesReclaimed, blobsRemoved, errors.Join(errs...)
}

func isObjectChanged(objBefore minio.ObjectInfo, objAfter minio.ObjectInfo) bool {
	if objBefore.ETag != objAfter.ETag ||
		objBefore.Size != objAfter.Size ||
		objBefore.LastModified.UTC().Truncate(time.Second) != objAfter.LastModified.UTC().Truncate(time.Second) {
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

func newObjectRecord(
	bucket string,
	blobBucket string,
	blobKey string,
	info minio.ObjectInfo,
	hash string,
	hashAlgo string,
	blobSize int64,
	scanID string,
	state cache.ObjectState,
) cache.ObjectRecord {
	return cache.ObjectRecord{
		Bucket:       bucket,
		BlobBucket:   blobBucket,
		BlobKey:      blobKey,
		Key:          info.Key,
		ETag:         info.ETag,
		Size:         info.Size,
		BlobSize:     blobSize,
		LastModified: info.LastModified,
		Hash:         hash,
		HashAlgo:     hashAlgo,
		LastSeenScan: scanID,
		State:        state,
	}
}

func (s *Scanner) register(
	ctx context.Context,
	bucket string,
	blobBucket string,
	blobKey string,
	info minio.ObjectInfo,
	hash string,
	hashAlgo string,
	blobSize int64,
	scanID string,
	state cache.ObjectState,
) error {
	return s.store.RegisterObject(ctx, newObjectRecord(
		bucket,
		blobBucket,
		blobKey,
		info,
		hash,
		hashAlgo,
		blobSize,
		scanID,
		state,
	))
}

func desiredPointerState(deleteOriginals bool) cache.ObjectState {
	if !deleteOriginals {
		return cache.ObjectStateBlobReady
	}
	return cache.ObjectStatePointer
}

func stateRank(state cache.ObjectState) int {
	switch state {
	case cache.ObjectStateReported:
		return 1
	case cache.ObjectStateBlobReady:
		return 2
	case cache.ObjectStatePointer:
		return 3
	default:
		return 0
	}
}
