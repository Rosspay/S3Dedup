package scanner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"s3-dedup/internal/cache"
	"s3-dedup/internal/config"
	"s3-dedup/internal/hashing"
	"s3-dedup/internal/pointer"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v6"
)

type MockS3Client struct {
	objects    []minio.ObjectInfo
	contents   map[string]string
	stats      map[string]minio.ObjectInfo
	errors     map[string]error
	statErrors map[string]error
	putErrors  map[string]error
	putCalls   map[string]int
	listErr    error
}

func (m *MockS3Client) ListObjects(
	ctx context.Context,
	bucket string,
	prefix string,
	recursive bool,
	fn func(minio.ObjectInfo) error,
) error {
	if m.listErr != nil {
		return m.listErr
	}
	for _, object := range m.objects {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !strings.HasPrefix(object.Key, prefix) {
			continue
		}
		if err := fn(object); err != nil {
			return err
		}

	}
	return nil
}

func (m *MockS3Client) GetObject(
	ctx context.Context,
	bucket string,
	key string,
) (io.ReadCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	id := objectID(bucket, key)
	if err := m.errors[id]; err != nil {
		return nil, err
	}
	content, ok := m.contents[id]
	if !ok {
		return nil, fmt.Errorf("object %q/%q not found", bucket, key)
	}
	return io.NopCloser(strings.NewReader(content)), nil
}

func (m *MockS3Client) PutObject(
	ctx context.Context,
	bucket string,
	objectName string,
	reader io.Reader,
	size int64,
) (int64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	id := objectID(bucket, objectName)
	if m.putCalls == nil {
		m.putCalls = make(map[string]int)
	}
	m.putCalls[id]++
	if putErr := m.putErrors[id]; putErr != nil {
		return 0, putErr
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return 0, err
	}
	if int64(len(data)) != size {
		return 0, fmt.Errorf("put object %q/%q: read %d bytes, expected %d", bucket, objectName, len(data), size)
	}

	if m.contents == nil {
		m.contents = make(map[string]string)
	}
	if m.stats == nil {
		m.stats = make(map[string]minio.ObjectInfo)
	}
	m.contents[id] = string(data)
	m.stats[id] = minio.ObjectInfo{
		Key:          objectName,
		Size:         int64(len(data)),
		ETag:         "etag-" + objectName,
		LastModified: time.Now().UTC(),
	}
	return int64(len(data)), nil
}

func (m *MockS3Client) StatObject(
	ctx context.Context,
	bucket string,
	objectName string,
) (minio.ObjectInfo, error) {
	select {
	case <-ctx.Done():
		return minio.ObjectInfo{}, ctx.Err()
	default:
	}

	id := objectID(bucket, objectName)
	if err := m.statErrors[id]; err != nil {
		return minio.ObjectInfo{}, err
	}
	if info, ok := m.stats[id]; ok {
		return info, nil
	}
	if content, ok := m.contents[id]; ok {
		return minio.ObjectInfo{
			Key:          objectName,
			Size:         int64(len(content)),
			ETag:         "etag-" + objectName,
			LastModified: time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC),
		}, nil
	}
	return minio.ObjectInfo{}, minio.ErrorResponse{
		Code:       "NoSuchKey",
		Message:    "object does not exist",
		BucketName: bucket,
		Key:        objectName,
		StatusCode: 404,
	}
}

func TestScanOnceFindDuplicateContent(t *testing.T) {
	const content = "duplicate"
	const expObjectsScanned = 2
	const expUniqueBlobs = 1
	const expDuplicatesFound = 1
	const expBytesReclaimable = int64(len(content))
	const expErrors = 0
	const expMode = "report_only"

	store := openTestStore(t)
	client := &MockS3Client{
		objects: []minio.ObjectInfo{
			objectInfo("one.txt", int64(len(content))),
			objectInfo("two.txt", int64(len(content))),
		},
		contents: map[string]string{
			objectID("bucket", "one.txt"): content,
			objectID("bucket", "two.txt"): content,
		},
		errors: make(map[string]error),
	}

	scanner := NewScanner(client, store, testConfig())
	res, err := scanner.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}

	if res.ObjectsScanned != expObjectsScanned {
		t.Errorf("ObjectsScanned = %d, expected %d", res.ObjectsScanned, expObjectsScanned)
	}
	if res.UniqueBlobs != expUniqueBlobs {
		t.Errorf("UniqueBlobs = %d, expected %d", res.UniqueBlobs, expUniqueBlobs)
	}
	if res.BytesReclaimable != expBytesReclaimable {
		t.Errorf("BytesRecalimable = %d, expected %d", res.BytesReclaimable, expBytesReclaimable)
	}
	if res.Errors != expErrors {
		t.Errorf("Errors = %d, expected %d", res.Errors, expErrors)
	}
	if res.Mode != expMode {
		t.Errorf("Mode = %q, expected %q", res.Mode, expMode)
	}
	if res.ScanStarted.IsZero() {
		t.Error("Scan timestamps is not set")
	}
}

func TestScanOnceListObjectsErrorNoFinalize(t *testing.T) {
	store := openTestStore(t)
	someObj := record("bucket", "some.txt", "hash", 100)
	if err := store.RegisterObject(context.Background(), someObj); err != nil {
		t.Fatal(err)
	}

	listErr := errors.New("Error listing objects in \"bucket\":")
	client := &MockS3Client{
		listErr:  listErr,
		contents: make(map[string]string),
		errors:   make(map[string]error),
	}

	scanner := NewScanner(client, store, testConfig())
	res, resErr := scanner.ScanOnce(context.Background())
	if !errors.Is(resErr, listErr) {
		t.Fatalf("ScanOnce error = %v, expected %v", resErr, listErr)
	}
	if res.Errors != 1 {
		t.Errorf("Errors = %d, expected %d", res.Errors, 1)
	}

	//Object still must be in cache, because it was marked
	//Regardless of what happens next, so we won't accidentally delete it in the future
	stats, err := store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats error: %v", err)
	}
	if stats.UniqueBlobs != 1 {
		t.Error("Object deleted from cache, but shouldn't be so")
	}
}

func TestScanOnceMarkObjectSeen(t *testing.T) {
	store := openTestStore(t)
	someObj := record("bucket", "some.txt", "hash", 100)

	if err := store.RegisterObject(context.Background(), someObj); err != nil {
		t.Fatalf("Register object error: %v", err)
	}

	someErr := errors.New("GetObject error")
	client := &MockS3Client{
		objects: []minio.ObjectInfo{
			objectInfo("some.txt", 100),
		},
		contents: make(map[string]string),
		errors: map[string]error{
			objectID("bucket", "some.txt"): someErr,
		},
	}

	scanner := NewScanner(client, store, testConfig())
	res, err := scanner.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if res.ObjectsScanned != 1 {
		t.Errorf("Objects scanned = %d, expected %d", res.ObjectsScanned, 0)
	}
	if res.Errors != 1 {
		t.Errorf("Errors = %d, expected %d", res.Errors, 1)
	}

	stats, err := store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats error: %v", err)
	}
	if stats.UniqueBlobs != 1 {
		t.Error("Object was not marked so, was deleted from cache")
	}
}

func TestScanOnceNoObjectLostDupes(t *testing.T) {
	t.Parallel()
	const content = "duplicate"
	const expObjectsScanned = 100
	const expBytesReclaimable = 9900
	const expUniqueBlobs = 1
	const expDuplicatesFound = 99
	const expErrors = 0

	store := openTestStore(t)
	var objs []minio.ObjectInfo
	contents := make(map[string]string)
	for i := 0; i < 100; i++ {
		objs = append(objs, objectInfo("file.txt"+strconv.Itoa(i), 100))
		contents[objectID("bucket", "file.txt"+strconv.Itoa(i))] = content
	}
	client := &MockS3Client{
		objects:  objs,
		contents: contents,
		errors:   make(map[string]error),
	}
	config := testConfig()
	config.Schedule.Workers = 4
	scanner := NewScanner(client, store, config)
	res, err := scanner.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if res.ObjectsScanned != expObjectsScanned {
		t.Errorf("ObjectsScanned = %d, expected %d", res.ObjectsScanned, expObjectsScanned)
	}
	if res.UniqueBlobs != expUniqueBlobs {
		t.Errorf("UniqueBlobs = %d, expected %d", res.UniqueBlobs, expUniqueBlobs)
	}
	if res.BytesReclaimable != expBytesReclaimable {
		t.Errorf("BytesRecalimable = %d, expected %d", res.BytesReclaimable, expBytesReclaimable)
	}
	if res.Errors != expErrors {
		t.Errorf("Errors = %d, expected %d", res.Errors, expErrors)
	}
}

func TestScanOnceNoObjectLostDupeless(t *testing.T) {
	t.Parallel()
	const content = "duplicate"
	const expObjectsScanned = 100
	const expBytesReclaimable = 0
	const expUniqueBlobs = 100
	const expDuplicatesFound = 0
	const expErrors = 0

	store := openTestStore(t)
	var objs []minio.ObjectInfo
	contents := make(map[string]string)
	for i := 0; i < 100; i++ {
		objs = append(objs, objectInfo("file.txt"+strconv.Itoa(i), 100+int64(i)))
		contents[objectID("bucket", "file.txt"+strconv.Itoa(i))] = content + strconv.Itoa(i)
	}
	client := &MockS3Client{
		objects:  objs,
		contents: contents,
		errors:   make(map[string]error),
	}

	config := testConfig()
	config.Schedule.Workers = 6
	scanner := NewScanner(client, store, config)
	res, err := scanner.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if res.ObjectsScanned != expObjectsScanned {
		t.Errorf("ObjectsScanned = %d, expected %d", res.ObjectsScanned, expObjectsScanned)
	}
	if res.UniqueBlobs != expUniqueBlobs {
		t.Errorf("UniqueBlobs = %d, expected %d", res.UniqueBlobs, expUniqueBlobs)
	}
	if res.BytesReclaimable != expBytesReclaimable {
		t.Errorf("BytesRecalimable = %d, expected %d", res.BytesReclaimable, expBytesReclaimable)
	}
	if res.Errors != expErrors {
		t.Errorf("Errors = %d, expected %d", res.Errors, expErrors)
	}
}

func TestScanOnceNoObjectLostWithError(t *testing.T) {
	const content = "duplicate"
	const expObjectsScanned = 100
	const expBytesReclaimable = 0
	const expUniqueBlobs = 97
	const expDuplicatesFound = 0
	const expErrors = 3

	store := openTestStore(t)
	var objs []minio.ObjectInfo
	contents := make(map[string]string)
	for i := 0; i < 100; i++ {
		objs = append(objs, objectInfo("file.txt"+strconv.Itoa(i), 100+int64(i)))
		contents[objectID("bucket", "file.txt"+strconv.Itoa(i))] = content + strconv.Itoa(i)
	}
	someErr := errors.New("GetObject error")
	client := &MockS3Client{
		objects:  objs,
		contents: contents,
		errors: map[string]error{
			objectID("bucket", "file.txt3"):  someErr,
			objectID("bucket", "file.txt33"): someErr,
			objectID("bucket", "file.txt66"): someErr,
		},
	}

	config := testConfig()
	config.Schedule.Workers = 8
	scanner := NewScanner(client, store, config)
	res, err := scanner.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if res.ObjectsScanned != expObjectsScanned {
		t.Errorf("ObjectsScanned = %d, expected %d", res.ObjectsScanned, expObjectsScanned)
	}
	if res.UniqueBlobs != expUniqueBlobs {
		t.Errorf("UniqueBlobs = %d, expected %d", res.UniqueBlobs, expUniqueBlobs)
	}
	if res.BytesReclaimable != expBytesReclaimable {
		t.Errorf("BytesRecalimable = %d, expected %d", res.BytesReclaimable, expBytesReclaimable)
	}
	if res.Errors != expErrors {
		t.Errorf("Errors = %d, expected %d", res.Errors, expErrors)
	}
}

func TestPointerModeDuplicateObjectsCreateOneBlob(t *testing.T) {
	const content = "same pointer-mode content"

	store := openTestStore(t)
	client := &MockS3Client{
		objects: []minio.ObjectInfo{
			objectInfo("one.txt", int64(len(content))),
			objectInfo("two.txt", int64(len(content))),
		},
		contents: map[string]string{
			objectID("bucket", "one.txt"): content,
			objectID("bucket", "two.txt"): content,
		},
	}

	cfg := pointerTestConfig()
	result, err := NewScanner(client, store, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}

	hash := hashContent(t, content)
	blobKey := cfg.Dedup.BlobPrefix + hash
	if got := client.content(objectID("bucket", blobKey)); got != content {
		t.Errorf("blob content = %q, expected %q", got, content)
	}
	if got := client.countObjectsWithPrefix("bucket", cfg.Dedup.BlobPrefix); got != 1 {
		t.Errorf("blob object count = %d, expected 1", got)
	}
	if got := client.putCallCount("bucket", blobKey); got != 1 {
		t.Errorf("PutObject calls for blob = %d, expected 1", got)
	}
	if result.UniqueBlobs != 1 || result.DuplicatesFound != 1 {
		t.Errorf("stats = unique %d, duplicates %d; expected 1 and 1", result.UniqueBlobs, result.DuplicatesFound)
	}
	if result.BytesReclaimable != int64(len(content)) {
		t.Errorf("BytesReclaimable = %d, expected %d", result.BytesReclaimable, len(content))
	}
}

func TestPointerModeRepeatedScanDoesNotUploadBlobAgain(t *testing.T) {
	const content = "content uploaded once"

	store := openTestStore(t)
	client := &MockS3Client{
		objects: []minio.ObjectInfo{
			objectInfo("one.txt", int64(len(content))),
			objectInfo("two.txt", int64(len(content))),
		},
		contents: map[string]string{
			objectID("bucket", "one.txt"): content,
			objectID("bucket", "two.txt"): content,
		},
	}
	cfg := pointerTestConfig()
	scanner := NewScanner(client, store, cfg)

	if _, err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("first ScanOnce error: %v", err)
	}
	blobKey := cfg.Dedup.BlobPrefix + hashContent(t, content)
	firstPutCalls := client.putCallCount("bucket", blobKey)

	if _, err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("second ScanOnce error: %v", err)
	}
	if got := client.putCallCount("bucket", blobKey); got != firstPutCalls {
		t.Errorf("PutObject calls after repeated scan = %d, expected %d", got, firstPutCalls)
	}
	if firstPutCalls != 1 {
		t.Errorf("PutObject calls after first scan = %d, expected 1", firstPutCalls)
	}
}

func TestPointerModeDifferentContentsCreateDifferentBlobs(t *testing.T) {
	const firstContent = "first content"
	const secondContent = "second content"

	store := openTestStore(t)
	client := &MockS3Client{
		objects: []minio.ObjectInfo{
			objectInfo("one.txt", int64(len(firstContent))),
			objectInfo("two.txt", int64(len(secondContent))),
		},
		contents: map[string]string{
			objectID("bucket", "one.txt"): firstContent,
			objectID("bucket", "two.txt"): secondContent,
		},
	}
	cfg := pointerTestConfig()

	result, err := NewScanner(client, store, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if got := client.countObjectsWithPrefix("bucket", cfg.Dedup.BlobPrefix); got != 2 {
		t.Errorf("blob object count = %d, expected 2", got)
	}
	if client.content(objectID("bucket", cfg.Dedup.BlobPrefix+hashContent(t, firstContent))) != firstContent {
		t.Error("first blob was not created with expected content")
	}
	if client.content(objectID("bucket", cfg.Dedup.BlobPrefix+hashContent(t, secondContent))) != secondContent {
		t.Error("second blob was not created with expected content")
	}
	if result.UniqueBlobs != 2 || result.DuplicatesFound != 0 {
		t.Errorf("stats = unique %d, duplicates %d; expected 2 and 0", result.UniqueBlobs, result.DuplicatesFound)
	}
}

func TestPointerModeUploadErrorKeepsOriginalAndDoesNotRegister(t *testing.T) {
	const content = "must remain untouched"
	const key = "original.txt"

	store := openTestStore(t)
	cfg := pointerTestConfig()
	blobKey := cfg.Dedup.BlobPrefix + hashContent(t, content)
	putErr := errors.New("simulated blob upload error")
	client := &MockS3Client{
		objects: []minio.ObjectInfo{objectInfo(key, int64(len(content)))},
		contents: map[string]string{
			objectID("bucket", key): content,
		},
		putErrors: map[string]error{
			objectID("bucket", blobKey): putErr,
		},
	}

	result, err := NewScanner(client, store, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if result.Errors != 1 {
		t.Errorf("Errors = %d, expected 1", result.Errors)
	}
	if got := client.content(objectID("bucket", key)); got != content {
		t.Errorf("original content = %q, expected %q", got, content)
	}
	if got := client.content(objectID("bucket", blobKey)); got != "" {
		t.Errorf("failed blob upload left content %q", got)
	}
	stats, err := store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats error: %v", err)
	}
	if stats.UniqueBlobs != 0 || stats.DuplicatesFound != 0 || stats.BytesReclaimable != 0 {
		t.Errorf("cache was changed after failed upload: %+v", stats)
	}
}

func TestPointerModeObjectsInsideBlobPrefixAreNotScanned(t *testing.T) {
	const blobKey = "blobs/existing-hash"
	const content = "already stored blob"

	store := openTestStore(t)
	client := &MockS3Client{
		objects: []minio.ObjectInfo{objectInfo(blobKey, int64(len(content)))},
		contents: map[string]string{
			objectID("bucket", blobKey): content,
		},
	}
	cfg := pointerTestConfig()

	result, err := NewScanner(client, store, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if result.ObjectsScanned != 0 {
		t.Errorf("ObjectsScanned = %d, expected 0", result.ObjectsScanned)
	}
	if got := client.totalPutCalls(); got != 0 {
		t.Errorf("total PutObject calls = %d, expected 0", got)
	}
	stats, err := store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats error: %v", err)
	}
	if stats.UniqueBlobs != 0 {
		t.Errorf("existing blob was registered as a logical object: %+v", stats)
	}
}

func TestPointerModeExistingPointersRestoreCacheWithoutCreatingBlob(t *testing.T) {
	const blobContent = "shared logical content"
	const blobKey = "blobs/shared-hash"

	store := openTestStore(t)
	pointerBody := pointerDocument(t, "bucket", blobKey, "shared-hash", int64(len(blobContent)))
	firstInfo := pointerObjectInfo("one.txt", pointerBody)
	secondInfo := pointerObjectInfo("two.txt", pointerBody)
	client := &MockS3Client{
		objects: []minio.ObjectInfo{firstInfo, secondInfo},
		contents: map[string]string{
			objectID("bucket", firstInfo.Key):  pointerBody,
			objectID("bucket", secondInfo.Key): pointerBody,
			objectID("bucket", blobKey):        blobContent,
		},
		stats: map[string]minio.ObjectInfo{
			objectID("bucket", firstInfo.Key):  withContentType(firstInfo, pointer.ContentPointerType),
			objectID("bucket", secondInfo.Key): withContentType(secondInfo, pointer.ContentPointerType),
			objectID("bucket", blobKey):        objectInfo(blobKey, int64(len(blobContent))),
		},
	}
	cfg := pointerTestConfig()

	result, err := NewScanner(client, store, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if result.Errors != 0 {
		t.Errorf("Errors = %d, expected 0", result.Errors)
	}
	if result.UniqueBlobs != 1 || result.DuplicatesFound != 1 {
		t.Errorf("stats = unique %d, duplicates %d; expected 1 and 1", result.UniqueBlobs, result.DuplicatesFound)
	}
	if result.BytesReclaimable != int64(len(blobContent)) {
		t.Errorf("BytesReclaimable = %d, expected %d", result.BytesReclaimable, len(blobContent))
	}
	if got := client.totalPutCalls(); got != 0 {
		t.Errorf("total PutObject calls = %d, expected 0", got)
	}
	probe := record("bucket", "probe.txt", "shared-hash", int64(len(blobContent)))
	if err := store.RegisterObject(context.Background(), probe); err != nil {
		t.Fatalf("register probe using pointer hash: %v", err)
	}
	stats, err := store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats after probe error: %v", err)
	}
	if stats.UniqueBlobs != 1 || stats.DuplicatesFound != 2 || stats.BytesReclaimable != 2*int64(len(blobContent)) {
		t.Errorf("pointer hash or refcount was not restored correctly: %+v", stats)
	}
}

func TestPointerModePointerCanReferenceBlobInDifferentBucket(t *testing.T) {
	const sourceBucket = "source-bucket"
	const blobBucket = "blob-bucket"
	const blobContent = "cross-bucket content"
	const blobKey = "blobs/cross-bucket-hash"
	const pointerKey = "document.txt"

	store := openTestStore(t)
	pointerBody := pointerDocument(t, blobBucket, blobKey, "cross-bucket-hash", int64(len(blobContent)))
	pointerInfo := pointerObjectInfo(pointerKey, pointerBody)
	client := &MockS3Client{
		objects: []minio.ObjectInfo{pointerInfo},
		contents: map[string]string{
			objectID(sourceBucket, pointerKey): pointerBody,
			objectID(blobBucket, blobKey):      blobContent,
		},
		stats: map[string]minio.ObjectInfo{
			objectID(sourceBucket, pointerKey): withContentType(pointerInfo, pointer.ContentPointerType),
			objectID(blobBucket, blobKey):      objectInfo(blobKey, int64(len(blobContent))),
		},
	}
	cfg := pointerTestConfig()
	cfg.S3.Buckets[0].Name = sourceBucket

	result, err := NewScanner(client, store, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if result.Errors != 0 {
		t.Fatalf("Errors = %d, expected 0", result.Errors)
	}
	if result.UniqueBlobs != 1 {
		t.Errorf("UniqueBlobs = %d, expected 1", result.UniqueBlobs)
	}
}

func openTestStore(t *testing.T) *cache.SQLiteStore {
	t.Helper()

	store, err := cache.OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("OpenSQLite err: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close error: %v", err)
		}
	})
	return store
}

func testConfig() *config.Config {
	return &config.Config{
		S3: config.S3{
			Buckets: []config.Bucket{
				{Name: "bucket", Prefix: ""},
			},
		},
		Dedup: config.Dedup{
			HashAlgo:     "sha256",
			MinSizeBytes: 0,
			BlobPrefix:   "blobs/",
			Mode:         "report_only",
		},
	}
}

func record(bucket, key, hash string, size int64) cache.ObjectRecord {
	return cache.ObjectRecord{
		Bucket:       bucket,
		Key:          key,
		ETag:         "etag",
		Size:         size,
		BlobSize:     size,
		LastModified: time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC),
		Hash:         hash,
		LastSeenScan: "scan-1",
	}
}

func objectInfo(key string, size int64) minio.ObjectInfo {
	return minio.ObjectInfo{
		Key:          key,
		Size:         size,
		ETag:         "etag-" + key,
		LastModified: time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC),
	}
}

func objectID(bucket, key string) string {
	return bucket + "\\" + key
}

func pointerTestConfig() *config.Config {
	cfg := testConfig()
	cfg.Dedup.Mode = "pointer"
	cfg.Dedup.BlobPrefix = "blobs/"
	cfg.Schedule.Workers = 1
	return cfg
}

func hashContent(t *testing.T, content string) string {
	t.Helper()
	hash, err := hashing.HashReader(strings.NewReader(content), "sha256")
	if err != nil {
		t.Fatalf("HashReader error: %v", err)
	}
	return hash
}

func pointerDocument(t *testing.T, blobBucket, blobKey, hash string, size int64) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"blob_bucket":  blobBucket,
		"blob_key":     blobKey,
		"hash":         hash,
		"size":         size,
		"content_type": "application/octet-stream",
	})
	if err != nil {
		t.Fatalf("marshal pointer document: %v", err)
	}
	return string(body)
}

func pointerObjectInfo(key, body string) minio.ObjectInfo {
	return minio.ObjectInfo{
		Key:          key,
		Size:         int64(len(body)),
		ETag:         "etag-" + key,
		LastModified: time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC),
	}
}

func withContentType(info minio.ObjectInfo, contentType string) minio.ObjectInfo {
	info.ContentType = contentType
	return info
}

func (m *MockS3Client) content(id string) string {
	return m.contents[id]
}

func (m *MockS3Client) putCallCount(bucket, key string) int {
	return m.putCalls[objectID(bucket, key)]
}

func (m *MockS3Client) totalPutCalls() int {
	var total int
	for _, calls := range m.putCalls {
		total += calls
	}
	return total
}

func (m *MockS3Client) countObjectsWithPrefix(bucket, prefix string) int {
	idPrefix := objectID(bucket, prefix)
	var count int
	for id := range m.contents {
		if strings.HasPrefix(id, idPrefix) {
			count++
		}
	}
	return count
}
