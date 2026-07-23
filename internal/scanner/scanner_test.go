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

// Mocking S3 client for testing purposes
// so we can simulate basic behavior and errors
type MockS3Client struct {
	objects    []minio.ObjectInfo
	contents   map[string]string
	stats      map[string]minio.ObjectInfo
	errors     map[string]error
	statErrors map[string]error
	putErrors  map[string]error
	putCalls   map[string]int
	statCalls  map[string]int
	statHooks  map[string]func(*MockS3Client, int)
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
	contentType string,
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
	info := minio.ObjectInfo{
		Key:          objectName,
		Size:         int64(len(data)),
		ETag:         fmt.Sprintf("etag-%s-%d", objectName, m.putCalls[id]),
		LastModified: time.Now().UTC(),
		ContentType:  contentType,
	}
	m.stats[id] = info
	for i := range m.objects {
		if m.objects[i].Key == objectName {
			m.objects[i] = info
			return int64(len(data)), nil
		}
	}
	m.objects = append(m.objects, info)
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
	if m.statCalls == nil {
		m.statCalls = make(map[string]int)
	}
	m.statCalls[id]++
	if hook := m.statHooks[id]; hook != nil {
		hook(m, m.statCalls[id])
	}
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

func (m *MockS3Client) RemoveObjects(
	ctx context.Context,
	bucket string,
	keys []string,
) ([]string, error) {
	return nil, nil
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

func TestPointerModeChangedObjectAfterListingIsNotReplaced(t *testing.T) {
	const key = "document.txt"
	const originalContent = "old-data"
	const changedContent = "new-data"

	store := openTestStore(t)
	listedInfo := objectInfo(key, int64(len(originalContent)))
	client := &MockS3Client{
		objects: []minio.ObjectInfo{listedInfo},
		contents: map[string]string{
			objectID("bucket", key): originalContent,
		},
		stats: map[string]minio.ObjectInfo{
			objectID("bucket", key): listedInfo,
		},
	}
	client.statHooks = map[string]func(*MockS3Client, int){
		objectID("bucket", key): func(m *MockS3Client, call int) {
			if call != 2 {
				return
			}
			changedInfo := listedInfo
			changedInfo.ETag = "etag-changed"
			changedInfo.LastModified = listedInfo.LastModified.Add(time.Second)
			m.contents[objectID("bucket", key)] = changedContent
			m.stats[objectID("bucket", key)] = changedInfo
		},
	}
	cfg := pointerTestConfig()
	cfg.Dedup.DeleteOriginals = true

	result, err := NewScanner(client, store, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if got := client.content(objectID("bucket", key)); got != changedContent {
		t.Errorf("changed object content = %q, expected %q", got, changedContent)
	}
	if got := client.putCallCount("bucket", key); got != 0 {
		t.Errorf("pointer PutObject calls = %d, expected 0", got)
	}
	if result.ObjectsRelinked != 0 {
		t.Errorf("ObjectsRelinked = %d, expected 0", result.ObjectsRelinked)
	}
	if result.BytesReclaimed != 0 {
		t.Errorf("BytesReclaimed = %d, expected 0", result.BytesReclaimed)
	}
}

func TestPointerModePointerWriteErrorDoesNotRegisterReference(t *testing.T) {
	const key = "original.txt"
	const content = "pointer write must fail"

	store := openTestStore(t)
	cfg := pointerTestConfig()
	cfg.Dedup.DeleteOriginals = true
	hash := hashContent(t, content)
	blobKey := cfg.Dedup.BlobPrefix + hash
	info := objectInfo(key, int64(len(content)))
	client := &MockS3Client{
		objects: []minio.ObjectInfo{info},
		contents: map[string]string{
			objectID("bucket", key):     content,
			objectID("bucket", blobKey): content,
		},
		stats: map[string]minio.ObjectInfo{
			objectID("bucket", key):     info,
			objectID("bucket", blobKey): objectInfo(blobKey, int64(len(content))),
		},
		putErrors: map[string]error{
			objectID("bucket", key): errors.New("simulated pointer upload error"),
		},
	}

	result, err := NewScanner(client, store, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if result.Errors != 1 {
		t.Errorf("Errors = %d, expected 1", result.Errors)
	}
	if result.ObjectsRelinked != 0 {
		t.Errorf("ObjectsRelinked = %d, expected 0", result.ObjectsRelinked)
	}
	if got := client.content(objectID("bucket", key)); got != content {
		t.Errorf("original content = %q, expected %q", got, content)
	}
	stats, err := store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats error: %v", err)
	}
	if stats.UniqueBlobs != 0 || stats.DuplicatesFound != 0 || stats.BytesReclaimable != 0 {
		t.Errorf("cache was changed after failed pointer upload: %+v", stats)
	}
}

func TestPointerModeNextScanRecognizesPointerWithoutCreatingBlobAgain(t *testing.T) {
	const key = "document.txt"
	const content = "content converted to pointer"

	firstStore := openTestStore(t)
	info := objectInfo(key, int64(len(content)))
	client := &MockS3Client{
		objects: []minio.ObjectInfo{info},
		contents: map[string]string{
			objectID("bucket", key): content,
		},
		stats: map[string]minio.ObjectInfo{
			objectID("bucket", key): info,
		},
	}
	cfg := pointerTestConfig()
	cfg.Dedup.DeleteOriginals = true
	scanner := NewScanner(client, firstStore, cfg)

	firstResult, err := scanner.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("first ScanOnce error: %v", err)
	}
	if firstResult.Errors != 0 || firstResult.ObjectsRelinked != 1 {
		t.Fatalf("first result = errors %d, relinked %d; expected 0 and 1", firstResult.Errors, firstResult.ObjectsRelinked)
	}
	hash := hashContent(t, content)
	blobKey := cfg.Dedup.BlobPrefix + hash
	blobPutCalls := client.putCallCount("bucket", blobKey)
	pointerPutCalls := client.putCallCount("bucket", key)
	if blobPutCalls != 1 || pointerPutCalls != 1 {
		t.Fatalf("first PutObject calls = blob %d, pointer %d; expected 1 and 1", blobPutCalls, pointerPutCalls)
	}
	if got := client.stats[objectID("bucket", key)].ContentType; got != pointer.ContentPointerType {
		t.Fatalf("pointer ContentType = %q, expected %q", got, pointer.ContentPointerType)
	}

	secondStore := openTestStore(t)
	secondResult, err := NewScanner(client, secondStore, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("second ScanOnce error: %v", err)
	}
	if secondResult.Errors != 0 {
		t.Errorf("second Errors = %d, expected 0", secondResult.Errors)
	}
	if secondResult.ObjectsRelinked != 0 {
		t.Errorf("second ObjectsRelinked = %d, expected 0", secondResult.ObjectsRelinked)
	}
	if got := client.putCallCount("bucket", blobKey); got != blobPutCalls {
		t.Errorf("blob PutObject calls after second scan = %d, expected %d", got, blobPutCalls)
	}
	if got := client.putCallCount("bucket", key); got != pointerPutCalls {
		t.Errorf("pointer PutObject calls after second scan = %d, expected %d", got, pointerPutCalls)
	}
	if secondResult.UniqueBlobs != 1 {
		t.Errorf("second UniqueBlobs = %d, expected 1", secondResult.UniqueBlobs)
	}
}

func TestBlobReferencesAreIndependentAcrossBuckets(t *testing.T) {
	const (
		hash = "same-hash"
		size = int64(100)
	)

	ctx := context.Background()
	store := openTestStore(t)
	makeRecord := func(bucket, key, blobBucket string) cache.ObjectRecord {
		return cache.ObjectRecord{
			Bucket:       bucket,
			Key:          key,
			ETag:         "etag-" + key,
			Size:         size,
			BlobBucket:   blobBucket,
			BlobKey:      "blobs/" + hash,
			BlobSize:     size,
			LastModified: time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC),
			Hash:         hash,
			LastSeenScan: "scan-1",
		}
	}

	objects := []cache.ObjectRecord{
		makeRecord("source-a", "a-1.txt", "blob-bucket-a"),
		makeRecord("source-a", "a-2.txt", "blob-bucket-a"),
		makeRecord("source-b", "b-1.txt", "blob-bucket-b"),
		makeRecord("source-b", "b-2.txt", "blob-bucket-b"),
	}
	for _, object := range objects {
		if err := store.RegisterObject(ctx, object); err != nil {
			t.Fatalf("RegisterObject %q/%q: %v", object.Bucket, object.Key, err)
		}
	}

	stats, err := store.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats error: %v", err)
	}
	if stats.UniqueBlobs != 2 || stats.DuplicatesFound != 2 || stats.BytesReclaimable != 2*size {
		t.Fatalf("stats before unregister = %+v, expected 2 blobs, 2 duplicates, %d bytes", stats, 2*size)
	}

	if err := store.UnregisterObject(ctx, "source-a", "a-1.txt"); err != nil {
		t.Fatalf("UnregisterObject error: %v", err)
	}

	stats, err = store.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats after unregister error: %v", err)
	}
	if stats.UniqueBlobs != 2 || stats.DuplicatesFound != 1 || stats.BytesReclaimable != size {
		t.Errorf("stats after unregister = %+v, expected 2 blobs, 1 duplicate, %d bytes", stats, size)
	}
}

func TestRegisterObjectMovesReferenceBetweenBlobBuckets(t *testing.T) {
	const (
		hash = "same-hash"
		size = int64(100)
	)

	ctx := context.Background()
	store := openTestStore(t)
	makeRecord := func(key, blobBucket string) cache.ObjectRecord {
		record := record("source", key, hash, size)
		record.BlobBucket = blobBucket
		return record
	}

	moving := makeRecord("moving.txt", "blob-bucket-a")
	objects := []cache.ObjectRecord{
		moving,
		makeRecord("anchor-a.txt", "blob-bucket-a"),
		makeRecord("anchor-b.txt", "blob-bucket-b"),
	}
	for _, object := range objects {
		if err := store.RegisterObject(ctx, object); err != nil {
			t.Fatalf("RegisterObject %q/%q: %v", object.Bucket, object.Key, err)
		}
	}

	moving.BlobBucket = "blob-bucket-b"
	moving.ETag = "etag-moved"
	moving.LastSeenScan = "scan-2"
	if err := store.RegisterObject(ctx, moving); err != nil {
		t.Fatalf("move object reference: %v", err)
	}

	if err := store.UnregisterObject(ctx, "source", "anchor-a.txt"); err != nil {
		t.Fatalf("UnregisterObject anchor-a.txt: %v", err)
	}

	stats, err := store.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats error: %v", err)
	}
	if stats.UniqueBlobs != 1 || stats.DuplicatesFound != 1 || stats.BytesReclaimable != size {
		t.Errorf("stats after moving reference = %+v, expected 1 blob, 1 duplicate, %d bytes", stats, size)
	}
}

func TestFinalizeScopeDecrementsOnlyMatchingBlobBucket(t *testing.T) {
	const (
		hash = "same-hash"
		size = int64(100)
	)

	ctx := context.Background()
	store := openTestStore(t)
	makeRecord := func(key, blobBucket, scanID string) cache.ObjectRecord {
		record := record("source", key, hash, size)
		record.BlobBucket = blobBucket
		record.LastSeenScan = scanID
		return record
	}

	objects := []cache.ObjectRecord{
		makeRecord("docs/stale-a.txt", "blob-bucket-a", "scan-1"),
		makeRecord("docs/current-a.txt", "blob-bucket-a", "scan-2"),
		makeRecord("docs/current-b-one.txt", "blob-bucket-b", "scan-2"),
		makeRecord("docs/current-b-two.txt", "blob-bucket-b", "scan-2"),
	}
	for _, object := range objects {
		if err := store.RegisterObject(ctx, object); err != nil {
			t.Fatalf("RegisterObject %q/%q: %v", object.Bucket, object.Key, err)
		}
	}

	removed, err := store.FinalizeScope(ctx, "source", "docs/", "scan-2")
	if err != nil {
		t.Fatalf("FinalizeScope error: %v", err)
	}
	if removed != 1 {
		t.Fatalf("FinalizeScope removed = %d, expected 1", removed)
	}

	for _, key := range []string{"docs/current-b-one.txt", "docs/current-b-two.txt"} {
		if err := store.UnregisterObject(ctx, "source", key); err != nil {
			t.Errorf("UnregisterObject %q after FinalizeScope: %v", key, err)
		}
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
		BlobBucket:   bucket,
		BlobKey:      "blobs/" + hash,
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
