package scanner

import (
	"context"
	"s3-dedup/internal/pointer"
	"strconv"
	"testing"

	"github.com/minio/minio-go/v6"
)

func TestScanOnceFindDuplicateContent(t *testing.T) {
	const content = "duplicate"
	const expObjectsScanned = 2
	const expUniqueBlobs = 1
	const expDuplicatesFound = 1
	const expBytesReclaimable = int64(len(content))
	const expErrors = 0
	const expMode = "report_only"

	store := openTestStore(t)
	client := &mockS3Client{
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
	client := &mockS3Client{
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
	client := &mockS3Client{
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

func TestPointerModeDuplicateObjectsCreateOneBlob(t *testing.T) {
	const content = "same pointer-mode content"

	store := openTestStore(t)
	client := &mockS3Client{
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

func TestPointerModeDifferentContentsCreateDifferentBlobs(t *testing.T) {
	const firstContent = "first content"
	const secondContent = "second content"

	store := openTestStore(t)
	client := &mockS3Client{
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

func TestPointerModeObjectsInsideBlobPrefixAreNotScanned(t *testing.T) {
	const blobKey = "blobs/existing-hash"
	const content = "already stored blob"

	store := openTestStore(t)
	client := &mockS3Client{
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

func TestPointerModePointerCanReferenceBlobInDifferentBucket(t *testing.T) {
	const sourceBucket = "source-bucket"
	const blobBucket = "blob-bucket"
	const blobContent = "cross-bucket content"
	const blobKey = "blobs/cross-bucket-hash"
	const pointerKey = "document.txt"

	store := openTestStore(t)
	pointerBody := pointerDocument(t, blobBucket, blobKey, "cross-bucket-hash", int64(len(blobContent)))
	pointerInfo := pointerObjectInfo(pointerKey, pointerBody)
	client := &mockS3Client{
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
