package scanner

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"s3-dedup/internal/cache"
	"s3-dedup/internal/pointer"

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

	scanner := newTestScanner(t, client, store, testConfig())
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
	scanner := newTestScanner(t, client, store, config)
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
	scanner := newTestScanner(t, client, store, config)
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
	result, err := newTestScanner(t, client, store, cfg).ScanOnce(context.Background())
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
	expectedReclaimable := 2 * int64(len(content))
	if result.BytesReclaimable != expectedReclaimable {
		t.Errorf("BytesReclaimable = %d, expected %d", result.BytesReclaimable, expectedReclaimable)
	}
}

func TestPointerModeUniqueObjectsDoNotCreateBlobs(t *testing.T) {
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

	result, err := newTestScanner(t, client, store, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if got := client.countObjectsWithPrefix("bucket", cfg.Dedup.BlobPrefix); got != 0 {
		t.Errorf("blob object count = %d, expected 0", got)
	}
	if got := client.totalPutCalls(); got != 0 {
		t.Errorf("total PutObject calls = %d, expected 0", got)
	}
	if got := client.content(objectID("bucket", "one.txt")); got != firstContent {
		t.Errorf("first original content = %q, expected %q", got, firstContent)
	}
	if got := client.content(objectID("bucket", "two.txt")); got != secondContent {
		t.Errorf("second original content = %q, expected %q", got, secondContent)
	}
	if result.UniqueBlobs != 2 || result.DuplicatesFound != 0 || result.BytesReclaimable != 0 {
		t.Errorf("stats = %+v; expected two unique blobs and no duplicates or reclaimable bytes", result)
	}
}

func TestPointerModeBytesReclaimedIncludesNewBlobCost(t *testing.T) {
	const firstKey = "one.bin"
	const secondKey = "two.bin"
	content := strings.Repeat("x", 2048)

	store := openTestStore(t)
	client := &mockS3Client{
		objects: []minio.ObjectInfo{
			objectInfo(firstKey, int64(len(content))),
			objectInfo(secondKey, int64(len(content))),
		},
		contents: map[string]string{
			objectID("bucket", firstKey):  content,
			objectID("bucket", secondKey): content,
		},
	}
	cfg := pointerTestConfig()
	cfg.Dedup.DeleteOriginals = true

	result, err := newTestScanner(t, client, store, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	firstPointerSize := int64(len(client.content(objectID("bucket", firstKey))))
	secondPointerSize := int64(len(client.content(objectID("bucket", secondKey))))
	expected := int64(len(content)) - firstPointerSize - secondPointerSize
	if result.BytesReclaimed != expected {
		t.Errorf("BytesReclaimed = %d, expected %d", result.BytesReclaimed, expected)
	}
}

func TestPointerModeBytesReclaimedWhenBlobAlreadyExists(t *testing.T) {
	const firstKey = "one.bin"
	const secondKey = "two.bin"
	content := strings.Repeat("y", 2048)
	cfg := pointerTestConfig()
	cfg.Dedup.DeleteOriginals = true
	blobKey := cfg.Dedup.BlobPrefix + hashContent(t, content)

	store := openTestStore(t)
	client := &mockS3Client{
		objects: []minio.ObjectInfo{
			objectInfo(firstKey, int64(len(content))),
			objectInfo(secondKey, int64(len(content))),
		},
		contents: map[string]string{
			objectID("bucket", firstKey):  content,
			objectID("bucket", secondKey): content,
			objectID("bucket", blobKey):   content,
		},
	}

	result, err := newTestScanner(t, client, store, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	firstPointerSize := int64(len(client.content(objectID("bucket", firstKey))))
	secondPointerSize := int64(len(client.content(objectID("bucket", secondKey))))
	expected := 2*int64(len(content)) - firstPointerSize - secondPointerSize
	if result.BytesReclaimed != expected {
		t.Errorf("BytesReclaimed = %d, expected %d", result.BytesReclaimed, expected)
	}
	if got := client.putCallCount("bucket", blobKey); got != 0 {
		t.Errorf("blob PutObject calls = %d, expected 0", got)
	}
}

func TestPointerModeAdvancesBlobReadyObjectsToPointers(t *testing.T) {
	const firstKey = "one.txt"
	const secondKey = "two.txt"
	const content = "duplicate content for state transitions"

	ctx := context.Background()
	store := openTestStore(t)
	client := &mockS3Client{
		objects: []minio.ObjectInfo{
			objectInfo(firstKey, int64(len(content))),
			objectInfo(secondKey, int64(len(content))),
		},
		contents: map[string]string{
			objectID("bucket", firstKey):  content,
			objectID("bucket", secondKey): content,
		},
	}
	cfg := pointerTestConfig()
	scanner := newTestScanner(t, client, store, cfg)

	if _, err := scanner.ScanOnce(ctx); err != nil {
		t.Fatalf("first ScanOnce error: %v", err)
	}
	for _, key := range []string{firstKey, secondKey} {
		info := objectInfo(key, int64(len(content)))
		status, err := store.GetObjectStatus(ctx, "bucket", key, info.ETag, cfg.Dedup.HashAlgo)
		if err != nil {
			t.Fatalf("GetObjectStatus %q: %v", key, err)
		}
		if !status.Unchanged || status.State != cache.ObjectStateBlobReady || status.RefCount != 2 {
			t.Errorf("status for %q = %+v, expected unchanged blob_ready object with ref_count 2", key, status)
		}
	}
	firstPutCalls := client.totalPutCalls()

	if _, err := scanner.ScanOnce(ctx); err != nil {
		t.Fatalf("second ScanOnce error: %v", err)
	}
	if got := client.totalPutCalls(); got != firstPutCalls {
		t.Errorf("PutObject calls for unchanged blob_ready objects = %d, expected %d", got, firstPutCalls)
	}

	cfg.Dedup.DeleteOriginals = true
	result, err := scanner.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("pointer ScanOnce error: %v", err)
	}
	if result.ObjectsRelinked != 2 {
		t.Errorf("ObjectsRelinked = %d, expected 2", result.ObjectsRelinked)
	}
	blobKey := cfg.Dedup.BlobPrefix + hashContent(t, content)
	if got := client.putCallCount("bucket", blobKey); got != 1 {
		t.Errorf("blob PutObject calls = %d, expected 1", got)
	}
	for _, key := range []string{firstKey, secondKey} {
		info := client.stats[objectID("bucket", key)]
		status, err := store.GetObjectStatus(ctx, "bucket", key, info.ETag, cfg.Dedup.HashAlgo)
		if err != nil {
			t.Fatalf("GetObjectStatus %q after relink: %v", key, err)
		}
		if !status.Unchanged || status.State != cache.ObjectStatePointer || status.RefCount != 2 {
			t.Errorf("status for %q = %+v, expected unchanged pointer with ref_count 2", key, status)
		}
	}
}

func TestPointerModeMigratesPointerHashAlgorithmAndCollectsOldBlob(t *testing.T) {
	const firstKey = "one.txt"
	const secondKey = "two.txt"
	const content = "content referenced by legacy pointers"

	ctx := context.Background()
	store := openTestStore(t)
	oldHash := hashContentWithAlgo(t, content, "sha256")
	newHash := hashContentWithAlgo(t, content, "sha512")
	oldBlobKey := "blobs/" + oldHash
	newBlobKey := "blobs/" + newHash
	pointerBody := pointerDocumentWithAlgo(t, "bucket", oldBlobKey, "sha256", oldHash, int64(len(content)))
	firstInfo := pointerObjectInfo(firstKey, pointerBody)
	secondInfo := pointerObjectInfo(secondKey, pointerBody)

	for _, info := range []minio.ObjectInfo{firstInfo, secondInfo} {
		object := record("bucket", info.Key, oldHash, int64(len(content)))
		object.BlobKey = oldBlobKey
		object.ETag = info.ETag
		object.Size = info.Size
		object.LastModified = info.LastModified
		object.HashAlgo = "sha256"
		object.State = cache.ObjectStatePointer
		if err := store.RegisterObject(ctx, object); err != nil {
			t.Fatalf("register legacy pointer %q: %v", info.Key, err)
		}
	}

	client := &mockS3Client{
		objects: []minio.ObjectInfo{firstInfo, secondInfo},
		contents: map[string]string{
			objectID("bucket", firstKey):   pointerBody,
			objectID("bucket", secondKey):  pointerBody,
			objectID("bucket", oldBlobKey): content,
		},
		stats: map[string]minio.ObjectInfo{
			objectID("bucket", firstKey):   withContentType(firstInfo, pointer.ContentPointerType),
			objectID("bucket", secondKey):  withContentType(secondInfo, pointer.ContentPointerType),
			objectID("bucket", oldBlobKey): objectInfo(oldBlobKey, int64(len(content))),
		},
	}
	cfg := pointerTestConfig()
	cfg.Dedup.HashAlgo = "sha512"
	cfg.Dedup.DeleteOriginals = true

	result, err := newTestScanner(t, client, store, cfg).ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if result.Errors != 0 {
		t.Errorf("Errors = %d, expected 0", result.Errors)
	}
	if got := client.content(objectID("bucket", oldBlobKey)); got != "" {
		t.Errorf("old blob still exists with %d bytes", len(got))
	}
	if got := client.content(objectID("bucket", newBlobKey)); got != content {
		t.Errorf("new blob content = %q, expected %q", got, content)
	}
	if got := client.putCallCount("bucket", newBlobKey); got != 1 {
		t.Errorf("new blob PutObject calls = %d, expected 1", got)
	}
	for _, key := range []string{firstKey, secondKey} {
		p, err := pointer.ReadPointer(strings.NewReader(client.content(objectID("bucket", key))))
		if err != nil {
			t.Fatalf("ReadPointer %q: %v", key, err)
		}
		if p.HashAlgo != "sha512" || p.Hash != newHash || p.BlobKey != newBlobKey {
			t.Errorf("migrated pointer %q = %+v", key, p)
		}
	}
	stats, err := store.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats error: %v", err)
	}
	if stats.UniqueBlobs != 1 || stats.DuplicatesFound != 1 || stats.BytesReclaimable != 0 {
		t.Errorf("stats after migration = %+v", stats)
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

	result, err := newTestScanner(t, client, store, cfg).ScanOnce(context.Background())
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

func TestReportOnlyRejectsPointerWhoseBlobSizeDoesNotMatch(t *testing.T) {
	const key = "pointer.txt"
	const blobKey = "blobs/hash"
	const blobContent = "blob content"

	store := openTestStore(t)
	pointerBody := pointerDocument(t, "bucket", blobKey, "hash", int64(len(blobContent)+1))
	pointerInfo := pointerObjectInfo(key, pointerBody)
	client := &mockS3Client{
		objects: []minio.ObjectInfo{pointerInfo},
		contents: map[string]string{
			objectID("bucket", key):     pointerBody,
			objectID("bucket", blobKey): blobContent,
		},
		stats: map[string]minio.ObjectInfo{
			objectID("bucket", key):     withContentType(pointerInfo, pointer.ContentPointerType),
			objectID("bucket", blobKey): objectInfo(blobKey, int64(len(blobContent))),
		},
	}

	result, err := newTestScanner(t, client, store, testConfig()).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if result.Errors != 1 {
		t.Errorf("Errors = %d, expected 1", result.Errors)
	}
	stats, err := store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats error: %v", err)
	}
	if stats != (cache.Stats{}) {
		t.Errorf("cache stats = %+v, expected empty cache", stats)
	}
	if got := client.totalPutCalls(); got != 0 {
		t.Errorf("PutObject calls = %d, expected 0", got)
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

	result, err := newTestScanner(t, client, store, cfg).ScanOnce(context.Background())
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
