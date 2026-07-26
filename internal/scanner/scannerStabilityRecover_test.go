package scanner

import (
	"context"
	"errors"
	"s3-dedup/internal/cache"
	"s3-dedup/internal/config"
	"s3-dedup/internal/pointer"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v6"
)

type failRegisterStore struct {
	cache.Store
	mu        sync.Mutex
	remaining int
	err       error
}

func (s *failRegisterStore) RegisterObject(ctx context.Context, object cache.ObjectRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.remaining > 0 {
		s.remaining--
		return s.err
	}
	return s.Store.RegisterObject(ctx, object)
}

func TestPointerModePointerSurvivesCacheFailureAndRestoresReference(t *testing.T) {
	const key = "original.txt"
	const content = "pointer must restore a missing cache reference"

	ctx := context.Background()
	store := openTestStore(t)
	cfg := pointerTestConfig()
	cfg.Dedup.DeleteOriginals = true
	client := &mockS3Client{
		objects: []minio.ObjectInfo{objectInfo(key, int64(len(content)))},
		contents: map[string]string{
			objectID("bucket", key): content,
		},
	}
	failingStore := &failRegisterStore{
		Store:     store,
		remaining: 1,
		err:       errors.New("simulated cache commit error"),
	}

	firstResult, err := newTestScanner(t, client, failingStore, cfg).ScanOnce(ctx)
	if err != nil {
		t.Fatalf("first ScanOnce error: %v", err)
	}
	if firstResult.Errors != 1 {
		t.Errorf("first Errors = %d, expected 1", firstResult.Errors)
	}
	pointerID := objectID("bucket", key)
	if got := client.stats[pointerID].ContentType; got != pointer.ContentPointerType {
		t.Fatalf("object ContentType after cache failure = %q, expected %q", got, pointer.ContentPointerType)
	}
	firstPutCalls := client.totalPutCalls()
	if firstPutCalls != 2 {
		t.Errorf("first PutObject calls = %d, expected 2", firstPutCalls)
	}

	stats, err := store.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats after cache failure: %v", err)
	}
	if stats.UniqueBlobs != 0 || stats.DuplicatesFound != 0 || stats.BytesReclaimable != 0 {
		t.Errorf("cache changed despite RegisterObject failure: %+v", stats)
	}

	secondResult, err := newTestScanner(t, client, store, cfg).ScanOnce(ctx)
	if err != nil {
		t.Fatalf("second ScanOnce error: %v", err)
	}
	if secondResult.Errors != 0 {
		t.Errorf("second Errors = %d, expected 0", secondResult.Errors)
	}
	if secondResult.ObjectsRelinked != 0 {
		t.Errorf("second ObjectsRelinked = %d, expected 0", secondResult.ObjectsRelinked)
	}
	if got := client.totalPutCalls(); got != firstPutCalls {
		t.Errorf("PutObject calls after recovery = %d, expected %d", got, firstPutCalls)
	}

	stats, err = store.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats after recovery: %v", err)
	}
	if stats.UniqueBlobs != 1 || stats.DuplicatesFound != 0 {
		t.Errorf("cache reference was not restored: %+v", stats)
	}
}

func TestCollectGarbageKeepsFailedBlobAndDeletesSuccessfulBlob(t *testing.T) {
	const (
		firstBucket   = "bucket-a"
		secondBucket  = "bucket-b"
		firstKey      = "blobs/hash-a"
		secondKey     = "blobs/hash-b"
		firstHash     = "hash-a"
		secondHash    = "hash-b"
		firstContent  = "first orphan"
		secondContent = "second orphan"
	)

	ctx := context.Background()
	store := openTestStore(t)
	firstRecord := record(firstBucket, "source-a.txt", firstHash, int64(len(firstContent)))
	firstRecord.BlobKey = firstKey
	secondRecord := record(secondBucket, "source-b.txt", secondHash, int64(len(secondContent)))
	secondRecord.BlobKey = secondKey
	for _, object := range []cache.ObjectRecord{firstRecord, secondRecord} {
		if err := store.RegisterObject(ctx, object); err != nil {
			t.Fatalf("RegisterObject %q: %v", object.Key, err)
		}
		if err := store.UnregisterObject(ctx, object.Bucket, object.Key); err != nil {
			t.Fatalf("UnregisterObject %q: %v", object.Key, err)
		}
	}

	client := &mockS3Client{
		contents: map[string]string{
			objectID(firstBucket, firstKey):   firstContent,
			objectID(secondBucket, secondKey): secondContent,
		},
		removeErrs: map[string]map[string]error{
			secondBucket: {
				secondKey: errors.New("simulated S3 delete error"),
			},
		},
	}
	cfg := pointerTestConfig()
	cfg.S3.Buckets = []config.Bucket{
		{Name: firstBucket},
		{Name: secondBucket},
	}

	bytesReclaimed, blobsRemoved, err := newTestScanner(t, client, store, cfg).collectGarbage(ctx)
	if err == nil {
		t.Fatal("collectGarbage error = nil, expected partial deletion error")
	}
	if bytesReclaimed != int64(len(firstContent)) {
		t.Errorf("bytesReclaimed = %d, expected %d", bytesReclaimed, len(firstContent))
	}
	if blobsRemoved != 1 {
		t.Errorf("blobsRemoved = %d, expected 1", blobsRemoved)
	}
	if got := client.content(objectID(firstBucket, firstKey)); got != "" {
		t.Errorf("successfully deleted blob still exists: %q", got)
	}
	if got := client.content(objectID(secondBucket, secondKey)); got != secondContent {
		t.Errorf("failed blob content = %q, expected %q", got, secondContent)
	}

	firstBlobs, err := store.ListUnreferencedBlobs(ctx, firstBucket)
	if err != nil {
		t.Fatalf("ListUnreferencedBlobs first bucket: %v", err)
	}
	if len(firstBlobs) != 0 {
		t.Errorf("first bucket has %d unreferenced blobs, expected 0", len(firstBlobs))
	}
	secondBlobs, err := store.ListUnreferencedBlobs(ctx, secondBucket)
	if err != nil {
		t.Fatalf("ListUnreferencedBlobs second bucket: %v", err)
	}
	if len(secondBlobs) != 1 || secondBlobs[0].Key != secondKey {
		t.Errorf("second bucket unreferenced blobs = %+v, expected only %q", secondBlobs, secondKey)
	}

	client.mu.RLock()
	firstCalls := append([][]string(nil), client.removeCalls[firstBucket]...)
	secondCalls := append([][]string(nil), client.removeCalls[secondBucket]...)
	client.mu.RUnlock()
	if len(firstCalls) != 1 || len(firstCalls[0]) != 1 || firstCalls[0][0] != firstKey {
		t.Errorf("RemoveObjects calls for first bucket = %v", firstCalls)
	}
	if len(secondCalls) != 1 || len(secondCalls[0]) != 1 || secondCalls[0][0] != secondKey {
		t.Errorf("RemoveObjects calls for second bucket = %v", secondCalls)
	}
}

func TestScanOnceCancellationDuringListingDoesNotFinalizeOrCollectGarbage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := openTestStore(t)
	stale := record("bucket", "stale.txt", "stale-hash", 100)
	if err := store.RegisterObject(ctx, stale); err != nil {
		t.Fatalf("RegisterObject stale object: %v", err)
	}

	client := &mockS3Client{
		objects: []minio.ObjectInfo{
			objectInfo("first.txt", 100),
			objectInfo("second.txt", 100),
		},
		contents: map[string]string{
			objectID("bucket", "first.txt"):  "first",
			objectID("bucket", "second.txt"): "second",
		},
		listHook: func(processed int) {
			if processed == 1 {
				cancel()
			}
		},
	}

	result, err := newTestScanner(t, client, store, testConfig()).ScanOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ScanOnce error = %v, expected context.Canceled", err)
	}
	if result.Errors == 0 {
		t.Error("Errors = 0, expected cancellation error")
	}

	unchanged, err := store.IsObjectUnchanged(
		context.Background(),
		stale.Bucket,
		stale.Key,
		stale.ETag,
		stale.Size,
		stale.LastModified,
	)
	if err != nil {
		t.Fatalf("IsObjectUnchanged stale object: %v", err)
	}
	if !unchanged {
		t.Error("stale object was finalized after incomplete listing")
	}

	client.mu.RLock()
	removeCalls := len(client.removeCalls)
	client.mu.RUnlock()
	if removeCalls != 0 {
		t.Errorf("RemoveObjects bucket count = %d, expected 0", removeCalls)
	}
}

func TestScanOnceListObjectsErrorNoFinalize(t *testing.T) {
	store := openTestStore(t)
	someObj := record("bucket", "some.txt", "hash", 100)
	if err := store.RegisterObject(context.Background(), someObj); err != nil {
		t.Fatal(err)
	}

	listErr := errors.New("Error listing objects in \"bucket\":")
	client := &mockS3Client{
		listErr:  listErr,
		contents: make(map[string]string),
		errors:   make(map[string]error),
	}

	scanner := newTestScanner(t, client, store, testConfig())
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
	client := &mockS3Client{
		objects: []minio.ObjectInfo{
			objectInfo("some.txt", 100),
		},
		contents: make(map[string]string),
		errors: map[string]error{
			objectID("bucket", "some.txt"): someErr,
		},
	}

	scanner := newTestScanner(t, client, store, testConfig())
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
	client := &mockS3Client{
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
func TestPointerModeUploadErrorKeepsOriginalAndDoesNotRegister(t *testing.T) {
	const content = "must remain untouched"
	const key = "original.txt"

	store := openTestStore(t)
	cfg := pointerTestConfig()
	blobKey := cfg.Dedup.BlobPrefix + hashContent(t, content)
	putErr := errors.New("simulated blob upload error")
	client := &mockS3Client{
		objects: []minio.ObjectInfo{objectInfo(key, int64(len(content)))},
		contents: map[string]string{
			objectID("bucket", key): content,
		},
		putErrors: map[string]error{
			objectID("bucket", blobKey): putErr,
		},
	}

	result, err := newTestScanner(t, client, store, cfg).ScanOnce(context.Background())
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
func TestPointerModeExistingPointersRestoreCacheWithoutCreatingBlob(t *testing.T) {
	const blobContent = "shared logical content"
	const blobKey = "blobs/shared-hash"

	store := openTestStore(t)
	pointerBody := pointerDocument(t, "bucket", blobKey, "shared-hash", int64(len(blobContent)))
	firstInfo := pointerObjectInfo("one.txt", pointerBody)
	secondInfo := pointerObjectInfo("two.txt", pointerBody)
	client := &mockS3Client{
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

	result, err := newTestScanner(t, client, store, cfg).ScanOnce(context.Background())
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
func TestPointerModeChangedObjectAfterListingIsNotReplaced(t *testing.T) {
	const key = "document.txt"
	const originalContent = "old-data"
	const changedContent = "new-data"

	store := openTestStore(t)
	listedInfo := objectInfo(key, int64(len(originalContent)))
	client := &mockS3Client{
		objects: []minio.ObjectInfo{listedInfo},
		contents: map[string]string{
			objectID("bucket", key): originalContent,
		},
		stats: map[string]minio.ObjectInfo{
			objectID("bucket", key): listedInfo,
		},
	}
	client.statHooks = map[string]func(*mockS3Client, int){
		objectID("bucket", key): func(m *mockS3Client, call int) {
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

	result, err := newTestScanner(t, client, store, cfg).ScanOnce(context.Background())
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
	stats, err := store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats error: %v", err)
	}
	if stats.UniqueBlobs != 0 || stats.DuplicatesFound != 0 || stats.BytesReclaimable != 0 {
		t.Errorf("changed object was registered with stale metadata: %+v", stats)
	}
}
func TestPointerModeCreatedBlobSurvivesPointerWriteFailureAndIsReused(t *testing.T) {
	const key = "original.txt"
	const content = "blob must be reused after pointer failure"

	store := openTestStore(t)
	cfg := pointerTestConfig()
	cfg.Dedup.DeleteOriginals = true
	hash := hashContent(t, content)
	blobKey := cfg.Dedup.BlobPrefix + hash
	pointerID := objectID("bucket", key)
	client := &mockS3Client{
		objects: []minio.ObjectInfo{objectInfo(key, int64(len(content)))},
		contents: map[string]string{
			pointerID: content,
		},
		putErrors: map[string]error{
			pointerID: errors.New("simulated pointer upload error"),
		},
	}
	scanner := newTestScanner(t, client, store, cfg)

	firstResult, err := scanner.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("first ScanOnce error: %v", err)
	}
	if firstResult.Errors != 1 {
		t.Errorf("first Errors = %d, expected 1", firstResult.Errors)
	}
	if got := client.content(pointerID); got != content {
		t.Errorf("original content after pointer failure = %q, expected %q", got, content)
	}
	if got := client.content(objectID("bucket", blobKey)); got != content {
		t.Errorf("created blob content = %q, expected %q", got, content)
	}
	if got := client.putCallCount("bucket", blobKey); got != 1 {
		t.Errorf("blob PutObject calls after first scan = %d, expected 1", got)
	}
	stats, err := store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats after failed scan: %v", err)
	}
	if stats.UniqueBlobs != 0 || stats.DuplicatesFound != 0 || stats.BytesReclaimable != 0 {
		t.Errorf("cache changed after pointer failure: %+v", stats)
	}

	delete(client.putErrors, pointerID)
	secondResult, err := scanner.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("second ScanOnce error: %v", err)
	}
	if secondResult.Errors != 0 {
		t.Errorf("second Errors = %d, expected 0", secondResult.Errors)
	}
	if secondResult.ObjectsRelinked != 1 {
		t.Errorf("second ObjectsRelinked = %d, expected 1", secondResult.ObjectsRelinked)
	}
	if got := client.putCallCount("bucket", blobKey); got != 1 {
		t.Errorf("blob was uploaded again: PutObject calls = %d, expected 1", got)
	}
	if got := client.stats[pointerID].ContentType; got != pointer.ContentPointerType {
		t.Errorf("object ContentType after retry = %q, expected %q", got, pointer.ContentPointerType)
	}
	stats, err = store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats after retry: %v", err)
	}
	if stats.UniqueBlobs != 1 || stats.DuplicatesFound != 0 {
		t.Errorf("cache was not restored after retry: %+v", stats)
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
