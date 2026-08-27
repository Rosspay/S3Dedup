package scanner

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"s3-dedup/internal/cache"
	"s3-dedup/internal/config"
	"s3-dedup/internal/pointer"

	"github.com/minio/minio-go/v6"
)

type countingDiscoveryStore struct {
	cache.Store
	batchCalls        atomic.Int64
	bulkLookupCalls   atomic.Int64
	bulkLookupObjects atomic.Int64
	dedupBatchCalls   atomic.Int64
	dedupBatchRecords atomic.Int64
	registerCalls     atomic.Int64
	singleLookupCalls atomic.Int64
}

func (s *countingDiscoveryStore) ApplyDiscoveryBatch(ctx context.Context, mutations []cache.DiscoveryMutation) error {
	s.batchCalls.Add(1)
	return s.Store.ApplyDiscoveryBatch(ctx, mutations)
}

func (s *countingDiscoveryStore) GetObjectStatus(
	ctx context.Context,
	bucket string,
	key string,
	etag string,
	size int64,
	hashAlgo string,
	lastModified time.Time,
) (cache.ObjectStatus, error) {
	s.singleLookupCalls.Add(1)
	return s.Store.GetObjectStatus(
		ctx,
		bucket,
		key,
		etag,
		size,
		hashAlgo,
		lastModified,
	)
}

func (s *countingDiscoveryStore) GetObjectStatuses(
	ctx context.Context,
	bucket string,
	objects []cache.ObjectMetadata,
	hashAlgo string,
) ([]cache.ObjectStatus, error) {
	s.bulkLookupCalls.Add(1)
	s.bulkLookupObjects.Add(int64(len(objects)))
	return s.Store.GetObjectStatuses(ctx, bucket, objects, hashAlgo)
}

func (s *countingDiscoveryStore) ApplyDedupBatch(ctx context.Context, objects []cache.ObjectRecord) error {
	s.dedupBatchCalls.Add(1)
	s.dedupBatchRecords.Add(int64(len(objects)))
	return s.Store.ApplyDedupBatch(ctx, objects)
}

func (s *countingDiscoveryStore) RegisterObject(ctx context.Context, object cache.ObjectRecord) error {
	s.registerCalls.Add(1)
	return s.Store.RegisterObject(ctx, object)
}

type failingDiscoveryStore struct {
	cache.Store
	err error
}

func (s *failingDiscoveryStore) ApplyDiscoveryBatch(context.Context, []cache.DiscoveryMutation) error {
	return s.err
}

type failingBulkLookupStore struct {
	cache.Store
	err error
}

func (s *failingBulkLookupStore) GetObjectStatuses(
	context.Context,
	string,
	[]cache.ObjectMetadata,
	string,
) ([]cache.ObjectStatus, error) {
	return nil, s.err
}

func TestPointerModeListsS3Once(t *testing.T) {
	const content = "duplicate content for one-list pointer scan"
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
	store := openTestStore(t)
	cfg := pointerTestConfig()

	result, err := newTestScanner(t, client, store, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if result.Errors != 0 {
		t.Fatalf("Errors = %d, expected 0", result.Errors)
	}
	client.mu.RLock()
	listCalls := client.listCalls
	client.mu.RUnlock()
	if listCalls != len(cfg.S3.Buckets) {
		t.Fatalf("ListObjects calls = %d, expected %d", listCalls, len(cfg.S3.Buckets))
	}
}

func TestPointerDiscoveryUsesBulkLookupPages(t *testing.T) {
	const objectCount = discoveryLookupPageSize + 1
	client := &mockS3Client{
		objects:  make([]minio.ObjectInfo, 0, objectCount),
		contents: make(map[string]string, objectCount),
	}
	for index := 0; index < objectCount; index++ {
		key := fmt.Sprintf("object-%04d.txt", index)
		content := fmt.Sprintf("unique-content-%04d", index)
		client.objects = append(client.objects, objectInfo(key, int64(len(content))))
		client.contents[objectID("bucket", key)] = content
	}
	store := openTestStore(t)
	countingStore := &countingDiscoveryStore{Store: store}
	cfg := pointerTestConfig()
	cfg.Schedule.Workers = 64

	result, err := newTestScanner(t, client, countingStore, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if result.Errors != 0 || result.UniqueBlobs != objectCount {
		t.Fatalf("result = %+v, expected %d unique objects without errors", result, objectCount)
	}
	if got := countingStore.bulkLookupCalls.Load(); got != 2 {
		t.Fatalf("GetObjectStatuses calls = %d, expected 2", got)
	}
	if got := countingStore.bulkLookupObjects.Load(); got != objectCount {
		t.Fatalf("GetObjectStatuses objects = %d, expected %d", got, objectCount)
	}
	if got := countingStore.singleLookupCalls.Load(); got != 0 {
		t.Fatalf("GetObjectStatus calls = %d, expected 0", got)
	}
	if got := client.totalGetCalls(); got != objectCount {
		t.Fatalf("GetObject calls = %d, expected %d", got, objectCount)
	}
	if got := client.totalStatCalls(); got != objectCount {
		t.Fatalf("StatObject calls = %d, expected %d", got, objectCount)
	}
}

func TestPointerDiscoveryBulkLookupSkipsUnchangedObjects(t *testing.T) {
	const cachedContent = "cached-content"
	const newContent = "new-content"
	cachedInfo := objectInfo("cached.txt", int64(len(cachedContent)))
	newInfo := objectInfo("new.txt", int64(len(newContent)))
	client := &mockS3Client{
		objects: []minio.ObjectInfo{cachedInfo, newInfo},
		contents: map[string]string{
			objectID("bucket", cachedInfo.Key): cachedContent,
			objectID("bucket", newInfo.Key):    newContent,
		},
	}
	store := openTestStore(t)
	cached := record(
		"bucket",
		cachedInfo.Key,
		hashContent(t, cachedContent),
		cachedInfo.Size,
	)
	cached.ETag = cachedInfo.ETag
	cached.LastModified = cachedInfo.LastModified
	if err := store.RegisterObject(context.Background(), cached); err != nil {
		t.Fatalf("RegisterObject error: %v", err)
	}
	countingStore := &countingDiscoveryStore{Store: store}

	result, err := newTestScanner(
		t,
		client,
		countingStore,
		pointerTestConfig(),
	).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if result.Errors != 0 || result.UniqueBlobs != 2 {
		t.Fatalf("result = %+v, expected two unique objects without errors", result)
	}
	if got := countingStore.bulkLookupCalls.Load(); got != 1 {
		t.Fatalf("GetObjectStatuses calls = %d, expected 1", got)
	}
	if got := countingStore.singleLookupCalls.Load(); got != 0 {
		t.Fatalf("GetObjectStatus calls = %d, expected 0", got)
	}
	if got := client.totalGetCalls(); got != 1 {
		t.Fatalf("GetObject calls = %d, expected only the new object", got)
	}
	if got := client.totalStatCalls(); got != 1 {
		t.Fatalf("StatObject calls = %d, expected only the new object", got)
	}
}

func TestPointerDiscoveryBulkLookupErrorDoesNotFinalizeScope(t *testing.T) {
	lookupErr := errors.New("bulk lookup failed")
	client := &mockS3Client{
		objects: []minio.ObjectInfo{objectInfo("new.txt", 10)},
		contents: map[string]string{
			objectID("bucket", "new.txt"): "new-content",
		},
	}
	store := openTestStore(t)
	stale := record("bucket", "stale.txt", "stale-hash", 10)
	if err := store.RegisterObject(context.Background(), stale); err != nil {
		t.Fatalf("RegisterObject error: %v", err)
	}
	failingStore := &failingBulkLookupStore{Store: store, err: lookupErr}

	_, err := newTestScanner(
		t,
		client,
		failingStore,
		pointerTestConfig(),
	).ScanOnce(context.Background())
	if !errors.Is(err, lookupErr) {
		t.Fatalf("ScanOnce error = %v, expected %v", err, lookupErr)
	}
	if got := client.totalGetCalls(); got != 0 {
		t.Fatalf("GetObject calls = %d, expected 0", got)
	}
	status, statusErr := store.GetObjectStatus(
		context.Background(),
		stale.Bucket,
		stale.Key,
		stale.ETag,
		stale.Size,
		stale.HashAlgo,
		stale.LastModified,
	)
	if statusErr != nil {
		t.Fatalf("GetObjectStatus error: %v", statusErr)
	}
	if !status.Unchanged {
		t.Fatalf("stale status = %+v, expected scope not to be finalized", status)
	}
}

func TestPointerDiscoverySmallObjectsBypassBulkLookupAndAreUnregistered(t *testing.T) {
	const content = "small"
	info := objectInfo("small.txt", int64(len(content)))
	client := &mockS3Client{
		objects: []minio.ObjectInfo{info},
		contents: map[string]string{
			objectID("bucket", info.Key): content,
		},
	}
	store := openTestStore(t)
	cached := record("bucket", info.Key, hashContent(t, content), info.Size)
	cached.ETag = info.ETag
	cached.LastModified = info.LastModified
	if err := store.RegisterObject(context.Background(), cached); err != nil {
		t.Fatalf("RegisterObject error: %v", err)
	}
	countingStore := &countingDiscoveryStore{Store: store}
	cfg := pointerTestConfig()
	cfg.Dedup.MinSizeBytes = 100

	result, err := newTestScanner(t, client, countingStore, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if result.Errors != 0 || result.UniqueBlobs != 0 {
		t.Fatalf("result = %+v, expected no cached objects", result)
	}
	if got := countingStore.bulkLookupCalls.Load(); got != 0 {
		t.Fatalf("GetObjectStatuses calls = %d, expected 0", got)
	}
	if got := client.totalGetCalls(); got != 0 {
		t.Fatalf("GetObject calls = %d, expected 0", got)
	}
	status, statusErr := store.GetObjectStatus(
		context.Background(),
		cached.Bucket,
		cached.Key,
		cached.ETag,
		cached.Size,
		cached.HashAlgo,
		cached.LastModified,
	)
	if statusErr != nil {
		t.Fatalf("GetObjectStatus error: %v", statusErr)
	}
	if status.Unchanged {
		t.Fatalf("status = %+v, expected small object to be unregistered", status)
	}
}

func TestPointerModeAcceptsListAndStatTimestampPrecisionDifference(t *testing.T) {
	const content = "unique content"
	listed := objectInfo("one.txt", int64(len(content)))
	listed.LastModified = listed.LastModified.Add(789 * time.Millisecond)
	stat := listed
	stat.LastModified = stat.LastModified.Truncate(time.Second)
	client := &mockS3Client{
		objects: []minio.ObjectInfo{listed},
		contents: map[string]string{
			objectID("bucket", listed.Key): content,
		},
		stats: map[string]minio.ObjectInfo{
			objectID("bucket", listed.Key): stat,
		},
	}
	store := openTestStore(t)
	cfg := pointerTestConfig()

	result, err := newTestScanner(t, client, store, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if result.Errors != 0 || result.UniqueBlobs != 1 {
		t.Fatalf("result = %+v, expected one unique object without errors", result)
	}
	status, err := store.GetObjectStatus(
		context.Background(),
		"bucket",
		listed.Key,
		listed.ETag,
		listed.Size,
		cfg.Dedup.HashAlgo,
		listed.LastModified,
	)
	if err != nil {
		t.Fatalf("GetObjectStatus error: %v", err)
	}
	if !status.Unchanged {
		t.Fatalf("status = %+v, expected cached object to be unchanged", status)
	}
}

func TestPointerModeStopsWhenDiscoveryBatchFails(t *testing.T) {
	const content = "content that cannot be committed"
	client := &mockS3Client{
		objects: []minio.ObjectInfo{objectInfo("one.txt", int64(len(content)))},
		contents: map[string]string{
			objectID("bucket", "one.txt"): content,
		},
	}
	store := openTestStore(t)
	expectedErr := errors.New("simulated batch commit error")
	failingStore := &failingDiscoveryStore{Store: store, err: expectedErr}

	_, err := newTestScanner(t, client, failingStore, pointerTestConfig()).ScanOnce(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("ScanOnce error = %v, expected %v", err, expectedErr)
	}
	stats, statsErr := store.GetStats(context.Background())
	if statsErr != nil {
		t.Fatalf("GetStats error: %v", statsErr)
	}
	if stats != (cache.Stats{}) {
		t.Fatalf("cache stats = %+v, expected empty cache", stats)
	}
}

func TestPointerModeUniqueObjectsHaveNoDedupCandidates(t *testing.T) {
	const firstContent = "first unique content"
	const secondContent = "second unique content"
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
	store := openTestStore(t)
	countingStore := &countingDiscoveryStore{Store: store}
	cfg := pointerTestConfig()

	result, err := newTestScanner(t, client, countingStore, cfg).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if result.UniqueBlobs != 2 || result.DuplicatesFound != 0 {
		t.Fatalf("result = %+v, expected two unique blobs", result)
	}
	if got := client.totalPutCalls(); got != 0 {
		t.Fatalf("PutObject calls = %d, expected 0", got)
	}
	if got := countingStore.batchCalls.Load(); got != 1 {
		t.Fatalf("ApplyDiscoveryBatch calls = %d, expected 1", got)
	}
	if got := countingStore.dedupBatchCalls.Load(); got != 0 {
		t.Fatalf("ApplyDedupBatch calls = %d, expected 0", got)
	}
	if got := countingStore.registerCalls.Load(); got != 0 {
		t.Fatalf("individual RegisterObject calls = %d, expected 0", got)
	}
}

func TestPointerModeBatchesDeduplicationCacheUpdates(t *testing.T) {
	const content = "duplicate content committed in one batch"
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
	store := openTestStore(t)
	countingStore := &countingDiscoveryStore{Store: store}

	result, err := newTestScanner(t, client, countingStore, pointerTestConfig()).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if result.Errors != 0 {
		t.Fatalf("Errors = %d, expected 0", result.Errors)
	}
	if got := countingStore.dedupBatchCalls.Load(); got != 1 {
		t.Fatalf("ApplyDedupBatch calls = %d, expected 1", got)
	}
	if got := countingStore.dedupBatchRecords.Load(); got != 2 {
		t.Fatalf("ApplyDedupBatch records = %d, expected 2", got)
	}
	if got := countingStore.registerCalls.Load(); got != 0 {
		t.Fatalf("individual RegisterObject calls = %d, expected 0", got)
	}
}

func TestPointerDeduplicationDownloadsOneRepresentativePerBlobAcrossBuckets(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	cfg := pointerTestConfig()
	cfg.S3.Buckets = []config.Bucket{
		{Name: "first-bucket"},
		{Name: "second-bucket"},
	}
	cfg.Dedup.BlobBucket = "blob-bucket"
	cfg.Dedup.DeleteOriginals = true

	type logicalObject struct {
		bucket  string
		key     string
		content string
	}
	objects := []logicalObject{
		{bucket: "first-bucket", key: "alpha-one.txt", content: "alpha content"},
		{bucket: "first-bucket", key: "alpha-two.txt", content: "alpha content"},
		{bucket: "second-bucket", key: "alpha-three.txt", content: "alpha content"},
		{bucket: "second-bucket", key: "beta-one.txt", content: "beta content"},
		{bucket: "second-bucket", key: "beta-two.txt", content: "beta content"},
	}
	client := &mockS3Client{contents: make(map[string]string)}
	for _, object := range objects {
		info := objectInfo(object.key, int64(len(object.content)))
		client.contents[objectID(object.bucket, object.key)] = object.content
		hash := hashContent(t, object.content)
		record := record(object.bucket, object.key, hash, info.Size)
		record.BlobBucket = cfg.Dedup.BlobBucket
		record.BlobKey = cfg.Dedup.BlobPrefix + hash
		record.ETag = info.ETag
		record.LastModified = info.LastModified
		if err := store.RegisterObject(ctx, record); err != nil {
			t.Fatalf("RegisterObject %q/%q: %v", object.bucket, object.key, err)
		}
	}

	atomics := &atomicReportPart{}
	scanner := newTestScanner(t, client, store, cfg)
	if err := scanner.pointerDeduplication(ctx, atomics, 4, "scan-2"); err != nil {
		t.Fatalf("pointerDeduplication error: %v", err)
	}
	if atomics.processErrors.Load() != 0 {
		t.Fatalf("processErrors = %d, expected 0", atomics.processErrors.Load())
	}
	if got := client.totalGetCalls(); got != 2 {
		t.Fatalf("GetObject calls = %d, expected one representative for each of two blobs", got)
	}
	for _, content := range []string{"alpha content", "beta content"} {
		blobKey := cfg.Dedup.BlobPrefix + hashContent(t, content)
		if got := client.statCallCount(cfg.Dedup.BlobBucket, blobKey); got != 1 {
			t.Errorf("StatObject calls for blob %q = %d, expected 1", blobKey, got)
		}
	}
	for _, object := range objects {
		if got := objectContentType(t, client, object.bucket, object.key); got != pointer.ContentPointerType {
			t.Errorf("ContentType for %q/%q = %q, expected pointer", object.bucket, object.key, got)
		}
	}
}

func TestPointerDeduplicationDoesNotDownloadCandidatesWhenBlobExists(t *testing.T) {
	const content = "already materialized content"
	ctx := context.Background()
	store := openTestStore(t)
	cfg := pointerTestConfig()
	cfg.Dedup.BlobBucket = "blob-bucket"
	cfg.Dedup.DeleteOriginals = true
	hash := hashContent(t, content)
	blobKey := cfg.Dedup.BlobPrefix + hash
	client := &mockS3Client{contents: map[string]string{
		objectID(cfg.Dedup.BlobBucket, blobKey): content,
	}}
	for _, key := range []string{"one.txt", "two.txt", "three.txt"} {
		info := objectInfo(key, int64(len(content)))
		client.contents[objectID("bucket", key)] = content
		record := record("bucket", key, hash, info.Size)
		record.BlobBucket = cfg.Dedup.BlobBucket
		record.BlobKey = blobKey
		record.ETag = info.ETag
		record.LastModified = info.LastModified
		if err := store.RegisterObject(ctx, record); err != nil {
			t.Fatalf("RegisterObject %q: %v", key, err)
		}
	}

	atomics := &atomicReportPart{}
	if err := newTestScanner(t, client, store, cfg).pointerDeduplication(ctx, atomics, 3, "scan-2"); err != nil {
		t.Fatalf("pointerDeduplication error: %v", err)
	}
	if atomics.processErrors.Load() != 0 {
		t.Fatalf("processErrors = %d, expected 0", atomics.processErrors.Load())
	}
	if got := client.totalGetCalls(); got != 0 {
		t.Fatalf("GetObject calls = %d, expected 0 for an existing blob", got)
	}
	if got := client.putCallCount(cfg.Dedup.BlobBucket, blobKey); got != 0 {
		t.Fatalf("blob PutObject calls = %d, expected 0", got)
	}
	if got := client.statCallCount(cfg.Dedup.BlobBucket, blobKey); got != 1 {
		t.Fatalf("blob StatObject calls = %d, expected one shared existence check", got)
	}
	for _, key := range []string{"one.txt", "two.txt", "three.txt"} {
		if got := objectContentType(t, client, "bucket", key); got != pointer.ContentPointerType {
			t.Errorf("ContentType for %q = %q, expected pointer", key, got)
		}
	}
}

func TestPointerDeduplicationDoesNotTrustHashAfterMetadataChange(t *testing.T) {
	const content = "stable duplicate content"
	ctx := context.Background()
	store := openTestStore(t)
	cfg := pointerTestConfig()
	cfg.Dedup.DeleteOriginals = true
	hash := hashContent(t, content)
	client := &mockS3Client{contents: map[string]string{
		objectID("bucket", "one.txt"): content,
		objectID("bucket", "two.txt"): content,
	}}
	for _, key := range []string{"one.txt", "two.txt"} {
		info := objectInfo(key, int64(len(content)))
		record := record("bucket", key, hash, info.Size)
		record.ETag = info.ETag
		record.LastModified = info.LastModified
		if err := store.RegisterObject(ctx, record); err != nil {
			t.Fatalf("RegisterObject %q: %v", key, err)
		}
	}
	changed := objectInfo("one.txt", int64(len(content)))
	changed.ETag = "changed-etag"
	client.statHooks = map[string]func(*mockS3Client, int){
		objectID("bucket", "one.txt"): func(client *mockS3Client, call int) {
			if call == 1 {
				if client.stats == nil {
					client.stats = make(map[string]minio.ObjectInfo)
				}
				client.stats[objectID("bucket", "one.txt")] = changed
			}
		},
	}

	atomics := &atomicReportPart{}
	if err := newTestScanner(t, client, store, cfg).pointerDeduplication(ctx, atomics, 1, "scan-2"); err != nil {
		t.Fatalf("pointerDeduplication error: %v", err)
	}
	if atomics.processErrors.Load() != 0 {
		t.Fatalf("processErrors = %d, expected 0", atomics.processErrors.Load())
	}
	if got := client.getCallCount("bucket", "one.txt"); got != 0 {
		t.Fatalf("changed candidate GetObject calls = %d, expected 0", got)
	}
	if got := objectContentType(t, client, "bucket", "one.txt"); got == pointer.ContentPointerType {
		t.Fatal("metadata-changed candidate was replaced using a stale cached hash")
	}
	if got := objectContentType(t, client, "bucket", "two.txt"); got != pointer.ContentPointerType {
		t.Fatalf("stable duplicate ContentType = %q, expected pointer", got)
	}
}

func TestPointerDeduplicationRetriesMaterializationWithAnotherStableCandidate(t *testing.T) {
	const content = "duplicate content with changing representative"
	ctx := context.Background()
	store := openTestStore(t)
	cfg := pointerTestConfig()
	cfg.Dedup.DeleteOriginals = true
	hash := hashContent(t, content)
	client := &mockS3Client{contents: map[string]string{
		objectID("bucket", "one.txt"): content,
		objectID("bucket", "two.txt"): content,
	}}
	for _, key := range []string{"one.txt", "two.txt"} {
		info := objectInfo(key, int64(len(content)))
		record := record("bucket", key, hash, info.Size)
		record.ETag = info.ETag
		record.LastModified = info.LastModified
		if err := store.RegisterObject(ctx, record); err != nil {
			t.Fatalf("RegisterObject %q: %v", key, err)
		}
	}
	changed := objectInfo("one.txt", int64(len(content)))
	changed.ETag = "changed-during-read"
	client.getHooks = map[string]func(*mockS3Client, int){
		objectID("bucket", "one.txt"): func(client *mockS3Client, call int) {
			if call == 1 {
				if client.stats == nil {
					client.stats = make(map[string]minio.ObjectInfo)
				}
				client.stats[objectID("bucket", "one.txt")] = changed
			}
		},
	}

	atomics := &atomicReportPart{}
	if err := newTestScanner(t, client, store, cfg).pointerDeduplication(ctx, atomics, 1, "scan-2"); err != nil {
		t.Fatalf("pointerDeduplication error: %v", err)
	}
	if atomics.processErrors.Load() != 0 {
		t.Fatalf("processErrors = %d, expected 0", atomics.processErrors.Load())
	}
	if got := client.totalGetCalls(); got != 2 {
		t.Fatalf("GetObject calls = %d, expected changed representative and one retry", got)
	}
	if got := objectContentType(t, client, "bucket", "one.txt"); got == pointer.ContentPointerType {
		t.Fatal("representative changed during read but was replaced")
	}
	if got := objectContentType(t, client, "bucket", "two.txt"); got != pointer.ContentPointerType {
		t.Fatalf("stable fallback candidate ContentType = %q, expected pointer", got)
	}
	blobKey := cfg.Dedup.BlobPrefix + hash
	if got := client.content(objectID(cfg.Dedup.BlobBucket, blobKey)); got != content {
		t.Fatalf("materialized blob content = %q, expected %q", got, content)
	}
}

func TestPointerDeduplicationCancellationDoesNotDeadlock(t *testing.T) {
	const content = "duplicate content"
	firstInfo := objectInfo("one.txt", int64(len(content)))
	secondInfo := objectInfo("two.txt", int64(len(content)))
	client := &mockS3Client{
		objects: []minio.ObjectInfo{firstInfo, secondInfo},
		contents: map[string]string{
			objectID("bucket", firstInfo.Key):  content,
			objectID("bucket", secondInfo.Key): content,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	client.statHooks = map[string]func(*mockS3Client, int){
		objectID("bucket", firstInfo.Key): func(_ *mockS3Client, call int) {
			if call == 1 {
				cancel()
			}
		},
	}
	store := openTestStore(t)
	hash := hashContent(t, content)
	for _, info := range []minio.ObjectInfo{firstInfo, secondInfo} {
		object := record("bucket", info.Key, hash, info.Size)
		object.ETag = info.ETag
		object.LastModified = info.LastModified
		if err := store.RegisterObject(context.Background(), object); err != nil {
			t.Fatalf("RegisterObject error: %v", err)
		}
	}
	scanner := newTestScanner(t, client, store, pointerTestConfig())
	done := make(chan error, 1)
	go func() {
		done <- scanner.pointerDeduplication(ctx, &atomicReportPart{}, 1, "scan")
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pointerDeduplication error = %v, expected context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pointerDeduplication deadlocked after cancellation")
	}
}

func TestPointerModeReportsOnlyCurrentConfiguredBucket(t *testing.T) {
	store := openTestStore(t)
	firstContent := "first bucket duplicate"
	firstClient := &mockS3Client{
		objects: []minio.ObjectInfo{
			objectInfo("one.txt", int64(len(firstContent))),
			objectInfo("two.txt", int64(len(firstContent))),
		},
		contents: map[string]string{
			objectID("first-bucket", "one.txt"): firstContent,
			objectID("first-bucket", "two.txt"): firstContent,
		},
	}
	firstConfig := pointerTestConfig()
	firstConfig.S3.Buckets[0].Name = "first-bucket"

	firstResult, err := newTestScanner(t, firstClient, store, firstConfig).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("first ScanOnce error: %v", err)
	}
	if firstResult.UniqueBlobs != 1 || firstResult.DuplicatesFound != 1 {
		t.Fatalf("first result = %+v, expected one duplicate group", firstResult)
	}

	secondContent := "second bucket unique"
	secondClient := &mockS3Client{
		objects: []minio.ObjectInfo{objectInfo("only.txt", int64(len(secondContent)))},
		contents: map[string]string{
			objectID("second-bucket", "only.txt"): secondContent,
		},
	}
	secondConfig := pointerTestConfig()
	secondConfig.S3.Buckets[0].Name = "second-bucket"

	secondResult, err := newTestScanner(t, secondClient, store, secondConfig).ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("second ScanOnce error: %v", err)
	}
	if secondResult.UniqueBlobs != 1 || secondResult.DuplicatesFound != 0 || secondResult.BytesReclaimable != 0 {
		t.Fatalf("second result = %+v, expected statistics only for second-bucket", secondResult)
	}
}
