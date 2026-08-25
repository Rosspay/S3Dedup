package scanner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"s3-dedup/internal/cache"

	"github.com/minio/minio-go/v6"
)

type countingDiscoveryStore struct {
	cache.Store
	batchCalls        atomic.Int64
	dedupBatchCalls   atomic.Int64
	dedupBatchRecords atomic.Int64
	registerCalls     atomic.Int64
}

func (s *countingDiscoveryStore) ApplyDiscoveryBatch(ctx context.Context, mutations []cache.DiscoveryMutation) error {
	s.batchCalls.Add(1)
	return s.Store.ApplyDiscoveryBatch(ctx, mutations)
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
