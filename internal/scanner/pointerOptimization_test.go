package scanner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"s3-dedup/internal/cache"

	"github.com/minio/minio-go/v6"
)

type countingDiscoveryStore struct {
	cache.Store
	batchCalls    atomic.Int64
	registerCalls atomic.Int64
}

func (s *countingDiscoveryStore) ApplyDiscoveryBatch(ctx context.Context, mutations []cache.DiscoveryMutation) error {
	s.batchCalls.Add(1)
	return s.Store.ApplyDiscoveryBatch(ctx, mutations)
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
	if got := countingStore.registerCalls.Load(); got != 0 {
		t.Fatalf("individual RegisterObject calls = %d, expected 0", got)
	}
}
