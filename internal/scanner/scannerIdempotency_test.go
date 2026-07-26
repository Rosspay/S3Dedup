package scanner

import (
	"context"
	"errors"
	"path/filepath"
	"s3-dedup/internal/cache"
	"s3-dedup/internal/pointer"
	"testing"

	"github.com/minio/minio-go/v6"
)

func TestPointerModeRestartWithSameSQLiteDoesNotRewriteObjects(t *testing.T) {
	const key = "original.txt"
	const content = "persisted cache must survive restart"

	ctx := context.Background()
	storePath := filepath.Join(t.TempDir(), "cache.db")
	firstStore, err := cache.OpenSQLite(storePath)
	if err != nil {
		t.Fatalf("OpenSQLite first store: %v", err)
	}

	cfg := pointerTestConfig()
	cfg.Dedup.DeleteOriginals = true
	client := &mockS3Client{
		objects: []minio.ObjectInfo{objectInfo(key, int64(len(content)))},
		contents: map[string]string{
			objectID("bucket", key): content,
		},
	}

	firstResult, err := NewScanner(client, firstStore, cfg).ScanOnce(ctx)
	if err != nil {
		firstStore.Close()
		t.Fatalf("first ScanOnce error: %v", err)
	}
	if firstResult.Errors != 0 {
		firstStore.Close()
		t.Fatalf("first Errors = %d, expected 0", firstResult.Errors)
	}
	firstStats, err := firstStore.GetStats(ctx)
	if err != nil {
		firstStore.Close()
		t.Fatalf("GetStats before restart: %v", err)
	}
	firstPutCalls := client.totalPutCalls()
	if err := firstStore.Close(); err != nil {
		t.Fatalf("Close first store: %v", err)
	}

	secondStore, err := cache.OpenSQLite(storePath)
	if err != nil {
		t.Fatalf("OpenSQLite second store: %v", err)
	}
	defer func() {
		if err := secondStore.Close(); err != nil {
			t.Errorf("Close second store: %v", err)
		}
	}()

	client.mu.Lock()
	if client.errors == nil {
		client.errors = make(map[string]error)
	}
	client.errors[objectID("bucket", key)] = errors.New("unchanged pointer must not be read")
	client.mu.Unlock()

	secondResult, err := NewScanner(client, secondStore, cfg).ScanOnce(ctx)
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
		t.Errorf("PutObject calls after restart = %d, expected %d", got, firstPutCalls)
	}

	secondStats, err := secondStore.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats after restart: %v", err)
	}
	if secondStats != firstStats {
		t.Errorf("cache stats after restart = %+v, expected %+v", secondStats, firstStats)
	}
}

func TestPointerModeRepeatedScanDoesNotUploadBlobAgain(t *testing.T) {
	const content = "content uploaded once"

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
	cfg.Dedup.DeleteOriginals = true
	scanner := NewScanner(client, store, cfg)

	firstResult, err := scanner.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("first ScanOnce error: %v", err)
	}
	blobKey := cfg.Dedup.BlobPrefix + hashContent(t, content)
	firstBlobPutCalls := client.putCallCount("bucket", blobKey)
	firstTotalPutCalls := client.totalPutCalls()
	firstStats, err := store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats after first scan: %v", err)
	}

	secondResult, err := scanner.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("second ScanOnce error: %v", err)
	}
	secondStats, err := store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats after second scan: %v", err)
	}

	if firstBlobPutCalls != 1 {
		t.Errorf("blob PutObject calls after first scan = %d, expected 1", firstBlobPutCalls)
	}
	if got := client.putCallCount("bucket", blobKey); got != firstBlobPutCalls {
		t.Errorf("blob PutObject calls after repeated scan = %d, expected %d", got, firstBlobPutCalls)
	}
	if got := client.totalPutCalls(); got != firstTotalPutCalls {
		t.Errorf("total PutObject calls after repeated scan = %d, expected %d", got, firstTotalPutCalls)
	}
	if secondResult.ObjectsRelinked != 0 {
		t.Errorf("second ObjectsRelinked = %d, expected 0", secondResult.ObjectsRelinked)
	}
	if secondResult.BytesReclaimed != 0 {
		t.Errorf("second BytesReclaimed = %d, expected 0", secondResult.BytesReclaimed)
	}
	if secondResult.UniqueBlobs != firstResult.UniqueBlobs ||
		secondResult.DuplicatesFound != firstResult.DuplicatesFound ||
		secondResult.BytesReclaimable != firstResult.BytesReclaimable {
		t.Errorf("second report stats = %+v, expected first report stats %+v", secondResult, firstResult)
	}
	if secondStats != firstStats {
		t.Errorf("cache stats after second scan = %+v, expected %+v", secondStats, firstStats)
	}
}
func TestPointerModeNextScanRecognizesPointerWithoutCreatingBlobAgain(t *testing.T) {
	const key = "document.txt"
	const content = "content converted to pointer"

	firstStore := openTestStore(t)
	info := objectInfo(key, int64(len(content)))
	client := &mockS3Client{
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
