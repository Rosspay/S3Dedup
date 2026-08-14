package scanner

import (
	"context"
	"testing"

	"s3-dedup/internal/pointer"

	"github.com/minio/minio-go/v6"
)

func objectContentType(t *testing.T, client *mockS3Client, bucket, key string) string {
	t.Helper()
	info, err := client.StatObject(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("StatObject %q/%q: %v", bucket, key, err)
	}
	return info.ContentType
}

func TestPointerModeConvertsExistingUniqueObjectWhenDuplicateArrivesLater(t *testing.T) {
	const content = "duplicate arriving later"
	const firstKey = "first.txt"
	const secondKey = "second.txt"

	store := openTestStore(t)
	client := &mockS3Client{
		objects: []minio.ObjectInfo{
			objectInfo(firstKey, int64(len(content))),
		},
		contents: map[string]string{
			objectID("bucket", firstKey): content,
		},
	}
	cfg := pointerTestConfig()
	cfg.Dedup.DeleteOriginals = true
	scanner := newTestScanner(t, client, store, cfg)

	if _, err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("first ScanOnce error: %v", err)
	}
	if got := objectContentType(t, client, "bucket", firstKey); got == pointer.ContentPointerType {
		t.Fatalf("unique object was unexpectedly converted to a pointer")
	}

	client.mu.Lock()
	client.objects = append(client.objects, objectInfo(secondKey, int64(len(content))))
	client.contents[objectID("bucket", secondKey)] = content
	client.mu.Unlock()

	result, err := scanner.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("second ScanOnce error: %v", err)
	}
	if result.ObjectsRelinked != 2 {
		t.Errorf("ObjectsRelinked = %d, expected 2", result.ObjectsRelinked)
	}
	for _, key := range []string{firstKey, secondKey} {
		if got := objectContentType(t, client, "bucket", key); got != pointer.ContentPointerType {
			t.Errorf("ContentType for %q = %q, expected pointer", key, got)
		}
	}
}

func TestPointerModeConvertsNewOriginalInExistingPointerGroup(t *testing.T) {
	const content = "same content"
	keys := []string{"first.txt", "second.txt"}

	store := openTestStore(t)
	client := &mockS3Client{
		objects: []minio.ObjectInfo{
			objectInfo(keys[0], int64(len(content))),
			objectInfo(keys[1], int64(len(content))),
		},
		contents: map[string]string{
			objectID("bucket", keys[0]): content,
			objectID("bucket", keys[1]): content,
		},
	}
	cfg := pointerTestConfig()
	cfg.Dedup.DeleteOriginals = true
	scanner := newTestScanner(t, client, store, cfg)

	if _, err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("first ScanOnce error: %v", err)
	}
	firstPutCalls := client.putCallCount("bucket", keys[0])
	secondPutCalls := client.putCallCount("bucket", keys[1])

	thirdKey := "third.txt"
	client.mu.Lock()
	client.objects = append(client.objects, objectInfo(thirdKey, int64(len(content))))
	client.contents[objectID("bucket", thirdKey)] = content
	client.mu.Unlock()

	result, err := scanner.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("second ScanOnce error: %v", err)
	}
	if result.ObjectsRelinked != 1 {
		t.Errorf("ObjectsRelinked = %d, expected 1", result.ObjectsRelinked)
	}
	if got := objectContentType(t, client, "bucket", thirdKey); got != pointer.ContentPointerType {
		t.Errorf("ContentType for new duplicate = %q, expected pointer", got)
	}
	if got := client.putCallCount("bucket", keys[0]); got != firstPutCalls {
		t.Errorf("first pointer was rewritten: PutObject calls = %d, expected %d", got, firstPutCalls)
	}
	if got := client.putCallCount("bucket", keys[1]); got != secondPutCalls {
		t.Errorf("second pointer was rewritten: PutObject calls = %d, expected %d", got, secondPutCalls)
	}
}
