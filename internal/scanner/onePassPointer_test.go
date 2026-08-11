package scanner

import (
	"context"
	"s3-dedup/internal/pointer"
	"testing"

	"github.com/minio/minio-go/v6"
)

func TestOnePassPointerConvertsExistingUniqueWhenDuplicateArrives(t *testing.T) {
	const (
		firstKey  = "first.txt"
		secondKey = "second.txt"
		content   = "duplicate content arriving in a later scan"
	)

	ctx := context.Background()
	store := openTestStore(t)
	cfg := pointerTestConfig()
	cfg.Dedup.DeleteOriginals = true
	cfg.Schedule.Workers = 4
	client := &mockS3Client{
		objects: []minio.ObjectInfo{objectInfo(firstKey, int64(len(content)))},
		contents: map[string]string{
			objectID("bucket", firstKey): content,
		},
	}
	scanner := newTestScanner(t, client, store, cfg)

	firstResult, err := scanner.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("first ScanOnce error: %v", err)
	}
	if firstResult.ObjectsRelinked != 0 {
		t.Fatalf("first ObjectsRelinked = %d, expected 0", firstResult.ObjectsRelinked)
	}
	if got := client.putCallCount("bucket", firstKey); got != 0 {
		t.Fatalf("first object PutObject calls = %d, expected 0", got)
	}

	client.mu.Lock()
	client.objects = append(client.objects, objectInfo(secondKey, int64(len(content))))
	client.contents[objectID("bucket", secondKey)] = content
	client.mu.Unlock()

	secondResult, err := scanner.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("second ScanOnce error: %v", err)
	}
	if secondResult.ObjectsRelinked != 2 {
		t.Errorf("second ObjectsRelinked = %d, expected 2", secondResult.ObjectsRelinked)
	}
	for _, key := range []string{firstKey, secondKey} {
		info, err := client.StatObject(ctx, "bucket", key)
		if err != nil {
			t.Fatalf("StatObject %q: %v", key, err)
		}
		if info.ContentType != pointer.ContentPointerType {
			t.Errorf("%q ContentType = %q, expected pointer", key, info.ContentType)
		}
	}
}

func TestOnePassPointerConvertsOnlyNewOriginalInExistingPointerGroup(t *testing.T) {
	const (
		firstKey  = "first.txt"
		secondKey = "second.txt"
		thirdKey  = "third.txt"
		content   = "content shared by existing pointers and a new original"
	)

	ctx := context.Background()
	store := openTestStore(t)
	cfg := pointerTestConfig()
	cfg.Dedup.DeleteOriginals = true
	cfg.Schedule.Workers = 4
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
	scanner := newTestScanner(t, client, store, cfg)

	firstResult, err := scanner.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("first ScanOnce error: %v", err)
	}
	if firstResult.ObjectsRelinked != 2 {
		t.Fatalf("first ObjectsRelinked = %d, expected 2", firstResult.ObjectsRelinked)
	}
	firstCalls := client.putCallCount("bucket", firstKey)
	secondCalls := client.putCallCount("bucket", secondKey)

	client.mu.Lock()
	client.objects = append(client.objects, objectInfo(thirdKey, int64(len(content))))
	client.contents[objectID("bucket", thirdKey)] = content
	client.mu.Unlock()

	secondResult, err := scanner.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("second ScanOnce error: %v", err)
	}
	if secondResult.ObjectsRelinked != 1 {
		t.Errorf("second ObjectsRelinked = %d, expected 1", secondResult.ObjectsRelinked)
	}
	if got := client.putCallCount("bucket", firstKey); got != firstCalls {
		t.Errorf("first pointer rewritten: calls = %d, expected %d", got, firstCalls)
	}
	if got := client.putCallCount("bucket", secondKey); got != secondCalls {
		t.Errorf("second pointer rewritten: calls = %d, expected %d", got, secondCalls)
	}
	if got := client.putCallCount("bucket", thirdKey); got != 1 {
		t.Errorf("third original PutObject calls = %d, expected 1", got)
	}
}

type bucketedS3Client struct {
	*mockS3Client
	listings map[string][]minio.ObjectInfo
}

func (c *bucketedS3Client) ListObjects(
	ctx context.Context,
	bucket string,
	prefix string,
	recursive bool,
	fn func(minio.ObjectInfo) error,
) error {
	for _, info := range c.listings[bucket] {
		if err := fn(info); err != nil {
			return err
		}
	}
	return nil
}

func TestOnePassPointerFindsDuplicatesAcrossBuckets(t *testing.T) {
	const content = "cross bucket duplicate"
	ctx := context.Background()
	store := openTestStore(t)
	cfg := pointerTestConfig()
	cfg.S3.Buckets[0].Name = "source-a"
	cfg.S3.Buckets = append(cfg.S3.Buckets, cfg.S3.Buckets[0])
	cfg.S3.Buckets[1].Name = "source-b"
	cfg.Dedup.BlobBucket = "blob-bucket"
	cfg.Dedup.DeleteOriginals = true
	cfg.Schedule.Workers = 4

	first := objectInfo("first.txt", int64(len(content)))
	second := objectInfo("second.txt", int64(len(content)))
	client := &bucketedS3Client{
		mockS3Client: &mockS3Client{contents: map[string]string{
			objectID("source-a", first.Key):  content,
			objectID("source-b", second.Key): content,
		}},
		listings: map[string][]minio.ObjectInfo{
			"source-a": {first},
			"source-b": {second},
		},
	}

	result, err := newTestScanner(t, client, store, cfg).ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if result.ObjectsRelinked != 2 {
		t.Errorf("ObjectsRelinked = %d, expected 2", result.ObjectsRelinked)
	}
	for bucket, key := range map[string]string{"source-a": first.Key, "source-b": second.Key} {
		info, err := client.StatObject(ctx, bucket, key)
		if err != nil {
			t.Fatalf("StatObject %q/%q: %v", bucket, key, err)
		}
		if info.ContentType != pointer.ContentPointerType {
			t.Errorf("%q/%q ContentType = %q, expected pointer", bucket, key, info.ContentType)
		}
	}
}
