package scanner

import (
	"encoding/json"
	"path/filepath"
	"s3-dedup/internal/cache"
	"s3-dedup/internal/config"
	"s3-dedup/internal/hashing"
	"s3-dedup/internal/logger"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v6"
)

func newTestScanner(
	t *testing.T,
	client S3Client,
	store cache.Store,
	cfg *config.Config,
) *Scanner {
	t.Helper()

	logging, err := logger.New("error", filepath.Join(t.TempDir(), "scanner.log"))
	if err != nil {
		t.Fatalf("logger.New error: %v", err)
	}
	t.Cleanup(func() {
		if err := logging.Close(); err != nil {
			t.Errorf("logger Close error: %v", err)
		}
	})
	return NewScanner(client, store, cfg, logging)
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
			BlobBucket:   "bucket",
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
		HashAlgo:     "sha256",
		LastSeenScan: "scan-1",
		State:        cache.ObjectStateReported,
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
	return hashContentWithAlgo(t, content, "sha256")
}

func hashContentWithAlgo(t *testing.T, content, hashAlgo string) string {
	t.Helper()
	hash, err := hashing.HashReader(strings.NewReader(content), hashAlgo)
	if err != nil {
		t.Fatalf("HashReader error: %v", err)
	}
	return hash
}

func pointerDocument(t *testing.T, blobBucket, blobKey, hash string, size int64) string {
	t.Helper()
	return pointerDocumentWithAlgo(t, blobBucket, blobKey, "sha256", hash, size)
}

func pointerDocumentWithAlgo(t *testing.T, blobBucket, blobKey, hashAlgo, hash string, size int64) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"blob_bucket":  blobBucket,
		"blob_key":     blobKey,
		"hash_algo":    hashAlgo,
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
