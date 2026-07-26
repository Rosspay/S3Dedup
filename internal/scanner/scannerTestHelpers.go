package scanner

import (
	"encoding/json"
	"path/filepath"
	"s3-dedup/internal/cache"
	"s3-dedup/internal/config"
	"s3-dedup/internal/hashing"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v6"
)

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
		LastSeenScan: "scan-1",
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
	hash, err := hashing.HashReader(strings.NewReader(content), "sha256")
	if err != nil {
		t.Fatalf("HashReader error: %v", err)
	}
	return hash
}

func pointerDocument(t *testing.T, blobBucket, blobKey, hash string, size int64) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"blob_bucket":  blobBucket,
		"blob_key":     blobKey,
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
