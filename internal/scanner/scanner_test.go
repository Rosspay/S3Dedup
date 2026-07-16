package scanner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"s3-dedup/internal/cache"
	"s3-dedup/internal/config"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v6"
)

// Mocking S3 client for testing purposes
// so we can simulate basic behavior and errors
type MockS3Client struct {
	objects  []minio.ObjectInfo
	contents map[string]string
	errors   map[string]error
	listErr  error
}

func (m *MockS3Client) ListObjects(
	ctx context.Context,
	bucket string,
	prefix string,
	recursive bool,
	fn func(minio.ObjectInfo) error,
) error {
	if m.listErr != nil {
		return m.listErr
	}
	for _, object := range m.objects {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !strings.HasPrefix(object.Key, prefix) {
			continue
		}
		if err := fn(object); err != nil {
			return err
		}

	}
	return nil
}

func (m *MockS3Client) GetObject(
	ctx context.Context,
	bucket string,
	key string,
) (io.ReadCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	id := objectID(bucket, key)
	if err := m.errors[id]; err != nil {
		return nil, err
	}

	content, ok := m.contents[id]
	if !ok {
		return nil, fmt.Errorf("object %q/%q not found", bucket, key)
	}
	return io.NopCloser(strings.NewReader(content)), nil
}

func TestScanOnceFindDuplicateContent(t *testing.T) {
	const content = "duplicate"
	const expObjectsScanned = 2
	const expUniqueBlobs = 1
	const expDuplicatesFound = 1
	const expBytesReclaimable = int64(len(content))
	const expErrors = 0
	const expMode = "report_only"

	store := openTestStore(t)
	client := &MockS3Client{
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

	scanner := NewScanner(client, store, testConfig())
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

func TestScanOnceListObjectsErrorNoFinalize(t *testing.T) {
	store := openTestStore(t)
	someObj := record("bucket", "some.txt", "hash", 100)
	if err := store.RegisterObject(context.Background(), someObj); err != nil {
		t.Fatal(err)
	}

	listErr := errors.New("Error listing objects in \"bucket\":")
	client := &MockS3Client{
		listErr:  listErr,
		contents: make(map[string]string),
		errors:   make(map[string]error),
	}

	scanner := NewScanner(client, store, testConfig())
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
	client := &MockS3Client{
		objects: []minio.ObjectInfo{
			objectInfo("some.txt", 100),
		},
		contents: make(map[string]string),
		errors: map[string]error{
			objectID("bucket", "some.txt"): someErr,
		},
	}

	scanner := NewScanner(client, store, testConfig())
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
	client := &MockS3Client{
		objects:  objs,
		contents: contents,
		errors:   make(map[string]error),
	}
	config := testConfig()
	config.Schedule.Workers = 4
	scanner := NewScanner(client, store, config)
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
	client := &MockS3Client{
		objects:  objs,
		contents: contents,
		errors:   make(map[string]error),
	}

	config := testConfig()
	config.Schedule.Workers = 6
	scanner := NewScanner(client, store, config)
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
	client := &MockS3Client{
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
	scanner := NewScanner(client, store, config)
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
			Mode:         "report_only",
		},
	}
}

func record(bucket, key, hash string, size int64) cache.ObjectRecord {
	return cache.ObjectRecord{
		Bucket:       bucket,
		Key:          key,
		ETag:         "etag",
		Size:         size,
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
