package cache

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestRegisteringFirstObject(t *testing.T) {
	store := openTestStore(t)
	register(t, store, record("bucket", "one.txt", "hash", 100))

	assertRefCount(t, store, "hash", 1)
	assertStats(t, store, Stats{UniqueBlobs: 1})
}

func TestRegisterObjectDuplicate(t *testing.T) {
	store := openTestStore(t)
	register(t, store, record("bucket", "one.txt", "sameHash", 100))
	register(t, store, record("bucket", "two.txt", "sameHash", 100))

	assertRefCount(t, store, "sameHash", 2)
	assertStats(t, store, Stats{
		UniqueBlobs:      1,
		DuplicatesFound:  1,
		BytesReclaimable: 100,
	})
}

func TestRegisterObjectDifferent(t *testing.T) {
	store := openTestStore(t)
	register(t, store, record("bucket", "one.txt", "hash", 100))
	register(t, store, record("bucket", "two.txt", "diffHash", 125))

	assertRefCount(t, store, "hash", 1)
	assertRefCount(t, store, "diffHash", 1)

	assertStats(t, store, Stats{UniqueBlobs: 2})
}

func TestRegisterObjectRepeatedPassIsIdempotent(t *testing.T) {
	store := openTestStore(t)
	object := record("bucket", "one.txt", "hash", 100)
	register(t, store, object)

	object.ETag = "new-etag"
	object.LastSeenScan = "scan-2"
	register(t, store, object)

	assertRefCount(t, store, "hash", 1)
	assertStats(t, store, Stats{UniqueBlobs: 1})
}

func TestRegisterObjectContentChanged(t *testing.T) {
	store := openTestStore(t)
	register(t, store, record("bucket", "one.txt", "hash", 100))
	register(t, store, record("bucket", "one.txt", "newHash", 200))

	assertRefCount(t, store, "hash", 0)
	assertRefCount(t, store, "newHash", 1)
	assertStats(t, store, Stats{UniqueBlobs: 1})
}

func TestRegisterObjectMoreDupes(t *testing.T) {
	store := openTestStore(t)
	register(t, store, record("bucket", "one.txt", "sameHash", 100))
	register(t, store, record("bucket", "two.txt", "sameHash", 100))
	register(t, store, record("bucket", "three.txt", "sameHash", 100))

	assertRefCount(t, store, "sameHash", 3)
	assertStats(t, store, Stats{
		UniqueBlobs:      1,
		DuplicatesFound:  2,
		BytesReclaimable: 200,
	})
}

func TestRegisterObjectDupesDiffBuckets(t *testing.T) {
	store := openTestStore(t)
	register(t, store, record("bucket", "one.txt", "hash", 100))
	register(t, store, record("diffBucket", "one.txt", "hash", 100))

	assertRefCount(t, store, "hash", 2)
	assertStats(t, store, Stats{
		UniqueBlobs:      1,
		DuplicatesFound:  1,
		BytesReclaimable: 100,
	})
}

func TestOpenSQLiteErrors(t *testing.T) {
	_, err := OpenSQLite("")
	if err == nil {
		t.Fatal("Must be error path is empty")
	}
	//TODO other errors
}

func TestFinalizeScopeObjectIsNotDiscoveredSameHash(t *testing.T) {
	store := openTestStore(t)
	register(t, store, record("bucket", "one.txt", "hash", 100))
	register(t, store, record("bucket", "two.txt", "hash", 100))
	err := store.MarkObjectSeen(context.Background(), "bucket", "one.txt", "scan-2")
	if err != nil {
		t.Fatalf("Error marking an object: %v", err)
	}
	removed, err := store.FinalizeScope(context.Background(), "bucket", "", "scan-2")
	if err != nil {
		t.Fatalf("Error finalizing scope: %v", err)
	}
	if removed != 1 {
		t.Fatalf("Object was not removed during scope finalize")
	}
	assertRefCount(t, store, "hash", 1)
	assertStats(t, store, Stats{
		UniqueBlobs:      1,
		DuplicatesFound:  0,
		BytesReclaimable: 0,
	})
}

func TestFinalizeScopeObjectIsNotDiscoveredDiffHash(t *testing.T) {
	store := openTestStore(t)
	register(t, store, record("bucket", "one.txt", "hash", 100))
	register(t, store, record("bucket", "two.txt", "diffHash", 100))
	err := store.MarkObjectSeen(context.Background(), "bucket", "one.txt", "scan-2")
	if err != nil {
		t.Fatalf("Error marking an object: %v", err)
	}
	removed, err := store.FinalizeScope(context.Background(), "bucket", "", "scan-2")
	if err != nil {
		t.Fatalf("Error finalizing scope: %v", err)
	}
	if removed != 1 {
		t.Fatalf("Object was not removed during scope finalize")
	}
	assertRefCount(t, store, "hash", 1)
	assertRefCount(t, store, "diffHash", 0)
	assertStats(t, store, Stats{
		UniqueBlobs:      1,
		DuplicatesFound:  0,
		BytesReclaimable: 0,
	})
}

func TestFinalizeScopeIsIdempotent(t *testing.T) {
	store := openTestStore(t)
	register(t, store, record("bucket", "one.txt", "hash", 100))
	register(t, store, record("bucket", "two.txt", "hash", 100))
	removed, err := store.FinalizeScope(context.Background(), "bucket", "", "scan-1")
	if err != nil {
		t.Fatalf("Error finalizing scope: %v", err)
	}
	if removed != 0 {
		t.Fatalf("Object shouldn't be removed")
	}
	assertRefCount(t, store, "hash", 2)
	assertStats(t, store, Stats{
		UniqueBlobs:      1,
		DuplicatesFound:  1,
		BytesReclaimable: 100,
	})
	removed, err = store.FinalizeScope(context.Background(), "bucket", "", "scan-1")
	if err != nil {
		t.Fatalf("Error finalizing scope: %v", err)
	}
	if removed != 0 {
		t.Fatalf("Object shouldn't be removed")
	}
	assertRefCount(t, store, "hash", 2)
	assertStats(t, store, Stats{
		UniqueBlobs:      1,
		DuplicatesFound:  1,
		BytesReclaimable: 100,
	})
}

func TestFinalizeScopeDoesNotAffectDiffPrefix(t *testing.T) {
	store := openTestStore(t)
	register(t, store, record("bucket", "2026/one.txt", "hash", 100))
	register(t, store, record("bucket", "2025/one.txt", "hash", 100))
	err := store.MarkObjectSeen(context.Background(), "bucket", "2026/one.txt", "scan-2")
	if err != nil {
		t.Fatalf("Error marking an object: %v", err)
	}
	removed, err := store.FinalizeScope(context.Background(), "bucket", "2025/", "scan-2")
	if err != nil {
		t.Fatalf("Error finalizing scope: %v", err)
	}
	if removed == 2 {
		t.Fatalf("Finalize scope affected different prefix")
	}
	assertRefCount(t, store, "hash", 1)
	assertStats(t, store, Stats{
		UniqueBlobs:      1,
		DuplicatesFound:  0,
		BytesReclaimable: 0,
	})

}

func openTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	store, err := OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("OpenSQLite error: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Store Close error: %v", err)
		}
	})
	return store
}

func register(t *testing.T, store *SQLiteStore, object ObjectRecord) {
	t.Helper()
	if err := store.RegisterObject(context.Background(), object); err != nil {
		t.Fatalf("RegisterObject error: %v", err)
	}
}

func record(bucket, key, hash string, size int64) ObjectRecord {
	return ObjectRecord{
		Bucket:       bucket,
		Key:          key,
		ETag:         "etag",
		Size:         size,
		LastModified: time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC),
		Hash:         hash,
		LastSeenScan: "scan-1",
	}
}

func assertRefCount(t *testing.T, store *SQLiteStore, hash string, expected int64) {
	t.Helper()
	var got int64
	if err := store.db.QueryRow(`SELECT ref_count FROM blobs WHERE hash = ?`, hash).Scan(&got); err != nil {
		t.Fatalf("Reading ref_count for hash %q: %v", hash, err)
	}
	if got != expected {
		t.Errorf("Ref_count for hash %q = %d, expected %d", hash, got, expected)
	}
}

func assertStats(t *testing.T, store *SQLiteStore, expected Stats) {
	t.Helper()
	got, err := store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats error: %v", err)
	}
	if got != expected {
		t.Errorf("GetStats = %+v, expected %+v", got, expected)
	}
}
