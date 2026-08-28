package cache

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestRegisteringFirstObject(t *testing.T) {
	store := openTestStore(t)
	register(t, store, record("bucket", "one.txt", "hash", 100))

	assertRefCount(t, store, "bucket", "hash", 1)
	assertStats(t, store, Stats{UniqueBlobs: 1})
}

func TestRegisterObjectDuplicate(t *testing.T) {
	store := openTestStore(t)
	register(t, store, record("bucket", "one.txt", "sameHash", 100))
	register(t, store, record("bucket", "two.txt", "sameHash", 100))

	assertRefCount(t, store, "bucket", "sameHash", 2)
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

	assertRefCount(t, store, "bucket", "hash", 1)
	assertRefCount(t, store, "bucket", "diffHash", 1)

	assertStats(t, store, Stats{UniqueBlobs: 2})
}

func TestRegisterObjectRepeatedPassIsIdempotent(t *testing.T) {
	store := openTestStore(t)
	object := record("bucket", "one.txt", "hash", 100)
	register(t, store, object)

	object.ETag = "new-etag"
	object.LastSeenScan = "scan-2"
	register(t, store, object)

	assertRefCount(t, store, "bucket", "hash", 1)
	assertStats(t, store, Stats{UniqueBlobs: 1})
}

func TestRegisterObjectContentChanged(t *testing.T) {
	store := openTestStore(t)
	register(t, store, record("bucket", "one.txt", "hash", 100))
	register(t, store, record("bucket", "one.txt", "newHash", 200))

	assertRefCount(t, store, "bucket", "hash", 0)
	assertRefCount(t, store, "bucket", "newHash", 1)
	assertStats(t, store, Stats{UniqueBlobs: 1})
}

func TestRegisterObjectMoreDupes(t *testing.T) {
	store := openTestStore(t)
	register(t, store, record("bucket", "one.txt", "sameHash", 100))
	register(t, store, record("bucket", "two.txt", "sameHash", 100))
	register(t, store, record("bucket", "three.txt", "sameHash", 100))

	assertRefCount(t, store, "bucket", "sameHash", 3)
	assertStats(t, store, Stats{
		UniqueBlobs:      1,
		DuplicatesFound:  2,
		BytesReclaimable: 200,
	})
}

func TestRegisterObjectSameHashDifferentBlobBuckets(t *testing.T) {
	store := openTestStore(t)
	register(t, store, record("bucket", "one.txt", "hash", 100))
	register(t, store, record("diffBucket", "one.txt", "hash", 100))

	assertRefCount(t, store, "bucket", "hash", 1)
	assertRefCount(t, store, "diffBucket", "hash", 1)
	assertStats(t, store, Stats{UniqueBlobs: 2})
}

func TestOpenSQLiteErrors(t *testing.T) {
	_, err := OpenSQLite("")
	if err == nil {
		t.Fatal("Must be error path is empty")
	}
}

func TestOpenSQLiteUsesMemoryTempStore(t *testing.T) {
	store := openTestStore(t)
	var tempStore int
	if err := store.db.QueryRow(`PRAGMA temp_store`).Scan(&tempStore); err != nil {
		t.Fatalf("read temp_store: %v", err)
	}
	if tempStore != 2 {
		t.Fatalf("temp_store = %d, expected MEMORY (2)", tempStore)
	}
}

func TestGetObjectStatusesSupportsInMemoryDatabase(t *testing.T) {
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite error: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close error: %v", err)
		}
	}()
	object := record("bucket", "one.txt", "hash", 100)
	register(t, store, object)

	statuses, err := store.GetObjectStatuses(
		context.Background(),
		object.Bucket,
		[]ObjectMetadata{{
			Key:          object.Key,
			ETag:         object.ETag,
			Size:         object.Size,
			LastModified: object.LastModified,
		}},
		object.HashAlgo,
	)
	if err != nil {
		t.Fatalf("GetObjectStatuses error: %v", err)
	}
	if len(statuses) != 1 || !statuses[0].Unchanged {
		t.Fatalf("statuses = %+v, expected unchanged object", statuses)
	}
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
	assertRefCount(t, store, "bucket", "hash", 1)
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
	assertRefCount(t, store, "bucket", "hash", 1)
	assertRefCount(t, store, "bucket", "diffHash", 0)
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
	assertRefCount(t, store, "bucket", "hash", 2)
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
	assertRefCount(t, store, "bucket", "hash", 2)
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
	assertRefCount(t, store, "bucket", "hash", 1)
	assertStats(t, store, Stats{
		UniqueBlobs:      1,
		DuplicatesFound:  0,
		BytesReclaimable: 0,
	})

}

func TestGetObjectStatusReturnsStateAndGroupRefCount(t *testing.T) {
	store := openTestStore(t)
	first := record("bucket", "one.txt", "same-hash", 100)
	second := record("bucket", "two.txt", "same-hash", 100)
	register(t, store, first)
	register(t, store, second)

	status, err := store.GetObjectStatus(
		context.Background(),
		first.Bucket,
		first.Key,
		first.ETag,
		first.Size,
		first.HashAlgo,
		first.LastModified,
	)
	if err != nil {
		t.Fatalf("GetObjectStatus error: %v", err)
	}
	if !status.Unchanged || status.State != ObjectStateReported || status.RefCount != 2 {
		t.Errorf("status = %+v, expected unchanged reported object with ref_count 2", status)
	}
}

func TestGetObjectStatusNormalizesLastModifiedToSeconds(t *testing.T) {
	store := openTestStore(t)
	object := record("bucket", "one.txt", "hash", 100)
	object.LastModified = object.LastModified.Add(123 * time.Millisecond)
	register(t, store, object)

	status, err := store.GetObjectStatus(
		context.Background(),
		object.Bucket,
		object.Key,
		object.ETag,
		object.Size,
		object.HashAlgo,
		object.LastModified.Add(700*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("GetObjectStatus error: %v", err)
	}
	if !status.Unchanged {
		t.Errorf("status = %+v, expected sub-second timestamp difference to be ignored", status)
	}
}

func TestGetObjectStatusDifferentHashAlgorithmIsChanged(t *testing.T) {
	store := openTestStore(t)
	object := record("bucket", "one.txt", "hash", 100)
	register(t, store, object)

	status, err := store.GetObjectStatus(
		context.Background(),
		object.Bucket,
		object.Key,
		object.ETag,
		object.Size,
		"sha512",
		object.LastModified,
	)
	if err != nil {
		t.Fatalf("GetObjectStatus error: %v", err)
	}
	if status.Unchanged {
		t.Errorf("status = %+v, expected changed object for a different hash algorithm", status)
	}
}

func TestGetObjectStatusesReturnsMixedResultsInInputOrder(t *testing.T) {
	store := openTestStore(t)
	exact := record("bucket", `folder/'quoted".txt`, "shared-hash", 100)
	exact.LastModified = exact.LastModified.Add(123 * time.Millisecond)
	changedETag := record("bucket", "changed-etag.txt", "shared-hash", 100)
	changedSize := record("bucket", "changed-size.txt", "shared-hash", 100)
	changedTime := record("bucket", "changed-time.txt", "shared-hash", 100)
	for _, object := range []ObjectRecord{exact, changedETag, changedSize, changedTime} {
		register(t, store, object)
	}

	statuses, err := store.GetObjectStatuses(
		context.Background(),
		"bucket",
		[]ObjectMetadata{
			{
				Key:          exact.Key,
				ETag:         exact.ETag,
				Size:         exact.Size,
				LastModified: exact.LastModified.Add(700 * time.Millisecond),
			},
			{
				Key:          changedETag.Key,
				ETag:         "different-etag",
				Size:         changedETag.Size,
				LastModified: changedETag.LastModified,
			},
			{
				Key:          "missing.txt",
				ETag:         "missing-etag",
				Size:         100,
				LastModified: exact.LastModified,
			},
			{
				Key:          changedSize.Key,
				ETag:         changedSize.ETag,
				Size:         changedSize.Size + 1,
				LastModified: changedSize.LastModified,
			},
			{
				Key:          changedTime.Key,
				ETag:         changedTime.ETag,
				Size:         changedTime.Size,
				LastModified: changedTime.LastModified.Add(time.Second),
			},
		},
		"sha256",
	)
	if err != nil {
		t.Fatalf("GetObjectStatuses error: %v", err)
	}
	if len(statuses) != 5 {
		t.Fatalf("statuses length = %d, expected 5", len(statuses))
	}
	if got := statuses[0]; !got.Unchanged || got.State != ObjectStateReported || got.RefCount != 4 {
		t.Errorf("exact status = %+v, expected unchanged reported object with ref_count 4", got)
	}
	for index, status := range statuses[1:] {
		if status.Unchanged {
			t.Errorf("changed status %d = %+v, expected changed object", index+1, status)
		}
	}

	differentAlgo, err := store.GetObjectStatuses(
		context.Background(),
		"bucket",
		[]ObjectMetadata{{
			Key:          exact.Key,
			ETag:         exact.ETag,
			Size:         exact.Size,
			LastModified: exact.LastModified,
		}},
		"sha512",
	)
	if err != nil {
		t.Fatalf("GetObjectStatuses different hash algorithm: %v", err)
	}
	if len(differentAlgo) != 1 || differentAlgo[0].Unchanged {
		t.Fatalf("different hash algorithm statuses = %+v, expected changed object", differentAlgo)
	}
}

func TestGetObjectStatusesUsesReaderDuringWriteTransaction(t *testing.T) {
	store := openTestStore(t)
	object := record("bucket", "one.txt", "hash", 100)
	register(t, store, object)

	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx error: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(
		context.Background(),
		`UPDATE objects SET etag = ? WHERE bucket = ? AND object_key = ?`,
		"uncommitted-etag",
		object.Bucket,
		object.Key,
	); err != nil {
		t.Fatalf("update object in transaction: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	statuses, err := store.GetObjectStatuses(
		ctx,
		object.Bucket,
		[]ObjectMetadata{{
			Key:          object.Key,
			ETag:         object.ETag,
			Size:         object.Size,
			LastModified: object.LastModified,
		}},
		object.HashAlgo,
	)
	if err != nil {
		t.Fatalf("GetObjectStatuses while writer transaction is open: %v", err)
	}
	if len(statuses) != 1 || !statuses[0].Unchanged {
		t.Fatalf("statuses = %+v, expected committed object snapshot", statuses)
	}
}

func TestGetObjectStatusesRejectsDuplicateKeys(t *testing.T) {
	store := openTestStore(t)
	_, err := store.GetObjectStatuses(
		context.Background(),
		"bucket",
		[]ObjectMetadata{{Key: "same.txt"}, {Key: "same.txt"}},
		"sha256",
	)
	if err == nil {
		t.Fatal("GetObjectStatuses expected duplicate key error")
	}
}

func TestRegisterObjectSameBlobUpdatesStateAndHashAlgorithm(t *testing.T) {
	store := openTestStore(t)
	object := record("bucket", "one.txt", "hash", 100)
	register(t, store, object)

	object.HashAlgo = "sha512"
	object.State = ObjectStatePointer
	object.ETag = "pointer-etag"
	object.Size = 64
	object.LastSeenScan = "scan-2"
	register(t, store, object)

	assertRefCount(t, store, "bucket", "hash", 1)
	var hashAlgo string
	var state ObjectState
	if err := store.db.QueryRow(`
		SELECT hash_algo, object_state
		FROM objects
		WHERE bucket = ? AND object_key = ?
	`, object.Bucket, object.Key).Scan(&hashAlgo, &state); err != nil {
		t.Fatalf("read updated object: %v", err)
	}
	if hashAlgo != object.HashAlgo || state != object.State {
		t.Errorf("stored hash_algo/state = %q/%q, expected %q/%q", hashAlgo, state, object.HashAlgo, object.State)
	}
}

func TestApplyDedupBatchRollsBackAllRecordsOnError(t *testing.T) {
	store := openTestStore(t)
	first := record("bucket", "one.txt", "first-hash", 100)
	second := record("bucket", "two.txt", "second-hash", 200)
	register(t, store, first)
	register(t, store, second)

	secondHash := second.Hash
	first.State = ObjectStatePointer
	second.Hash = first.Hash
	second.BlobKey = first.BlobKey
	if err := store.ApplyDedupBatch(context.Background(), []ObjectRecord{first, second}); err == nil {
		t.Fatal("ApplyDedupBatch error = nil, expected blob size mismatch")
	}

	var state ObjectState
	if err := store.db.QueryRow(`
		SELECT object_state
		FROM objects
		WHERE bucket = ? AND object_key = ?
	`, first.Bucket, first.Key).Scan(&state); err != nil {
		t.Fatalf("read first object state: %v", err)
	}
	if state != ObjectStateReported {
		t.Errorf("first object state = %q, expected transaction rollback to %q", state, ObjectStateReported)
	}
	assertRefCount(t, store, first.BlobBucket, first.Hash, 1)
	assertRefCount(t, store, second.BlobBucket, secondHash, 1)
}

func TestGetStatsFiltersConfiguredScopes(t *testing.T) {
	store := openTestStore(t)
	register(t, store, record("first-bucket", "current/one.txt", "first-hash", 100))
	register(t, store, record("first-bucket", "current/two.txt", "first-hash", 100))
	firstShared := record("first-bucket", "other/one.txt", "shared-hash", 200)
	firstShared.BlobBucket = "blob-bucket"
	register(t, store, firstShared)
	secondShared := record("second-bucket", "current/one.txt", "shared-hash", 200)
	secondShared.BlobBucket = "blob-bucket"
	register(t, store, secondShared)

	stats, err := store.GetStats(context.Background(), Scope{Bucket: "first-bucket", Prefix: "current/"})
	if err != nil {
		t.Fatalf("GetStats error: %v", err)
	}
	expected := Stats{UniqueBlobs: 1, DuplicatesFound: 1, BytesReclaimable: 100}
	if stats != expected {
		t.Errorf("GetStats = %+v, expected %+v", stats, expected)
	}

	stats, err = store.GetStats(
		context.Background(),
		Scope{Bucket: "first-bucket", Prefix: "other/"},
		Scope{Bucket: "second-bucket", Prefix: "current/"},
	)
	if err != nil {
		t.Fatalf("GetStats for multiple scopes error: %v", err)
	}
	expected = Stats{UniqueBlobs: 1, DuplicatesFound: 1, BytesReclaimable: 200}
	if stats != expected {
		t.Errorf("GetStats for multiple scopes = %+v, expected %+v", stats, expected)
	}
}

func TestGetStatsTracksOnlyRemainingOriginalObjects(t *testing.T) {
	tests := []struct {
		name     string
		states   []ObjectState
		expected int64
	}{
		{name: "all reported", states: []ObjectState{ObjectStateReported, ObjectStateReported, ObjectStateReported}, expected: 200},
		{name: "blob exists with originals", states: []ObjectState{ObjectStatePointer, ObjectStateReported, ObjectStateReported}, expected: 200},
		{name: "all blob ready", states: []ObjectState{ObjectStateBlobReady, ObjectStateBlobReady, ObjectStateBlobReady}, expected: 300},
		{name: "all pointers", states: []ObjectState{ObjectStatePointer, ObjectStatePointer, ObjectStatePointer}, expected: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := openTestStore(t)
			for i, state := range test.states {
				object := record("bucket", fmt.Sprintf("object-%d", i), "same-hash", 100)
				object.State = state
				register(t, store, object)
			}

			stats, err := store.GetStats(context.Background())
			if err != nil {
				t.Fatalf("GetStats error: %v", err)
			}
			if stats.UniqueBlobs != 1 || stats.DuplicatesFound != 2 || stats.BytesReclaimable != test.expected {
				t.Errorf("GetStats = %+v, expected one blob, two duplicates and %d reclaimable bytes", stats, test.expected)
			}
		})
	}
}

func TestMarkObjectSeenDoesNotDependOnHashAlgorithm(t *testing.T) {
	store := openTestStore(t)
	object := record("bucket", "one.txt", "hash", 100)
	register(t, store, object)

	if err := store.MarkObjectSeen(context.Background(), object.Bucket, object.Key, "scan-2"); err != nil {
		t.Fatalf("MarkObjectSeen error: %v", err)
	}
	removed, err := store.FinalizeScope(context.Background(), object.Bucket, "", "scan-2")
	if err != nil {
		t.Fatalf("FinalizeScope error: %v", err)
	}
	if removed != 0 {
		t.Errorf("FinalizeScope removed %d objects, expected 0", removed)
	}
	assertRefCount(t, store, object.BlobBucket, object.Hash, 1)
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
		State:        ObjectStateReported,
	}
}

func assertRefCount(t *testing.T, store *SQLiteStore, bucket, hash string, expected int64) {
	t.Helper()
	var got int64
	if err := store.db.QueryRow(`
		SELECT ref_count
		FROM blobs
		WHERE bucket = ? AND hash = ?
	`, bucket, hash).Scan(&got); err != nil {
		t.Fatalf("read refcount for %q/%q: %v", bucket, hash, err)
	}
	if got != expected {
		t.Errorf("refcount for %q/%q = %d, expected %d", bucket, hash, got, expected)
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
