package cache

import (
	"context"
	"testing"
	"time"
)

func TestApplyDiscoveryBatchRegistersDuplicateGroup(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	first := record("bucket", "one.txt", "same-hash", 100)
	second := record("bucket", "two.txt", "same-hash", 100)
	if err := store.ApplyDiscoveryBatch(ctx, []DiscoveryMutation{
		{Kind: DiscoveryRegister, Object: first},
		{Kind: DiscoveryRegister, Object: second},
	}); err != nil {
		t.Fatalf("ApplyDiscoveryBatch error: %v", err)
	}

	assertRefCount(t, store, "bucket", "same-hash", 2)
	assertStats(t, store, Stats{
		UniqueBlobs:      1,
		DuplicatesFound:  1,
		BytesReclaimable: 100,
	})
}

func TestApplyDiscoveryBatchIsAtomicOnValidationError(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	valid := record("bucket", "one.txt", "hash", 100)
	invalid := record("bucket", "two.txt", "hash", 100)
	invalid.Hash = ""
	if err := store.ApplyDiscoveryBatch(ctx, []DiscoveryMutation{
		{Kind: DiscoveryRegister, Object: valid},
		{Kind: DiscoveryRegister, Object: invalid},
	}); err == nil {
		t.Fatal("ApplyDiscoveryBatch error = nil, expected validation error")
	}

	assertStats(t, store, Stats{})
}

func TestListDedupCandidatesFiltersStateAndPaginates(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	first := record("bucket", "prefix/one.txt", "same-hash", 100)
	second := record("bucket", "prefix/two.txt", "same-hash", 100)
	outside := record("bucket", "outside.txt", "same-hash", 100)
	if err := store.ApplyDiscoveryBatch(ctx, []DiscoveryMutation{
		{Kind: DiscoveryRegister, Object: first},
		{Kind: DiscoveryRegister, Object: second},
		{Kind: DiscoveryRegister, Object: outside},
	}); err != nil {
		t.Fatalf("ApplyDiscoveryBatch error: %v", err)
	}

	page, err := store.ListDedupCandidates(ctx, "bucket", "prefix/", ObjectStatePointer, "", 1)
	if err != nil {
		t.Fatalf("ListDedupCandidates first page error: %v", err)
	}
	if len(page) != 1 || page[0].Key != first.Key {
		t.Fatalf("first page = %+v, expected %q", page, first.Key)
	}
	if candidate := page[0]; candidate.BlobBucket != first.BlobBucket ||
		candidate.BlobKey != first.BlobKey ||
		candidate.Hash != first.Hash ||
		candidate.HashAlgo != first.HashAlgo ||
		candidate.BlobSize != first.BlobSize {
		t.Fatalf("first candidate blob metadata = %+v, expected %+v", candidate, first)
	}
	page, err = store.ListDedupCandidates(ctx, "bucket", "prefix/", ObjectStatePointer, page[0].Key, 10)
	if err != nil {
		t.Fatalf("ListDedupCandidates second page error: %v", err)
	}
	if len(page) != 1 || page[0].Key != second.Key {
		t.Fatalf("second page = %+v, expected %q", page, second.Key)
	}

	first.State = ObjectStatePointer
	first.ETag = "pointer-etag"
	first.Size = 50
	if err := store.ApplyDiscoveryBatch(ctx, []DiscoveryMutation{{Kind: DiscoveryRegister, Object: first}}); err != nil {
		t.Fatalf("updating candidate state: %v", err)
	}
	candidates, err := store.ListDedupCandidates(ctx, "bucket", "prefix/", ObjectStatePointer, "", 10)
	if err != nil {
		t.Fatalf("ListDedupCandidates filtered error: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Key != second.Key {
		t.Fatalf("filtered candidates = %+v, expected only %q", candidates, second.Key)
	}
}

func TestApplyDiscoveryBatchMarksAndUnregisters(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	first := record("bucket", "one.txt", "same-hash", 100)
	second := record("bucket", "two.txt", "same-hash", 100)
	if err := store.ApplyDiscoveryBatch(ctx, []DiscoveryMutation{
		{Kind: DiscoveryRegister, Object: first},
		{Kind: DiscoveryRegister, Object: second},
	}); err != nil {
		t.Fatalf("register batch: %v", err)
	}

	if err := store.ApplyDiscoveryBatch(ctx, []DiscoveryMutation{
		{Kind: DiscoveryMarkSeen, ID: ObjectID{Bucket: first.Bucket, Key: first.Key, ScanID: "scan-2"}},
		{Kind: DiscoveryUnregister, ID: ObjectID{Bucket: second.Bucket, Key: second.Key}},
	}); err != nil {
		t.Fatalf("update batch: %v", err)
	}

	assertRefCount(t, store, "bucket", "same-hash", 1)
	removed, err := store.FinalizeScope(ctx, "bucket", "", "scan-2")
	if err != nil {
		t.Fatalf("FinalizeScope error: %v", err)
	}
	if removed != 0 {
		t.Fatalf("FinalizeScope removed = %d, expected 0", removed)
	}
}

func TestApplyDiscoveryBatchUpdatesMetadataWithoutChangingReference(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	object := record("bucket", "one.txt", "hash", 100)
	register(t, store, object)

	object.ETag = "updated-etag"
	object.Size = 64
	object.LastModified = object.LastModified.Add(123456789 * time.Nanosecond)
	object.HashAlgo = "sha512"
	object.LastSeenScan = "scan-2"
	object.State = ObjectStatePointer
	object.BlobKey = "ignored-for-existing-reference"
	object.BlobSize = 999
	if err := store.ApplyDiscoveryBatch(ctx, []DiscoveryMutation{{
		Kind:   DiscoveryRegister,
		Object: object,
	}}); err != nil {
		t.Fatalf("ApplyDiscoveryBatch error: %v", err)
	}

	assertRefCount(t, store, "bucket", "hash", 1)
	var etag string
	var size int64
	var lastModified string
	var hashAlgo string
	var scanID string
	var state ObjectState
	if err := store.db.QueryRow(`
		SELECT etag, size, last_modified, hash_algo, last_seen_scan, object_state
		FROM objects
		WHERE bucket = ? AND object_key = ?
	`, object.Bucket, object.Key).Scan(
		&etag,
		&size,
		&lastModified,
		&hashAlgo,
		&scanID,
		&state,
	); err != nil {
		t.Fatalf("read updated object: %v", err)
	}
	if etag != object.ETag ||
		size != object.Size ||
		lastModified != formatObjectTime(object.LastModified) ||
		hashAlgo != object.HashAlgo ||
		scanID != object.LastSeenScan ||
		state != object.State {
		t.Fatalf(
			"stored metadata = %q/%d/%q/%q/%q/%q, expected %+v",
			etag,
			size,
			lastModified,
			hashAlgo,
			scanID,
			state,
			object,
		)
	}
}

func TestApplyDiscoveryBatchMovesReferencesBetweenBlobs(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	first := record("bucket", "one.txt", "old-hash", 100)
	second := record("bucket", "two.txt", "old-hash", 100)
	target := record("bucket", "target.txt", "new-hash", 200)
	if err := store.ApplyDiscoveryBatch(ctx, []DiscoveryMutation{
		{Kind: DiscoveryRegister, Object: first},
		{Kind: DiscoveryRegister, Object: second},
		{Kind: DiscoveryRegister, Object: target},
	}); err != nil {
		t.Fatalf("register initial batch: %v", err)
	}

	first.Hash = target.Hash
	first.BlobKey = target.BlobKey
	first.Size = target.Size
	first.BlobSize = target.BlobSize
	first.ETag = "changed-etag"
	first.LastSeenScan = "scan-2"
	if err := store.ApplyDiscoveryBatch(ctx, []DiscoveryMutation{{
		Kind:   DiscoveryRegister,
		Object: first,
	}}); err != nil {
		t.Fatalf("change blob batch: %v", err)
	}

	assertRefCount(t, store, "bucket", "old-hash", 1)
	assertRefCount(t, store, "bucket", "new-hash", 2)
}

func TestApplyDiscoveryBatchAppliesMixedUniqueMutationsAtomically(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	seen := record("bucket", "seen.txt", "shared-hash", 100)
	removedOne := record("bucket", "removed-one.txt", "shared-hash", 100)
	removedTwo := record("bucket", "removed-two.txt", "shared-hash", 100)
	changed := record("bucket", "changed.txt", "old-hash", 50)
	if err := store.ApplyDiscoveryBatch(ctx, []DiscoveryMutation{
		{Kind: DiscoveryRegister, Object: seen},
		{Kind: DiscoveryRegister, Object: removedOne},
		{Kind: DiscoveryRegister, Object: removedTwo},
		{Kind: DiscoveryRegister, Object: changed},
	}); err != nil {
		t.Fatalf("register initial batch: %v", err)
	}

	added := record("bucket", "added.txt", "new-hash", 75)
	added.LastSeenScan = "scan-2"
	changed.Hash = added.Hash
	changed.BlobKey = added.BlobKey
	changed.Size = added.Size
	changed.BlobSize = added.BlobSize
	changed.LastSeenScan = "scan-2"
	if err := store.ApplyDiscoveryBatch(ctx, []DiscoveryMutation{
		{Kind: DiscoveryMarkSeen, ID: ObjectID{Bucket: seen.Bucket, Key: seen.Key, ScanID: "scan-2"}},
		{Kind: DiscoveryMarkSeen, ID: ObjectID{Bucket: "bucket", Key: "missing.txt", ScanID: "scan-2"}},
		{Kind: DiscoveryUnregister, ID: ObjectID{Bucket: removedOne.Bucket, Key: removedOne.Key}},
		{Kind: DiscoveryUnregister, ID: ObjectID{Bucket: removedTwo.Bucket, Key: removedTwo.Key}},
		{Kind: DiscoveryUnregister, ID: ObjectID{Bucket: "bucket", Key: "also-missing.txt"}},
		{Kind: DiscoveryRegister, Object: added},
		{Kind: DiscoveryRegister, Object: changed},
	}); err != nil {
		t.Fatalf("mixed batch: %v", err)
	}

	assertRefCount(t, store, "bucket", "shared-hash", 1)
	assertRefCount(t, store, "bucket", "old-hash", 0)
	assertRefCount(t, store, "bucket", "new-hash", 2)
	removed, err := store.FinalizeScope(ctx, "bucket", "", "scan-2")
	if err != nil {
		t.Fatalf("FinalizeScope error: %v", err)
	}
	if removed != 0 {
		t.Fatalf("FinalizeScope removed = %d, expected 0", removed)
	}
}

func TestApplyDiscoveryBatchDuplicateObjectFallsBackToSequentialOrder(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	object := record("bucket", "one.txt", "first-hash", 100)
	updated := record("bucket", "one.txt", "second-hash", 200)
	updated.LastSeenScan = "scan-2"
	if err := store.ApplyDiscoveryBatch(ctx, []DiscoveryMutation{
		{Kind: DiscoveryRegister, Object: object},
		{Kind: DiscoveryRegister, Object: updated},
		{Kind: DiscoveryMarkSeen, ID: ObjectID{Bucket: updated.Bucket, Key: updated.Key, ScanID: "scan-3"}},
	}); err != nil {
		t.Fatalf("ApplyDiscoveryBatch error: %v", err)
	}

	assertRefCount(t, store, "bucket", "first-hash", 0)
	assertRefCount(t, store, "bucket", "second-hash", 1)
	var hash string
	var scanID string
	if err := store.db.QueryRow(`
		SELECT blob_hash, last_seen_scan
		FROM objects
		WHERE bucket = ? AND object_key = ?
	`, updated.Bucket, updated.Key).Scan(&hash, &scanID); err != nil {
		t.Fatalf("read updated object: %v", err)
	}
	if hash != updated.Hash || scanID != "scan-3" {
		t.Fatalf("stored hash/scan = %q/%q, expected %q/scan-3", hash, scanID, updated.Hash)
	}
}

func TestApplyDiscoveryBatchRejectsIncompatibleBlobMetadata(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ObjectRecord)
	}{
		{
			name: "blob key",
			mutate: func(object *ObjectRecord) {
				object.BlobKey = "other/blob-key"
			},
		},
		{
			name: "blob size",
			mutate: func(object *ObjectRecord) {
				object.BlobSize++
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := openTestStore(t)
			ctx := context.Background()
			existing := record("bucket", "existing.txt", "hash", 100)
			register(t, store, existing)
			incompatible := record("bucket", "incompatible.txt", "hash", 100)
			test.mutate(&incompatible)
			valid := record("bucket", "valid.txt", "valid-hash", 50)

			if err := store.ApplyDiscoveryBatch(ctx, []DiscoveryMutation{
				{Kind: DiscoveryRegister, Object: valid},
				{Kind: DiscoveryRegister, Object: incompatible},
			}); err == nil {
				t.Fatal("ApplyDiscoveryBatch error = nil, expected metadata mismatch")
			}

			assertRefCount(t, store, existing.BlobBucket, existing.Hash, 1)
			var count int
			if err := store.db.QueryRow(`
				SELECT COUNT(*)
				FROM objects
				WHERE object_key IN (?, ?)
			`, valid.Key, incompatible.Key).Scan(&count); err != nil {
				t.Fatalf("count rolled-back objects: %v", err)
			}
			if count != 0 {
				t.Fatalf("rolled-back object count = %d, expected 0", count)
			}
		})
	}
}

func TestApplyDiscoveryBatchRejectsConflictingIncomingBlobMetadata(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	first := record("bucket", "one.txt", "same-hash", 100)
	second := record("bucket", "two.txt", "same-hash", 100)
	second.BlobKey = "different-key"

	if err := store.ApplyDiscoveryBatch(ctx, []DiscoveryMutation{
		{Kind: DiscoveryRegister, Object: first},
		{Kind: DiscoveryRegister, Object: second},
	}); err == nil {
		t.Fatal("ApplyDiscoveryBatch error = nil, expected conflicting metadata error")
	}
	assertStats(t, store, Stats{})
}

func TestApplyDiscoveryBatchRollsBackOnSQLError(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	if _, err := store.db.Exec(`
		CREATE TRIGGER reject_discovery_object
		BEFORE INSERT ON objects
		WHEN NEW.object_key = 'reject.txt'
		BEGIN
			SELECT RAISE(ABORT, 'forced discovery failure');
		END
	`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	valid := record("bucket", "valid.txt", "valid-hash", 100)
	rejected := record("bucket", "reject.txt", "rejected-hash", 200)
	if err := store.ApplyDiscoveryBatch(ctx, []DiscoveryMutation{
		{Kind: DiscoveryRegister, Object: valid},
		{Kind: DiscoveryRegister, Object: rejected},
	}); err == nil {
		t.Fatal("ApplyDiscoveryBatch error = nil, expected SQL error")
	}
	assertStats(t, store, Stats{})
	var blobCount int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM blobs`).Scan(&blobCount); err != nil {
		t.Fatalf("count rolled-back blobs: %v", err)
	}
	if blobCount != 0 {
		t.Fatalf("rolled-back blob count = %d, expected 0", blobCount)
	}
}

func TestApplyDiscoveryBatchTreatsObjectKeysAsData(t *testing.T) {
	store := openTestStore(t)
	object := record("bucket", "x'); DROP TABLE objects; --", "hash", 100)
	if err := store.ApplyDiscoveryBatch(context.Background(), []DiscoveryMutation{{
		Kind:   DiscoveryRegister,
		Object: object,
	}}); err != nil {
		t.Fatalf("ApplyDiscoveryBatch error: %v", err)
	}

	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM objects`).Scan(&count); err != nil {
		t.Fatalf("count objects: %v", err)
	}
	if count != 1 {
		t.Fatalf("object count = %d, expected 1", count)
	}
}
