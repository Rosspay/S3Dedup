package cache

import (
	"context"
	"testing"
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
