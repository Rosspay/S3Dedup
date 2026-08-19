package cache

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

const benchmarkDiscoveryObjects = 1000

func BenchmarkDiscoveryWrites(b *testing.B) {
	b.Run("individual-transactions", func(b *testing.B) {
		store := openBenchmarkStore(b)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchmarkDiscoveryObjects; j++ {
				if err := store.RegisterObject(ctx, benchmarkRecord(i, j)); err != nil {
					b.Fatal(err)
				}
			}
		}
		b.ReportMetric(benchmarkDiscoveryObjects, "objects/op")
	})

	b.Run("batched-transaction", func(b *testing.B) {
		store := openBenchmarkStore(b)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			mutations := make([]DiscoveryMutation, 0, benchmarkDiscoveryObjects)
			for j := 0; j < benchmarkDiscoveryObjects; j++ {
				mutations = append(mutations, DiscoveryMutation{
					Kind:   DiscoveryRegister,
					Object: benchmarkRecord(i, j),
				})
			}
			if err := store.ApplyDiscoveryBatch(ctx, mutations); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(benchmarkDiscoveryObjects, "objects/op")
	})
}

func openBenchmarkStore(b *testing.B) *SQLiteStore {
	b.Helper()
	store, err := OpenSQLite(filepath.Join(b.TempDir(), "benchmark.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Error(err)
		}
	})
	return store
}

func benchmarkRecord(iteration, index int) ObjectRecord {
	key := fmt.Sprintf("object-%d-%d", iteration, index)
	hash := fmt.Sprintf("hash-%d-%d", iteration, index)
	return ObjectRecord{
		Bucket:       "bucket",
		BlobBucket:   "bucket",
		BlobKey:      "blobs/" + hash,
		Key:          key,
		ETag:         "etag-" + key,
		Size:         100,
		BlobSize:     100,
		LastModified: time.Date(2026, time.July, 3, 0, 0, 0, 0, time.UTC),
		Hash:         hash,
		HashAlgo:     "sha256",
		LastSeenScan: "scan",
		State:        ObjectStateReported,
	}
}
