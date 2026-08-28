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

func BenchmarkDiscoveryBatchMutations(b *testing.B) {
	b.Run("mark-seen", func(b *testing.B) {
		store := openBenchmarkStore(b)
		ctx := context.Background()
		registerBenchmarkObjects(b, store, ctx)
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			mutations := make([]DiscoveryMutation, 0, benchmarkDiscoveryObjects)
			for index := 0; index < benchmarkDiscoveryObjects; index++ {
				mutations = append(mutations, DiscoveryMutation{
					Kind: DiscoveryMarkSeen,
					ID: ObjectID{
						Bucket: "bucket",
						Key:    benchmarkRecord(0, index).Key,
						ScanID: fmt.Sprintf("scan-%d", iteration),
					},
				})
			}
			if err := store.ApplyDiscoveryBatch(ctx, mutations); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(benchmarkDiscoveryObjects, "objects/op")
	})

	b.Run("metadata-update", func(b *testing.B) {
		store := openBenchmarkStore(b)
		ctx := context.Background()
		registerBenchmarkObjects(b, store, ctx)
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			mutations := make([]DiscoveryMutation, 0, benchmarkDiscoveryObjects)
			for index := 0; index < benchmarkDiscoveryObjects; index++ {
				object := benchmarkRecord(0, index)
				object.ETag = fmt.Sprintf("etag-%d-%d", iteration, index)
				object.LastSeenScan = fmt.Sprintf("scan-%d", iteration)
				mutations = append(mutations, DiscoveryMutation{
					Kind:   DiscoveryRegister,
					Object: object,
				})
			}
			if err := store.ApplyDiscoveryBatch(ctx, mutations); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(benchmarkDiscoveryObjects, "objects/op")
	})

	b.Run("blob-change", func(b *testing.B) {
		store := openBenchmarkStore(b)
		ctx := context.Background()
		registerBenchmarkObjects(b, store, ctx)
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			generation := iteration%2 + 1
			mutations := make([]DiscoveryMutation, 0, benchmarkDiscoveryObjects)
			for index := 0; index < benchmarkDiscoveryObjects; index++ {
				object := benchmarkRecord(0, index)
				object.Hash = fmt.Sprintf("hash-%d-%d", generation, index)
				object.BlobKey = "blobs/" + object.Hash
				object.ETag = fmt.Sprintf("etag-%d-%d", generation, index)
				object.LastSeenScan = fmt.Sprintf("scan-%d", iteration)
				mutations = append(mutations, DiscoveryMutation{
					Kind:   DiscoveryRegister,
					Object: object,
				})
			}
			if err := store.ApplyDiscoveryBatch(ctx, mutations); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(benchmarkDiscoveryObjects, "objects/op")
	})
}

func BenchmarkDiscoveryBatchMutationsLargeCache(b *testing.B) {
	store := openBenchmarkStore(b)
	ctx := context.Background()
	const largeCacheObjects = 100000
	populateBenchmarkStore(b, store, largeCacheObjects)

	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		mutations := make([]DiscoveryMutation, 0, benchmarkDiscoveryObjects)
		for index := 0; index < benchmarkDiscoveryObjects; index++ {
			mutations = append(mutations, DiscoveryMutation{
				Kind: DiscoveryMarkSeen,
				ID: ObjectID{
					Bucket: "bucket",
					Key:    benchmarkRecord(0, index).Key,
					ScanID: fmt.Sprintf("scan-%d", iteration),
				},
			})
		}
		if err := store.ApplyDiscoveryBatch(ctx, mutations); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(benchmarkDiscoveryObjects, "objects/op")
	b.ReportMetric(largeCacheObjects, "cache-objects")
}

func BenchmarkDiscoveryMetadataUpdateLargeCache(b *testing.B) {
	store := openBenchmarkStore(b)
	ctx := context.Background()
	const largeCacheObjects = 100000
	populateBenchmarkStore(b, store, largeCacheObjects)

	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		mutations := make([]DiscoveryMutation, 0, benchmarkDiscoveryObjects)
		for index := 0; index < benchmarkDiscoveryObjects; index++ {
			object := benchmarkRecord(0, index)
			object.ETag = fmt.Sprintf("etag-%d-%d", iteration+1, index)
			object.LastSeenScan = fmt.Sprintf("scan-%d", iteration+1)
			mutations = append(mutations, DiscoveryMutation{
				Kind:   DiscoveryRegister,
				Object: object,
			})
		}
		if err := store.ApplyDiscoveryBatch(ctx, mutations); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(benchmarkDiscoveryObjects, "objects/op")
	b.ReportMetric(largeCacheObjects, "cache-objects")
}

func BenchmarkDiscoveryRegistrationLargeCache(b *testing.B) {
	store := openBenchmarkStore(b)
	ctx := context.Background()
	const largeCacheObjects = 100000
	populateBenchmarkStore(b, store, largeCacheObjects)

	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		mutations := make([]DiscoveryMutation, 0, benchmarkDiscoveryObjects)
		start := largeCacheObjects + iteration*benchmarkDiscoveryObjects
		for index := start; index < start+benchmarkDiscoveryObjects; index++ {
			mutations = append(mutations, DiscoveryMutation{
				Kind:   DiscoveryRegister,
				Object: benchmarkRecord(0, index),
			})
		}
		if err := store.ApplyDiscoveryBatch(ctx, mutations); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(benchmarkDiscoveryObjects, "objects/op")
	b.ReportMetric(largeCacheObjects, "initial-cache-objects")
}

func BenchmarkDiscoveryLookups(b *testing.B) {
	store := openBenchmarkStore(b)
	ctx := context.Background()
	mutations := make([]DiscoveryMutation, 0, benchmarkDiscoveryObjects)
	metadata := make([]ObjectMetadata, 0, benchmarkDiscoveryObjects)
	for index := 0; index < benchmarkDiscoveryObjects; index++ {
		object := benchmarkRecord(0, index)
		mutations = append(mutations, DiscoveryMutation{
			Kind:   DiscoveryRegister,
			Object: object,
		})
		metadata = append(metadata, ObjectMetadata{
			Key:          object.Key,
			ETag:         object.ETag,
			Size:         object.Size,
			LastModified: object.LastModified,
		})
	}
	if err := store.ApplyDiscoveryBatch(ctx, mutations); err != nil {
		b.Fatal(err)
	}

	b.Run("individual-queries", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, object := range metadata {
				if _, err := store.GetObjectStatus(
					ctx,
					"bucket",
					object.Key,
					object.ETag,
					object.Size,
					"sha256",
					object.LastModified,
				); err != nil {
					b.Fatal(err)
				}
			}
		}
		b.ReportMetric(benchmarkDiscoveryObjects, "objects/op")
	})

	b.Run("bulk-query", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := store.GetObjectStatuses(
				ctx,
				"bucket",
				metadata,
				"sha256",
			); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(benchmarkDiscoveryObjects, "objects/op")
	})
}

func BenchmarkEmptyDiscoveryLookups(b *testing.B) {
	store := openBenchmarkStore(b)
	ctx := context.Background()
	metadata := make([]ObjectMetadata, 0, benchmarkDiscoveryObjects)
	for index := 0; index < benchmarkDiscoveryObjects; index++ {
		object := benchmarkRecord(0, index)
		metadata = append(metadata, ObjectMetadata{
			Key:          object.Key,
			ETag:         object.ETag,
			Size:         object.Size,
			LastModified: object.LastModified,
		})
	}

	b.Run("individual-queries", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, object := range metadata {
				if _, err := store.GetObjectStatus(
					ctx,
					"bucket",
					object.Key,
					object.ETag,
					object.Size,
					"sha256",
					object.LastModified,
				); err != nil {
					b.Fatal(err)
				}
			}
		}
		b.ReportMetric(benchmarkDiscoveryObjects, "objects/op")
	})

	b.Run("bulk-query", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := store.GetObjectStatuses(
				ctx,
				"bucket",
				metadata,
				"sha256",
			); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(benchmarkDiscoveryObjects, "objects/op")
	})
}

func registerBenchmarkObjects(
	b *testing.B,
	store *SQLiteStore,
	ctx context.Context,
) {
	b.Helper()
	mutations := make([]DiscoveryMutation, 0, benchmarkDiscoveryObjects)
	for index := 0; index < benchmarkDiscoveryObjects; index++ {
		mutations = append(mutations, DiscoveryMutation{
			Kind:   DiscoveryRegister,
			Object: benchmarkRecord(0, index),
		})
	}
	if err := store.ApplyDiscoveryBatch(ctx, mutations); err != nil {
		b.Fatal(err)
	}
}

func populateBenchmarkStore(b *testing.B, store *SQLiteStore, objectCount int) {
	b.Helper()
	ctx := context.Background()
	for start := 0; start < objectCount; start += benchmarkDiscoveryObjects {
		mutations := make([]DiscoveryMutation, 0, benchmarkDiscoveryObjects)
		limit := start + benchmarkDiscoveryObjects
		if limit > objectCount {
			limit = objectCount
		}
		for index := start; index < limit; index++ {
			mutations = append(mutations, DiscoveryMutation{
				Kind:   DiscoveryRegister,
				Object: benchmarkRecord(0, index),
			})
		}
		if err := store.ApplyDiscoveryBatch(ctx, mutations); err != nil {
			b.Fatal(err)
		}
	}
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
