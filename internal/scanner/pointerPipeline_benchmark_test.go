package scanner

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"s3-dedup/internal/cache"
	"s3-dedup/internal/logger"

	"github.com/minio/minio-go/v6"
)

const benchmarkPointerObjects = 5000

func BenchmarkPointerUniqueObjects(b *testing.B) {
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		objects := make([]minio.ObjectInfo, 0, benchmarkPointerObjects)
		contents := make(map[string]string, benchmarkPointerObjects)
		for i := 0; i < benchmarkPointerObjects; i++ {
			key := fmt.Sprintf("unique-%08d.txt", i)
			content := fmt.Sprintf("unique-content-%08d", i)
			objects = append(objects, objectInfo(key, int64(len(content))))
			contents[objectID("bucket", key)] = content
		}
		client := &mockS3Client{objects: objects, contents: contents}
		store, err := cache.OpenSQLite(filepath.Join(b.TempDir(), fmt.Sprintf("benchmark-%d.db", iteration)))
		if err != nil {
			b.Fatal(err)
		}
		logging, err := logger.New("error", "-")
		if err != nil {
			b.Fatal(err)
		}
		cfg := pointerTestConfig()
		cfg.Schedule.Workers = 16
		scanner := NewScanner(client, store, cfg, logging)
		b.StartTimer()

		result, err := scanner.ScanOnce(context.Background())
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if result.Errors != 0 || result.UniqueBlobs != benchmarkPointerObjects {
			b.Fatalf("unexpected result: %+v", result)
		}
		if err := store.Close(); err != nil {
			b.Fatal(err)
		}
		if err := logging.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(benchmarkPointerObjects, "objects/scan")
}
