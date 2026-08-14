package scanner

import (
	"context"
	"testing"

	"github.com/minio/minio-go/v6"
)

func TestReportOnlyDoesNotUseStore(t *testing.T) {
	const content = "same content"
	client := &mockS3Client{
		objects: []minio.ObjectInfo{
			objectInfo("one.txt", int64(len(content))),
			objectInfo("two.txt", int64(len(content))),
		},
		contents: map[string]string{
			objectID("bucket", "one.txt"): content,
			objectID("bucket", "two.txt"): content,
		},
	}

	scanner := newTestScanner(t, client, nil, testConfig())
	result, err := scanner.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	if result.UniqueBlobs != 1 || result.DuplicatesFound != 1 {
		t.Fatalf("report stats = %+v, expected one duplicate group", result)
	}
	if result.BytesReclaimable != int64(len(content)) {
		t.Errorf("BytesReclaimable = %d, expected %d", result.BytesReclaimable, len(content))
	}
	if client.totalPutCalls() != 0 {
		t.Errorf("PutObject calls = %d, expected 0", client.totalPutCalls())
	}
}
