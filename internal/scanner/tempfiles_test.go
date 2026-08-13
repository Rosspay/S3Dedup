package scanner

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"s3-dedup/internal/tempfiles"

	"github.com/minio/minio-go/v6"
)

func TestCleanupTempFilesRemovesOnlyStaleInactiveScannerFiles(t *testing.T) {
	directory := t.TempDir()
	scanner := &Scanner{tempDir: directory}

	stale := createScannerTemp(t, directory)
	fresh := createScannerTemp(t, directory)
	active, err := scanner.createTempFile()
	if err != nil {
		t.Fatalf("createTempFile error: %v", err)
	}
	defer scanner.removeTempFile(active)
	unrelated := filepath.Join(directory, "other-application.tmp")
	if err := os.WriteFile(unrelated, []byte("data"), 0o600); err != nil {
		t.Fatalf("WriteFile error: %v", err)
	}

	old := time.Now().Add(-2 * time.Hour)
	for _, path := range []string{stale, active.Name(), unrelated} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatalf("Chtimes(%q) error: %v", path, err)
		}
	}

	removed, err := scanner.CleanupTempFiles(time.Hour)
	if err != nil {
		t.Fatalf("CleanupTempFiles error: %v", err)
	}
	if removed != 1 {
		t.Errorf("removed = %d, expected 1", removed)
	}
	assertPathMissing(t, stale)
	assertPathExists(t, fresh)
	assertPathExists(t, active.Name())
	assertPathExists(t, unrelated)
}

func TestPointerScanRemovesOperationTemporaryFiles(t *testing.T) {
	const content = "duplicate content"
	store := openTestStore(t)
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
	scanner := newTestScanner(t, client, store, pointerTestConfig())
	scanner.tempDir = t.TempDir()

	if _, err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatalf("ScanOnce error: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(scanner.tempDir, tempfiles.ScannerPattern))
	if err != nil {
		t.Fatalf("Glob error: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("scanner temporary files remain: %v", matches)
	}
}

func TestScannerTempFileLifecycle(t *testing.T) {
	directory := t.TempDir()
	scanner := &Scanner{tempDir: directory}
	temp, err := scanner.createTempFile()
	if err != nil {
		t.Fatalf("createTempFile error: %v", err)
	}
	if filepath.Dir(temp.Name()) != directory {
		t.Errorf("temporary directory = %q, expected %q", filepath.Dir(temp.Name()), directory)
	}
	if _, active := scanner.tempFiles.Load(temp.Name()); !active {
		t.Fatal("temporary file is not marked active")
	}

	scanner.removeTempFile(temp)
	assertPathMissing(t, temp.Name())
	if _, active := scanner.tempFiles.Load(temp.Name()); active {
		t.Fatal("temporary file is still marked active")
	}
}

func createScannerTemp(t *testing.T, directory string) string {
	t.Helper()
	file, err := tempfiles.Create(directory, tempfiles.ScannerPattern)
	if err != nil {
		t.Fatalf("Create error: %v", err)
	}
	name := file.Name()
	if err := file.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	return name
}

func assertPathExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Errorf("expected %q to exist: %v", path, err)
	}
}

func assertPathMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("expected %q to be missing: %v", path, err)
	}
}
