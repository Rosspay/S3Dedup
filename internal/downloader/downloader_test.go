package downloader

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"s3-dedup/internal/pointer"
	"s3-dedup/internal/tempfiles"

	"github.com/minio/minio-go/v6"
)

type mockS3Client struct {
	objects  map[string]minio.ObjectInfo
	contents map[string]string
	readers  map[string]io.ReadCloser
	getErrs  map[string]error
	statErrs map[string]error
}

func (m *mockS3Client) GetObject(_ context.Context, bucket, key string) (io.ReadCloser, error) {
	id := bucket + "\x00" + key
	if err := m.getErrs[id]; err != nil {
		return nil, err
	}
	if reader := m.readers[id]; reader != nil {
		return reader, nil
	}
	content, ok := m.contents[id]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(strings.NewReader(content)), nil
}

func (m *mockS3Client) StatObject(_ context.Context, bucket, key string) (minio.ObjectInfo, error) {
	id := bucket + "\x00" + key
	if err := m.statErrs[id]; err != nil {
		return minio.ObjectInfo{}, err
	}
	info, ok := m.objects[id]
	if !ok {
		return minio.ObjectInfo{}, os.ErrNotExist
	}
	return info, nil
}

func TestDownloadObject(t *testing.T) {
	const content = "ordinary object content"
	client := &mockS3Client{
		objects: map[string]minio.ObjectInfo{
			"bucket\x00path/object.txt": {Key: "path/object.txt", Size: int64(len(content))},
		},
		contents: map[string]string{
			"bucket\x00path/object.txt": content,
		},
	}
	tempDir := t.TempDir()
	destination := filepath.Join(t.TempDir(), "nested", "object.txt")
	downloader := New(client)
	downloader.tempDir = tempDir

	result, err := downloader.Download(context.Background(), "bucket", "path/object.txt", destination)
	if err != nil {
		t.Fatalf("Download error: %v", err)
	}
	if result.WasPointer {
		t.Error("WasPointer = true, expected false")
	}
	if result.SourceBucket != "bucket" || result.SourceKey != "path/object.txt" {
		t.Errorf("source = %q/%q", result.SourceBucket, result.SourceKey)
	}
	data, err := os.ReadFile(destination)
	if err != nil {
		t.Fatalf("ReadFile error: %v", err)
	}
	if string(data) != content {
		t.Errorf("downloaded content = %q, expected %q", data, content)
	}
	assertNoDownloadTemps(t, tempDir)
}

func TestDownloadPointerResolvesBlob(t *testing.T) {
	const content = "pointer target content"
	body, err := pointer.WritePointer(pointer.Pointer{
		BlobBucket: "blob-bucket",
		BlobKey:    "blobs/hash",
		HashAlgo:   "sha256",
		Hash:       "hash",
		Size:       int64(len(content)),
	})
	if err != nil {
		t.Fatalf("WritePointer error: %v", err)
	}
	client := &mockS3Client{
		objects: map[string]minio.ObjectInfo{
			"source\x00object.txt": {
				Key:         "object.txt",
				Size:        int64(len(body)),
				ContentType: pointer.ContentPointerType,
			},
			"blob-bucket\x00blobs/hash": {Key: "blobs/hash", Size: int64(len(content))},
		},
		contents: map[string]string{
			"source\x00object.txt":      string(body),
			"blob-bucket\x00blobs/hash": content,
		},
	}
	tempDir := t.TempDir()
	destination := filepath.Join(t.TempDir(), "object.txt")
	downloader := New(client)
	downloader.tempDir = tempDir

	result, err := downloader.Download(context.Background(), "source", "object.txt", destination)
	if err != nil {
		t.Fatalf("Download error: %v", err)
	}
	if !result.WasPointer {
		t.Error("WasPointer = false, expected true")
	}
	if result.SourceBucket != "blob-bucket" || result.SourceKey != "blobs/hash" {
		t.Errorf("source = %q/%q", result.SourceBucket, result.SourceKey)
	}
	data, err := os.ReadFile(destination)
	if err != nil {
		t.Fatalf("ReadFile error: %v", err)
	}
	if string(data) != content {
		t.Errorf("downloaded content = %q, expected %q", data, content)
	}
	assertNoDownloadTemps(t, tempDir)
}

type failingReadCloser struct {
	read bool
}

func (r *failingReadCloser) Read(data []byte) (int, error) {
	if r.read {
		return 0, errors.New("read failed")
	}
	r.read = true
	return copy(data, "partial"), nil
}

func (r *failingReadCloser) Close() error {
	return nil
}

func TestDownloadFailureRemovesTemporaryFile(t *testing.T) {
	const content = "short"
	client := &mockS3Client{
		objects: map[string]minio.ObjectInfo{
			"bucket\x00object.txt": {Key: "object.txt", Size: int64(len(content) + 1)},
		},
		contents: map[string]string{
			"bucket\x00object.txt": content,
		},
	}
	tempDir := t.TempDir()
	destination := filepath.Join(t.TempDir(), "object.txt")
	downloader := New(client)
	downloader.tempDir = tempDir

	if _, err := downloader.Download(context.Background(), "bucket", "object.txt", destination); err == nil {
		t.Fatal("Download error = nil, expected size mismatch")
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination exists after failed download: %v", err)
	}
	assertNoDownloadTemps(t, tempDir)
}

func TestPartialReadRemovesTemporaryFile(t *testing.T) {
	client := &mockS3Client{
		objects: map[string]minio.ObjectInfo{
			"bucket\x00object.txt": {Key: "object.txt", Size: 20},
		},
		readers: map[string]io.ReadCloser{
			"bucket\x00object.txt": &failingReadCloser{},
		},
	}
	tempDir := t.TempDir()
	destination := filepath.Join(t.TempDir(), "object.txt")
	downloader := New(client)
	downloader.tempDir = tempDir

	if _, err := downloader.Download(context.Background(), "bucket", "object.txt", destination); err == nil {
		t.Fatal("Download error = nil, expected read error")
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination exists after partial read: %v", err)
	}
	assertNoDownloadTemps(t, tempDir)
}

func TestDownloadDoesNotOverwriteDestination(t *testing.T) {
	const content = "new content"
	client := &mockS3Client{
		objects: map[string]minio.ObjectInfo{
			"bucket\x00object.txt": {Key: "object.txt", Size: int64(len(content))},
		},
		contents: map[string]string{
			"bucket\x00object.txt": content,
		},
	}
	tempDir := t.TempDir()
	destination := filepath.Join(t.TempDir(), "object.txt")
	if err := os.WriteFile(destination, []byte("existing"), 0o644); err != nil {
		t.Fatalf("WriteFile error: %v", err)
	}
	downloader := New(client)
	downloader.tempDir = tempDir

	if _, err := downloader.Download(context.Background(), "bucket", "object.txt", destination); err == nil {
		t.Fatal("Download error = nil, expected existing destination error")
	}
	data, err := os.ReadFile(destination)
	if err != nil {
		t.Fatalf("ReadFile error: %v", err)
	}
	if string(data) != "existing" {
		t.Errorf("destination content = %q, expected existing content", data)
	}
	assertNoDownloadTemps(t, tempDir)
}

func TestDownloadRejectsInvalidPointer(t *testing.T) {
	client := &mockS3Client{
		objects: map[string]minio.ObjectInfo{
			"bucket\x00pointer": {Key: "pointer", Size: 2, ContentType: pointer.ContentPointerType},
		},
		contents: map[string]string{
			"bucket\x00pointer": "{}",
		},
	}
	destination := filepath.Join(t.TempDir(), "object")

	if _, err := New(client).Download(context.Background(), "bucket", "pointer", destination); err == nil {
		t.Fatal("Download error = nil, expected invalid pointer error")
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination exists for invalid pointer: %v", err)
	}
}

func TestDownloadRejectsMissingPointerBlob(t *testing.T) {
	body, err := pointer.WritePointer(pointer.Pointer{
		BlobBucket: "blob-bucket",
		BlobKey:    "missing",
		HashAlgo:   "sha256",
		Hash:       "hash",
		Size:       10,
	})
	if err != nil {
		t.Fatalf("WritePointer error: %v", err)
	}
	client := &mockS3Client{
		objects: map[string]minio.ObjectInfo{
			"bucket\x00pointer": {Key: "pointer", Size: int64(len(body)), ContentType: pointer.ContentPointerType},
		},
		contents: map[string]string{
			"bucket\x00pointer": string(body),
		},
	}
	destination := filepath.Join(t.TempDir(), "object")

	if _, err := New(client).Download(context.Background(), "bucket", "pointer", destination); err == nil {
		t.Fatal("Download error = nil, expected missing blob error")
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination exists for missing blob: %v", err)
	}
}

func assertNoDownloadTemps(t *testing.T, directory string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(directory, tempfiles.DownloadPattern))
	if err != nil {
		t.Fatalf("Glob error: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("download temporary files remain: %v", matches)
	}
}
