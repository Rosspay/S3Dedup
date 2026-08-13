package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"s3-dedup/internal/pointer"
	"s3-dedup/internal/tempfiles"

	"github.com/minio/minio-go/v6"
)

type S3Client interface {
	GetObject(ctx context.Context, bucket string, key string) (io.ReadCloser, error)
	StatObject(ctx context.Context, bucket string, key string) (minio.ObjectInfo, error)
}

type Client struct {
	s3      S3Client
	tempDir string
}

type Result struct {
	Bucket       string
	Key          string
	SourceBucket string
	SourceKey    string
	Destination  string
	Size         int64
	WasPointer   bool
}

func New(client S3Client) *Client {
	return &Client{s3: client, tempDir: os.TempDir()}
}

func (c *Client) Download(ctx context.Context, bucket, key, destination string) (Result, error) {
	if c == nil || c.s3 == nil {
		return Result{}, errors.New("download object: S3 client is nil")
	}
	if bucket == "" {
		return Result{}, errors.New("download object: bucket is empty")
	}
	if key == "" {
		return Result{}, errors.New("download object: key is empty")
	}
	if destination == "" {
		return Result{}, errors.New("download object: destination is empty")
	}

	destination, err := filepath.Abs(destination)
	if err != nil {
		return Result{}, fmt.Errorf("resolve destination %q: %w", destination, err)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return Result{}, fmt.Errorf("create destination directory: %w", err)
	}

	sourceBucket, sourceKey, expectedSize, wasPointer, err := c.resolveSource(ctx, bucket, key)
	if err != nil {
		return Result{}, err
	}

	object, err := c.s3.GetObject(ctx, sourceBucket, sourceKey)
	if err != nil {
		return Result{}, fmt.Errorf("get object %q/%q: %w", sourceBucket, sourceKey, err)
	}
	defer object.Close()

	temp, err := tempfiles.Create(c.tempDir, tempfiles.DownloadPattern)
	if err != nil {
		return Result{}, err
	}
	tempName := temp.Name()
	committed := false
	defer func() {
		temp.Close()
		if !committed {
			os.Remove(tempName)
		}
	}()

	written, err := io.Copy(temp, object)
	if err != nil {
		return Result{}, fmt.Errorf("download object %q/%q: %w", sourceBucket, sourceKey, err)
	}
	if written != expectedSize {
		return Result{}, fmt.Errorf("download object %q/%q: wrote %d bytes, expected %d", sourceBucket, sourceKey, written, expectedSize)
	}
	if err := object.Close(); err != nil {
		return Result{}, fmt.Errorf("close object %q/%q: %w", sourceBucket, sourceKey, err)
	}
	if err := tempfiles.Commit(temp, destination); err != nil {
		return Result{}, err
	}
	committed = true

	return Result{
		Bucket:       bucket,
		Key:          key,
		SourceBucket: sourceBucket,
		SourceKey:    sourceKey,
		Destination:  destination,
		Size:         written,
		WasPointer:   wasPointer,
	}, nil
}

func (c *Client) resolveSource(ctx context.Context, bucket, key string) (string, string, int64, bool, error) {
	info, err := c.s3.StatObject(ctx, bucket, key)
	if err != nil {
		return "", "", 0, false, fmt.Errorf("stat object %q/%q: %w", bucket, key, err)
	}
	if info.ContentType != pointer.ContentPointerType {
		return bucket, key, info.Size, false, nil
	}

	object, err := c.s3.GetObject(ctx, bucket, key)
	if err != nil {
		return "", "", 0, false, fmt.Errorf("get pointer %q/%q: %w", bucket, key, err)
	}
	p, readErr := pointer.ReadPointer(object)
	closeErr := object.Close()
	if readErr != nil {
		return "", "", 0, false, fmt.Errorf("read pointer %q/%q: %w", bucket, key, readErr)
	}
	if closeErr != nil {
		return "", "", 0, false, fmt.Errorf("close pointer %q/%q: %w", bucket, key, closeErr)
	}

	blobInfo, err := c.s3.StatObject(ctx, p.BlobBucket, p.BlobKey)
	if err != nil {
		return "", "", 0, false, fmt.Errorf("stat pointer blob %q/%q: %w", p.BlobBucket, p.BlobKey, err)
	}
	if blobInfo.Size != p.Size {
		return "", "", 0, false, fmt.Errorf("pointer blob %q/%q size is %d, expected %d", p.BlobBucket, p.BlobKey, blobInfo.Size, p.Size)
	}
	return p.BlobBucket, p.BlobKey, p.Size, true, nil
}
