package cache

import (
	"context"
	"time"
)

type Store interface {
	RegisterObject(ctx context.Context, object ObjectRecord) error
	UnregisterObject(ctx context.Context, bucket string, key string) error
	IsObjectUnchanged(ctx context.Context, bucket, key, etag string, size int64, lastModified time.Time) (bool, error)
	GetStats(ctx context.Context) (Stats, error)
	MarkObjectSeen(ctx context.Context, bucket, key, scanID string) error
	FinalizeScope(ctx context.Context, bucket, prefix, scanID string) (removed int64, err error)
	Close() error
}

type ObjectRecord struct {
	Bucket       string
	BlobBucket   string
	BlobKey      string
	Key          string
	ETag         string
	Size         int64
	BlobSize     int64
	LastModified time.Time
	Hash         string
	LastSeenScan string
}

type Stats struct {
	UniqueBlobs      int64
	DuplicatesFound  int64
	BytesReclaimable int64
}
