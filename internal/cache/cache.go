package cache

import (
	"context"
	"time"
)

type Store interface {
	RegisterObject(ctx context.Context, object ObjectRecord) error
	GetStats(ctx context.Context) (Stats, error)
	Close() error
}

type ObjectRecord struct {
	Bucket       string
	Key          string
	ETag         string
	Size         int64
	LastModified time.Time
	Hash         string
	LastSeenScan string
}

type Stats struct {
	UniqueBlobs      int64
	DuplicatesFound  int64
	BytesReclaimable int64
}
