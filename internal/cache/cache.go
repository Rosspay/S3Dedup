package cache

import (
	"context"
	"time"
)

type Store interface {
	RegisterObject(ctx context.Context, object ObjectRecord) error
	UnregisterObject(ctx context.Context, bucket string, key string) error
	//IsObjectUnchanged(ctx context.Context, bucket, key, etag string, size int64, lastModified time.Time) (bool, bool, error)
	GetObjectStatus(
		ctx context.Context,
		bucket string,
		key string,
		etag string,
		size int64,
		hashAlgo string,
		lastModified time.Time,
	) (status ObjectStatus, err error)
	GetStats(ctx context.Context) (Stats, error)
	ListObjectsByBlob(ctx context.Context, blobBucket, blobHash string) ([]ObjectRecord, error)
	MarkObjectSeen(ctx context.Context, bucket, key, scanID string) error
	FinalizeScope(ctx context.Context, bucket, prefix, scanID string) (removed int64, err error)
	ListUnreferencedBlobs(ctx context.Context, bucket string) (blobList []BlobRecord, err error)
	DeleteUnreferencedBlob(ctx context.Context, bucket string, hash string) error
	Close() error
}

type ObjectState string

const (
	ObjectStateReported  ObjectState = "reported"
	ObjectStateBlobReady ObjectState = "blob_ready"
	ObjectStatePointer   ObjectState = "pointer"
)

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
	HashAlgo     string
	LastSeenScan string
	State        ObjectState
}

type ObjectStatus struct {
	Unchanged bool
	State     ObjectState
	RefCount  int64
}

type BlobRecord struct {
	Bucket string
	Key    string
	Hash   string
	Size   int64
}

type Stats struct {
	UniqueBlobs      int64
	DuplicatesFound  int64
	BytesReclaimable int64
}
