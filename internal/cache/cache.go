package cache

import (
	"context"
	"time"
)

type Store interface {
	RegisterObject(ctx context.Context, object ObjectRecord) error
	UnregisterObject(ctx context.Context, bucket string, key string) error
	GetObjectStatus(
		ctx context.Context,
		bucket string,
		key string,
		etag string,
		size int64,
		hashAlgo string,
		lastModified time.Time,
	) (status ObjectStatus, err error)
	GetObjectStatuses(
		ctx context.Context,
		bucket string,
		objects []ObjectMetadata,
		hashAlgo string,
	) ([]ObjectStatus, error)
	ApplyDiscoveryBatch(ctx context.Context, mutations []DiscoveryMutation) error
	ApplyDedupBatch(ctx context.Context, objects []ObjectRecord) error
	ListDedupCandidates(
		ctx context.Context,
		bucket string,
		prefix string,
		desiredState ObjectState,
		afterKey string,
		limit int,
	) ([]DedupCandidate, error)
	GetStats(ctx context.Context, scopes ...Scope) (Stats, error)
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

type DiscoveryMutationKind uint8

const (
	DiscoveryRegister DiscoveryMutationKind = iota
	DiscoveryMarkSeen
	DiscoveryUnregister
)

type ObjectID struct {
	Bucket string
	Key    string
	ScanID string
}

type DiscoveryMutation struct {
	Kind   DiscoveryMutationKind
	Object ObjectRecord
	ID     ObjectID
}

type DedupCandidate struct {
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
	HashAlgo     string
	LastSeenScan string
	State        ObjectState
}

type ObjectStatus struct {
	Unchanged bool
	State     ObjectState
	RefCount  int64
}

type ObjectMetadata struct {
	Key          string
	ETag         string
	Size         int64
	LastModified time.Time
}

type BlobRecord struct {
	Bucket string
	Key    string
	Hash   string
	Size   int64
}

type Scope struct {
	Bucket string
	Prefix string
}

type Stats struct {
	UniqueBlobs      int64
	DuplicatesFound  int64
	BytesReclaimable int64
}
