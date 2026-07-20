package s3

import (
	"context"
	"fmt"
	"io"
	"s3-dedup/internal/config"

	"github.com/minio/minio-go/v6"
	"github.com/minio/minio-go/v6/pkg/credentials"
)

// Wrapper for minio client
type Client struct {
	S3Client *minio.Client
}

// Client constructor
// Receives context, config
// Returns pointer to a client and possible errors
func NewClient(ctx context.Context, config *config.Config) (*Client, error) {
	client, err := minio.NewWithOptions(config.S3.Endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(
			config.S3.AccessKey,
			config.S3.SecretKey,
			"",
		),
		Secure:       false,
		Region:       config.S3.Region,
		BucketLookup: ternary(config.S3.UsePathStyle, minio.BucketLookupPath, minio.BucketLookupAuto),
	})

	if err != nil {
		return nil, fmt.Errorf("Error creating S3 client: %w", err)
	}
	return &Client{S3Client: client}, nil
}

// Health check that:
// 1. Verifies that client can reach the S3 storage
// 2. Verifies that every bucket listed in config exists and is accesible
// Receives context and config
// Returns an error on the first bucket that fails
func (c *Client) HealthCheck(ctx context.Context, config *config.Config) error {
	if len(config.S3.Buckets) == 0 {
		return fmt.Errorf("healthcheck: no buckets configured")
	}

	for _, bucket := range config.S3.Buckets {
		exists, err := c.S3Client.BucketExistsWithContext(ctx, bucket.Name)
		if err != nil {
			return fmt.Errorf("healthcheck bucket %q: %w", bucket.Name, err)
		}
		if !exists {
			return fmt.Errorf("healthcheck bucket %q: bucket does not exist or is not accessible", bucket.Name)
		}
	}

	return nil
}

func (c *Client) ListObjects(ctx context.Context, bucket string, prefix string, recursive bool, fn func(minio.ObjectInfo) error) error {
	for object := range c.S3Client.ListObjectsV2(bucket, prefix, recursive, ctx.Done()) {
		if object.Err != nil {
			// TODO logger
			return fmt.Errorf("List objects %q: %w", bucket, object.Err)
		}
		err := fn(object)
		if err != nil {
			// TODO logger
			return fmt.Errorf("Error reading object: %w\n", err)
		}
	}
	return nil
}

func (c *Client) GetObject(ctx context.Context, bucket string, key string) (io.ReadCloser, error) {
	obj, err := c.S3Client.GetObjectWithContext(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("Get object %q: %w", key, err)
	}
	return obj, nil
}

func ternary[T any](cond bool, trueVal, falseVal T) T {
	if cond {
		return trueVal
	}
	return falseVal
}
