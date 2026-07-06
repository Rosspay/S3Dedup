package s3

import (
	"context"
	"s3-dedup/internal/configParser"

	"github.com/minio/minio-go/v6"
)

type Client struct {
	S3Client *minio.Client
}

func NewClient(ctx context.Context, config *configParser.Config) (*Client, error) {
	client, err := minio.New(config.S3.Endpoint, config.S3.Access_key, config.S3.Secret_key, false)
	if err != nil {
		return nil, err
	}
	return &Client{S3Client: client}, nil
}
