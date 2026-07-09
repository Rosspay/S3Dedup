package command

import (
	"context"
	"fmt"
	"os"

	"s3-dedup/internal/configParser"
	"s3-dedup/internal/hashing"
	"s3-dedup/internal/s3"

	"github.com/minio/minio-go/v6"
	"github.com/spf13/cobra"
)

var configPath string

var rootCmd = &cobra.Command{
	Use:   "s3-dedup",
	Short: "File deduplicator for S3-storage",
	Long:  "Service-deduplicator for object S3 storage",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		config, err := configParser.Config_parser(configPath)
		if err != nil {
			return fmt.Errorf("Error parsing config file: %w", err)
		}
		s3Client, err := s3.NewClient(ctx, config)
		if err != nil {
			return fmt.Errorf("Error creating S3 client: %w", err)
		}
		err = s3Client.HealthCheck(cmd.Context(), config)
		if err != nil {
			return fmt.Errorf("Health check failed: %w", err)
		}
		for _, bucket := range config.S3.Buckets {
			fmt.Printf("Bucket: %s\tPrefix: %s\n", bucket.Name, bucket.Prefix)

			// Getting objects in stream
			err := s3Client.ListObjects(ctx, bucket.Name, bucket.Prefix, true, func(info minio.ObjectInfo) error {
				// Filtering objects that are below min size from config
				if info.Size < int64(config.Dedup.Min_size_bytes) {
					return nil
				}
				hash, err := processObject(ctx, s3Client, bucket.Name, info, config.Dedup.Hash_algo)
				if err != nil {
					return err
				}
				fmt.Printf("Key: %s, Size: %d, Etag: %s, Last modified: %s, Hash: %s\n",
					info.Key, info.Size, info.ETag, info.LastModified, hash)
				return nil
			})
			if err != nil {
				return fmt.Errorf("Error listing objects in %q: %w", bucket.Name, err)
			}
		}
		return nil
	},
}

// processObject streams a single object's content and returns its content hash.
// It is a standalone function so that the deferred Close runs after every object
// (not at the end of the whole scan), keeping open connections bounded.
func processObject(ctx context.Context, client *s3.Client, bucket string,
	info minio.ObjectInfo, algo string) (string, error) {
	obj, err := client.S3Client.GetObjectWithContext(ctx, bucket, info.Key, minio.GetObjectOptions{})
	if err != nil {
		return "", fmt.Errorf("get object %q: %w", info.Key, err)
	}
	defer obj.Close()

	hash, err := hashing.HashReader(obj, algo)
	if err != nil {
		return "", fmt.Errorf("hash object %q: %w", info.Key, err)
	}
	return hash, nil
}

func init() {
	rootCmd.Flags().StringVarP(&configPath, "config", "c", "", "Config path")
	rootCmd.MarkFlagRequired("config")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
