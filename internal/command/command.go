package command

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"s3-dedup/internal/cache"
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
		scanID := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
		objectsScanned := 0
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
		store, err := cache.OpenSQLite(config.Cache.Path)
		defer store.Close()
		for _, bucket := range config.S3.Buckets {
			fmt.Printf("Bucket: %s\tPrefix: %s\n", bucket.Name, bucket.Prefix)

			// Getting objects in stream
			err := s3Client.ListObjects(ctx, bucket.Name, bucket.Prefix, false, func(info minio.ObjectInfo) error {
				// Marking an object anyway even if error will occur
				store.MarkObjectSeen(ctx, bucket.Name, info.Key, scanID)
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
				record := cache.ObjectRecord{
					Bucket:       bucket.Name,
					Key:          info.Key,
					ETag:         info.ETag,
					Size:         info.Size,
					LastModified: info.LastModified,
					Hash:         hash,
					LastSeenScan: scanID,
				}
				err = store.RegisterObject(ctx, record)
				if err != nil {
					return err
				}
				//Counting objects scanned for report
				objectsScanned++
				return nil
			})
			if err != nil {
				return fmt.Errorf("Error listing objects in %q: %w", bucket.Name, err)
			}
			removed, err := store.FinalizeScope(ctx, bucket.Name, bucket.Prefix, scanID)
			if err != nil {
				return err
			}
			fmt.Printf("Number of objects removed: %d\n", removed)
		}
		stats, err := store.GetStats(ctx)
		if err != nil {
			return fmt.Errorf("Error getting cache stats: %w", err)
		}
		fmt.Printf("unique_blobs: %d\nduplicates_found: %d\nbytes_reclaimable: %d\n",
			stats.UniqueBlobs, stats.DuplicatesFound, stats.BytesReclaimable)
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
