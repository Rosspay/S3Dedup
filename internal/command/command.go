package command

import (
	"fmt"
	"os"

	"s3-dedup/internal/configParser"
	"s3-dedup/internal/s3"

	"github.com/spf13/cobra"
)

var configPath string

var rootCmd = &cobra.Command{
	Use:   "s3-dedup",
	Short: "File deduplicator for S3-storage",
	Long:  "Service-deduplicator for object S3 storage",
	RunE: func(cmd *cobra.Command, args []string) error {
		config, err := configParser.Config_parser(configPath)
		if err != nil {
			return fmt.Errorf("Error parsing config file: %w", err)
		}
		s3Client, err := s3.NewClient(cmd.Context(), config)
		err = s3Client.HealthCheck(cmd.Context(), config)
		if err != nil {
			return fmt.Errorf("Health check failed: %w", err)
		}
		// for _, bucket := range config.S3.Buckets {
		// 	objsInfo, err := s3Client.ListObjects(cmd.Context(), bucket.Name, bucket.Prefix, true, /* callback func I guess*/)
		// 	if err != nil {
		// 		return fmt.Errorf("Error reading objects info: %w", err)
		// 	}
		// 	fmt.Printf("Bucket: %s	Prefix: %s\n", bucket.Name, bucket.Prefix)
		// 	for _, oInfo := range objsInfo {
		// 		fmt.Printf("Key: %s, Size: %d, Etag: %s, Last modified: %s\n", oInfo.Key, oInfo.Size, oInfo.ETag, oInfo.LastModified)
		// 	}

		// }
		return nil
	},
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
