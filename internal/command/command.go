package command

import (
	"fmt"
	"os"

	"s3-dedup/internal/cache"
	"s3-dedup/internal/config"
	"s3-dedup/internal/report"
	"s3-dedup/internal/s3"
	"s3-dedup/internal/scanner"

	"github.com/spf13/cobra"
)

var configPath string
var reportPath string

var rootCmd = &cobra.Command{
	Use:   "s3-dedup",
	Short: "File deduplicator for S3-storage",
	Long:  "Service-deduplicator for object S3 storage",
}

var scanOnce = &cobra.Command{
	Use:   "scan-once",
	Short: "Does one lap through S3 storage",
	Long:  "Reads config file, analyzes S3 storage and forms a report",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		config, err := config.ConfigParser(configPath)
		if err != nil {
			return fmt.Errorf("Error parsing config file: %w", err)
		}

		s3Client, err := s3.NewClient(ctx, config)
		if err != nil {
			return fmt.Errorf("Error creating S3 client: %w", err)
		}

		err = s3Client.HealthCheck(ctx, config)
		if err != nil {
			return fmt.Errorf("HealthCheck failed: %w", err)
		}

		store, err := cache.OpenSQLite(config.Cache.Path)
		if err != nil {
			return fmt.Errorf("Error opening state db: %w", err)
		}
		defer store.Close()

		scanner := scanner.NewScanner(
			s3Client,
			store,
			config,
		)

		scanReport, err := scanner.ScanOnce(ctx)

		if reportPath != "" {
			err := report.WriteJSON(reportPath, scanReport)
			if err != nil {
				return err
			}
		}

		return nil
	},
}

func init() {
	scanOnce.Flags().StringVarP(&configPath, "config", "c", "", "Config path")
	scanOnce.MarkFlagRequired("config")
	scanOnce.Flags().StringVarP(&reportPath, "out", "o", "", "Report path")
	rootCmd.AddCommand(scanOnce)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
