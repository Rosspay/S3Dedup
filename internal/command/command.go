package command

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"s3-dedup/internal/app"
	"s3-dedup/internal/report"

	"github.com/spf13/cobra"
)

var configPath string
var reportPath string

type ScanFunc func(context.Context) (report.Report, error)

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
		ctx, stop := signal.NotifyContext(
			cmd.Context(),
			os.Interrupt,
			syscall.SIGTERM,
		)
		defer stop()

		application, err := app.Open(ctx, configPath)
		if err != nil {
			return err
		}
		defer application.Close()

		return run(ctx, application.Scanner.ScanOnce, reportPath)
	},
}

var runInterval = &cobra.Command{
	Use:   "run",
	Short: "Scans S3 storage in interval from config",
	Long:  "Scans S3 storage in interval from config",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(
			cmd.Context(),
			os.Interrupt,
			syscall.SIGTERM,
		)
		defer stop()

		application, err := app.Open(ctx, configPath)
		if err != nil {
			return err
		}
		defer application.Close()

		interval, err := time.ParseDuration(
			application.Config.Schedule.ScanInterval,
		)
		if err != nil {
			return fmt.Errorf("Parse scan interval: %w", err)
		}

		return runLoop(ctx, interval, application.Scanner.ScanOnce, reportPath)
	},
}

func run(ctx context.Context, scan ScanFunc, out string) error {
	scanReport, scanErr := scan(ctx)

	var writeErr error
	if out != "" {
		scanReport.ScanFinished = time.Now().UTC()
		fmt.Printf("%+v\n", scanReport)
		writeErr = report.WriteJSON(reportPath, scanReport)
	}
	return errors.Join(scanErr, writeErr)
}

func runLoop(ctx context.Context, interval time.Duration, scan ScanFunc, out string) error {
	i := 0
	for {
		fmt.Printf("Scan N%d starts\n", i)
		if err := run(ctx, scan, out); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			fmt.Printf("scan failed: %v\n", err)
		}

		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		}
	}
}

func init() {
	scanOnce.Flags().StringVarP(&configPath, "config", "c", "", "Config path")
	scanOnce.MarkFlagRequired("config")
	scanOnce.Flags().StringVarP(&reportPath, "out", "o", "", "Report path")
	runInterval.Flags().StringVarP(&configPath, "config", "c", "", "Config path")
	runInterval.MarkFlagRequired("config")
	runInterval.Flags().StringVarP(&reportPath, "out", "o", "", "Report path")
	rootCmd.AddCommand(scanOnce)
	rootCmd.AddCommand(runInterval)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
