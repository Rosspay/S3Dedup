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
	"s3-dedup/internal/cache"
	"s3-dedup/internal/config"
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
		ctx := cmd.Context()

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
		ctx := cmd.Context()
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
		if interval <= 0 {
			return fmt.Errorf("Scan interval must be > 0")
		}

		return runLoop(ctx, interval, application.Scanner.ScanOnce, reportPath)
	},
}

var reportCommand = &cobra.Command{
	Use:   "report",
	Short: "Gets a report from previous scan",
	Long:  "Gets a report from previous scan",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		r, err := report.ReadJSON("report.json")
		if err != nil {
			return fmt.Errorf("ReadJSON error: %w", err)
		}

		cfg, err := config.ConfigParser(configPath)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}

		store, err := cache.OpenSQLite(cfg.Cache.Path)
		if err != nil {
			return fmt.Errorf("open cache: %w", err)
		}
		defer store.Close()

		stats, err := store.GetStats(ctx)
		r.UniqueBlobs = stats.UniqueBlobs
		r.DuplicatesFound = stats.DuplicatesFound
		r.BytesReclaimable = stats.BytesReclaimable

		err = report.WriteJSON(reportPath, r)
		if err != nil {
			return fmt.Errorf("WriteJSON error: %w", err)
		}
		fmt.Printf("%+v", r)
		return nil
	},
}

func run(ctx context.Context, scan ScanFunc, out string) error {
	scanReport, scanErr := scan(ctx)

	var writeErr error
	if out != "" {
		fmt.Printf("%+v\n", scanReport)
		writeErr = report.WriteJSON(out, scanReport)
	}
	return errors.Join(scanErr, writeErr)
}

func runLoop(ctx context.Context, interval time.Duration, scan ScanFunc, out string) error {
	stopCh := shutdownRequested(ctx)
	i := 0
	for {
		//Checking before scan if shutdown requested, cancelling after first signal
		select {
		case <-stopCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fmt.Printf("Scan N%d starts\n", i)
		if err := run(ctx, scan, out); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			fmt.Printf("scan failed: %v\n", err)
		}
		i++

		//Checking signal after scan, so there is no timer waiting
		select {
		case <-stopCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		timer := time.NewTimer(interval)

		//During waiting canceling after first signal
		select {
		case <-timer.C:
		case <-stopCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		}
	}
}

func init() {
	reportPath = "report.json"
	scanOnce.Flags().StringVarP(&configPath, "config", "c", "", "Config path")
	scanOnce.MarkFlagRequired("config")

	runInterval.Flags().StringVarP(&configPath, "config", "c", "", "Config path")
	runInterval.MarkFlagRequired("config")

	reportCommand.Flags().StringVarP(&configPath, "config", "c", "", "Config path")
	reportCommand.Flags().StringVarP(&reportPath, "out", "o", "", "Report path")
	reportCommand.MarkFlagRequired("out")
	rootCmd.AddCommand(scanOnce)
	rootCmd.AddCommand(runInterval)
	rootCmd.AddCommand(reportCommand)
}

type shutdownKey struct{}

func shutdownRequested(ctx context.Context) <-chan struct{} {
	if ch, ok := ctx.Value(shutdownKey{}).(<-chan struct{}); ok {
		return ch
	}
	return ctx.Done()
}

func Execute() {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	stopRequested := make(chan struct{})
	operationCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx := context.WithValue(
		operationCtx,
		shutdownKey{},
		(<-chan struct{})(stopRequested),
	)

	go func() {
		select {
		case <-signals:
			fmt.Println("Shutdown requested: finishing current scan")
			close(stopRequested)
		case <-operationCtx.Done():
			return
		}

		select {
		case <-signals:
			fmt.Println("Forced shutdown: cancelling current operation")
			close(stopRequested)
		case <-operationCtx.Done():
			return
		}
	}()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
