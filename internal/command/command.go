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
	"s3-dedup/internal/logger"
	"s3-dedup/internal/report"

	"github.com/spf13/cobra"
)

var configPath string
var scanReportPath string
var runReportPath string
var reportOutputPath string

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

		return run(ctx, application.Scanner.ScanOnce, scanReportPath)
	},
}

var runInterval = &cobra.Command{
	Use:   "run",
	Short: "Scans S3 storage in interval from config",
	Long:  "Scans S3 storage in interval from config",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runPeriodic(cmd.Context(), configPath, runReportPath)
	},
}

var reportCommand = &cobra.Command{
	Use:   "report",
	Short: "Gets a report from previous scan",
	Long:  "Gets a report from previous scan",
	RunE: func(cmd *cobra.Command, args []string) error {
		r, err := report.ReadJSON("report.json")
		if err != nil {
			return fmt.Errorf("ReadJSON error: %w", err)
		}
		err = report.WriteJSON(reportOutputPath, r)
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

type periodicJob struct {
	application *app.App
	interval    time.Duration
	out         string
}

func newPeriodicJob(
	ctx context.Context,
	configPath,
	out string,
) (*periodicJob, error) {
	application, err := app.Open(ctx, configPath)
	if err != nil {
		return nil, err
	}

	interval, err := time.ParseDuration(
		application.Config.Schedule.ScanInterval,
	)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("parse scan interval: %w", err),
			application.Close(),
		)
	}
	if interval <= 0 {
		return nil, errors.Join(
			errors.New("scan interval must be > 0"),
			application.Close(),
		)
	}

	return &periodicJob{
		application: application,
		interval:    interval,
		out:         out,
	}, nil
}

func (j *periodicJob) Run(ctx context.Context) (resultErr error) {
	defer func() {
		resultErr = errors.Join(resultErr, j.application.Close())
	}()

	return runLoop(
		ctx,
		j.interval,
		j.application.Scanner.ScanOnce,
		j.out,
		j.application.Logger,
	)
}

func runPeriodic(ctx context.Context, configPath, out string) error {
	job, err := newPeriodicJob(ctx, configPath, out)
	if err != nil {
		return err
	}
	return job.Run(ctx)
}

func runLoop(ctx context.Context, interval time.Duration, scan ScanFunc, out string, logger *logger.Logger) error {
	stopCh := shutdownRequested(ctx)
	i := 0
	for {
		//Checking before scan if shutdown requested, cancelling after first signal
		select {
		case <-stopCh:
			logger.Infof("First shutdown signal got, shutting down before next scan\n")
			return nil
		case <-ctx.Done():
			logger.Errorf("Second shutdown signal got, shutting down before next scan\n")
			return ctx.Err()
		default:
		}

		logger.Infof("Scan N%d starts\n", i)
		if err := run(ctx, scan, out); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logger.Errorf("scan failed: %v\n", err)
		}
		i++

		//Checking signal after scan, so there is no timer waiting
		select {
		case <-stopCh:
			logger.Infof("First shutdown signal got, shutting down before next scan\n")
			return nil
		case <-ctx.Done():
			logger.Errorf("Second shutdown signal got, shutting down before next scan\n")
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
	scanOnce.Flags().StringVarP(&configPath, "config", "c", "", "Config path")
	scanOnce.Flags().StringVarP(&scanReportPath, "out", "o", "report.json", "Report path")
	scanOnce.MarkFlagRequired("config")

	runInterval.Flags().StringVarP(&configPath, "config", "c", "", "Config path")
	runInterval.Flags().StringVarP(&runReportPath, "out", "o", "report.json", "Report path")
	runInterval.MarkFlagRequired("config")

	reportCommand.Flags().StringVarP(&reportOutputPath, "out", "o", "", "Report path")
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
			cancel()
		case <-operationCtx.Done():
			return
		}
	}()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
