package command

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	kardianos "github.com/kardianos/service"
	"github.com/spf13/cobra"
)

const (
	serviceName         = "s3-dedup"
	serviceDisplayName  = "S3 Deduplication Service"
	serviceDescription  = "Deduplicates objects in configured S3-compatible storage"
	serviceStartTimeout = 20 * time.Second
)

var installConfigPath string
var serviceConfigPath string
var serviceReportPath string

var installCommand = &cobra.Command{
	Use:   "install",
	Short: "Installs s3-dedup as an operating system service",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		executable, configPath, reportPath, err := resolveServicePaths(
			installConfigPath,
		)
		if err != nil {
			return err
		}

		svc, err := kardianos.New(
			&serviceProgram{},
			newServiceConfig(executable, configPath, reportPath),
		)
		if err != nil {
			return fmt.Errorf("create service: %w", err)
		}
		if err := svc.Install(); err != nil {
			return fmt.Errorf("install service: %w", err)
		}

		fmt.Fprintln(cmd.OutOrStdout(), "Service installed")
		return nil
	},
}

var uninstallCommand = &cobra.Command{
	Use:   "uninstall",
	Short: "Uninstalls the s3-dedup operating system service",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		executable, err := os.Executable()
		if err != nil {
			return fmt.Errorf("get executable path: %w", err)
		}

		svc, err := kardianos.New(
			&serviceProgram{},
			newServiceConfig(executable, "", ""),
		)
		if err != nil {
			return fmt.Errorf("create service: %w", err)
		}

		status, statusErr := svc.Status()
		if statusErr == nil && status == kardianos.StatusRunning {
			if err := svc.Stop(); err != nil {
				return fmt.Errorf("stop service before uninstall: %w", err)
			}
		}
		if err := svc.Uninstall(); err != nil {
			return fmt.Errorf("uninstall service: %w", err)
		}

		fmt.Fprintln(cmd.OutOrStdout(), "Service uninstalled")
		return nil
	},
}

var serviceRunCommand = &cobra.Command{
	Use:    "service-run",
	Short:  "Runs s3-dedup under the operating system service manager",
	Args:   cobra.NoArgs,
	Hidden: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		configPath, err := filepath.Abs(serviceConfigPath)
		if err != nil {
			return fmt.Errorf("resolve service config path: %w", err)
		}
		reportPath, err := filepath.Abs(serviceReportPath)
		if err != nil {
			return fmt.Errorf("resolve service report path: %w", err)
		}
		if err := os.Chdir(filepath.Dir(configPath)); err != nil {
			return fmt.Errorf("set service working directory: %w", err)
		}

		executable, err := os.Executable()
		if err != nil {
			return fmt.Errorf("get executable path: %w", err)
		}

		program := newServiceProgram(
			cmd.Context(),
			func(ctx context.Context) (func() error, error) {
				startupCtx, cancel := context.WithTimeout(
					ctx,
					serviceStartTimeout,
				)
				defer cancel()

				job, err := newPeriodicJob(startupCtx, configPath, reportPath)
				if err != nil {
					return nil, err
				}
				return func() error {
					return job.Run(ctx)
				}, nil
			},
		)
		svc, err := kardianos.New(
			program,
			newServiceConfig(executable, configPath, reportPath),
		)
		if err != nil {
			return fmt.Errorf("create service: %w", err)
		}
		if err := svc.Run(); err != nil {
			if serviceLogger, loggerErr := svc.Logger(nil); loggerErr == nil {
				_ = serviceLogger.Error(err)
			}
			return fmt.Errorf("run service: %w", err)
		}
		return nil
	},
}

func init() {
	installCommand.Flags().StringVarP(
		&installConfigPath,
		"config",
		"c",
		"",
		"Config path",
	)
	installCommand.MarkFlagRequired("config")

	serviceRunCommand.Flags().StringVarP(
		&serviceConfigPath,
		"config",
		"c",
		"",
		"Config path",
	)
	serviceRunCommand.Flags().StringVarP(
		&serviceReportPath,
		"out",
		"o",
		"",
		"Report path",
	)
	serviceRunCommand.MarkFlagRequired("config")
	serviceRunCommand.MarkFlagRequired("out")

	rootCmd.AddCommand(installCommand)
	rootCmd.AddCommand(uninstallCommand)
	rootCmd.AddCommand(serviceRunCommand)
}

func resolveServicePaths(configPath string) (
	executable string,
	absoluteConfigPath string,
	reportPath string,
	err error,
) {
	absoluteConfigPath, err = filepath.Abs(configPath)
	if err != nil {
		return "", "", "", fmt.Errorf("resolve config path: %w", err)
	}
	info, err := os.Stat(absoluteConfigPath)
	if err != nil {
		return "", "", "", fmt.Errorf("check config path: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", "", "", fmt.Errorf(
			"config path %q is not a regular file",
			absoluteConfigPath,
		)
	}

	executable, err = os.Executable()
	if err != nil {
		return "", "", "", fmt.Errorf("get executable path: %w", err)
	}
	executable, err = filepath.Abs(executable)
	if err != nil {
		return "", "", "", fmt.Errorf("resolve executable path: %w", err)
	}

	reportPath = filepath.Join(filepath.Dir(absoluteConfigPath), "report.json")
	return executable, absoluteConfigPath, reportPath, nil
}

func newServiceConfig(executable, configPath, reportPath string) *kardianos.Config {
	cfg := &kardianos.Config{
		Name:        serviceName,
		DisplayName: serviceDisplayName,
		Description: serviceDescription,
		Executable:  executable,
	}
	if configPath == "" {
		return cfg
	}

	cfg.WorkingDirectory = filepath.Dir(configPath)
	cfg.Arguments = []string{
		"service-run",
		"--config",
		configPath,
		"--out",
		reportPath,
	}
	return cfg
}

type serviceProgram struct {
	parent  context.Context
	prepare func(context.Context) (func() error, error)

	mu            sync.Mutex
	started       bool
	stopRequested chan struct{}
	stopOnce      sync.Once
	done          chan struct{}
	runErr        error
}

func newServiceProgram(
	parent context.Context,
	prepare func(context.Context) (func() error, error),
) *serviceProgram {
	return &serviceProgram{
		parent:  parent,
		prepare: prepare,
	}
}

func (p *serviceProgram) Start(kardianos.Service) error {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return errors.New("service program is already started")
	}
	if p.prepare == nil {
		p.mu.Unlock()
		return errors.New("service run function is not configured")
	}

	parent := p.parent
	if parent == nil {
		parent = context.Background()
	}
	operationCtx, cancel := context.WithCancel(parent)
	stopRequested := make(chan struct{})
	ctx := context.WithValue(
		operationCtx,
		shutdownKey{},
		(<-chan struct{})(stopRequested),
	)

	p.started = true
	p.stopRequested = stopRequested
	p.done = make(chan struct{})
	p.mu.Unlock()

	run, err := p.prepare(ctx)
	if err != nil {
		cancel()
		p.mu.Lock()
		p.runErr = err
		close(p.done)
		p.mu.Unlock()
		return err
	}

	go func() {
		defer cancel()
		err := run()

		p.mu.Lock()
		p.runErr = err
		close(p.done)
		p.mu.Unlock()
	}()

	return nil
}

func (p *serviceProgram) Stop(kardianos.Service) error {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return nil
	}
	stopRequested := p.stopRequested
	done := p.done
	p.mu.Unlock()

	p.stopOnce.Do(func() {
		close(stopRequested)
	})
	<-done

	p.mu.Lock()
	err := p.runErr
	p.mu.Unlock()
	return err
}
