package app

import (
	"context"
	"errors"
	"fmt"
	"s3-dedup/internal/cache"
	"s3-dedup/internal/config"
	"s3-dedup/internal/logger"
	"s3-dedup/internal/s3"
	"s3-dedup/internal/scanner"
)

type App struct {
	Config  *config.Config
	Scanner *scanner.Scanner
	store   cache.Store
	Logger  *logger.Logger
}

func Open(ctx context.Context, configPath string) (*App, error) {
	cfg, err := config.ConfigParser(configPath)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	logging, err := logger.New(cfg.Logging.Level, cfg.Logging.File)
	if err != nil {
		return nil, fmt.Errorf("logger.New: %w", err)
	}

	s3Client, err := s3.NewClient(ctx, cfg, *logging)
	if err != nil {
		logging.Errorf("create S3 client: %v", err)
		return nil, errors.Join(
			fmt.Errorf("create S3 client: %w", err),
			logging.Close(),
		)
	}

	if err := s3Client.HealthCheck(ctx, cfg); err != nil {
		logging.Errorf("S3 health check: %v", err)
		return nil, errors.Join(
			fmt.Errorf("S3 health check: %w", err),
			logging.Close(),
		)
	}

	store, err := cache.OpenSQLite(cfg.Cache.Path)
	if err != nil {
		logging.Errorf("open cache: %v", err)
		return nil, errors.Join(
			fmt.Errorf("open cache: %w", err),
			logging.Close(),
		)
	}

	scannerService := scanner.NewScanner(s3Client, store, cfg, logging)

	return &App{
		Config:  cfg,
		Scanner: scannerService,
		store:   store,
		Logger:  logging,
	}, nil
}

func (a *App) Close() error {
	storeCloseErr := a.store.Close()
	logCloseErr := a.Logger.Close()
	return errors.Join(storeCloseErr, logCloseErr)
}
