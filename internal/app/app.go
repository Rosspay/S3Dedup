package app

import (
	"context"
	"fmt"
	"s3-dedup/internal/cache"
	"s3-dedup/internal/config"
	"s3-dedup/internal/s3"
	"s3-dedup/internal/scanner"
)

type App struct {
	Config  *config.Config
	Scanner *scanner.Scanner
	store   cache.Store
}

func Open(ctx context.Context, configPath string) (*App, error) {
	cfg, err := config.ConfigParser(configPath)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	if cfg.Dedup.Mode != "report_only" && cfg.Dedup.Mode != "pointer" {
		return nil, fmt.Errorf("Mode %q is not supported", cfg.Dedup.Mode)
	}

	s3Client, err := s3.NewClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create S3 client: %w", err)
	}

	if err := s3Client.HealthCheck(ctx, cfg); err != nil {
		return nil, fmt.Errorf("S3 health check: %w", err)
	}

	store, err := cache.OpenSQLite(cfg.Cache.Path)
	if err != nil {
		return nil, fmt.Errorf("open cache: %w", err)
	}

	scannerService := scanner.NewScanner(s3Client, store, cfg)

	return &App{
		Config:  cfg,
		Scanner: scannerService,
		store:   store,
	}, nil
}

func (a *App) Close() error {
	return a.store.Close()
}
