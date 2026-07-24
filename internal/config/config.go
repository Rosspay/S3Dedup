package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/kelseyhightower/envconfig"
	"gopkg.in/yaml.v2"
)

// Structures to convert a yaml file to
type Bucket struct {
	Name   string `yaml:"name"`
	Prefix string `yaml:"prefix"`
}

type S3 struct {
	Endpoint     string   `yaml:"endpoint"`
	Region       string   `yaml:"region"`
	AccessKey    string   `yaml:"access_key" envconfig:"S3_ACCESS_KEY"`
	SecretKey    string   `yaml:"secret_key" envconfig:"S3_SECRET_KEY"`
	UsePathStyle bool     `yaml:"use_path_style"`
	Buckets      []Bucket `yaml:"buckets"`
}

type Dedup struct {
	HashAlgo        string `yaml:"hash_algo"`
	MinSizeBytes    int64  `yaml:"min_size_bytes"`
	BlobPrefix      string `yaml:"blob_prefix"`
	Mode            string `yaml:"mode"`
	DeleteOriginals bool   `yaml:"delete_originals"`
}

type Cache struct {
	Backend string `yaml:"backend"`
	Path    string `yaml:"path"`
}

type Schedule struct {
	ScanInterval string `yaml:"scan_interval"`
	Workers      int64  `yaml:"workers"`
}

type Logging struct {
	Level string `yaml:"level"`
	File  string `yaml:"file"`
}

// Main config structure
type Config struct {
	S3       S3       `yaml:"s3"`
	Dedup    Dedup    `yaml:"dedup"`
	Cache    Cache    `yaml:"cache"`
	Schedule Schedule `yaml:"schedule"`
	Logging  Logging  `yaml:"logging"`
}

// Config parser with env var priority
// input: a path to a yaml config file in string format
// output: pointer to a struct config and possible errors
func ConfigParser(filePath string) (*Config, error) {
	filename, _ := filepath.Abs(filePath)
	yamlFile, err := os.ReadFile(filename)

	if err != nil {
		return nil, fmt.Errorf("ConfigParser config path error: %w", err)
	}

	var cfg Config

	decoder := yaml.NewDecoder(bytes.NewReader(yamlFile))
	decoder.SetStrict(true)

	err = decoder.Decode(&cfg)
	if err != nil {
		return nil, fmt.Errorf("ConfigParser: parsing config file error: %w", err)
	}

	if cfg.Dedup.Mode != "report_only" && cfg.Dedup.Mode != "pointer" {
		return nil, fmt.Errorf("ConfigParser: mode must be either report_only or pointer, got %q", cfg.Dedup.Mode)
	}

	if cfg.Cache.Backend != "sqlite" {
		return nil, fmt.Errorf("ConfgiParser: cache backend must be sqlite, got %q", cfg.Cache.Backend)
	}

	interval, err := time.ParseDuration(cfg.Schedule.ScanInterval)
	if err != nil {
		return nil, fmt.Errorf("Parse scan interval: %w", err)
	}
	if interval <= 0 {
		return nil, fmt.Errorf("Scan interval must be > 0")
	}

	err = envconfig.Process("", &cfg)
	if err != nil {
		return nil, fmt.Errorf("ConfgiParser: parsing env variables error: %w", err)
	}
	return &cfg, nil
}
