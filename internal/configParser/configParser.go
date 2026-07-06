package configParser

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"

	"github.com/kelseyhightower/envconfig"
	"gopkg.in/yaml.v2"
)

// Structures to convert a yaml file to
type Bucket struct {
	Name   string `yaml:"name"`
	Prefix string `yaml:"prefix"`
}

type S3 struct {
	Endpoint       string   `yaml:"endpoint"`
	Region         string   `yaml:"region"`
	Access_key     string   `yaml:"access_key" envconfig:"S3_ACCESS_KEY"`
	Secret_key     string   `yaml:"secret_key" envconfig:"S3_SECRET_KEY"`
	Use_path_style bool     `yaml:"use_path_style"`
	Buckets        []Bucket `yaml:"buckets"`
}

type Dedup struct {
	Hash_algo        string `yaml:"hash_algo"`
	Min_size_bytes   int    `yaml:"min_size_bytes"`
	Blob_prefix      string `yaml:"blob_prefix"`
	Mode             string `yaml:"mode"`
	Delete_originals bool   `yaml:"delete_originals"`
}

type Cache struct {
	Backend string `yaml:"backend"`
	Path    string `yaml:"path"`
}

type Schedule struct {
	Scan_interval string `yaml:"scan_interval"`
	Workers       int    `yaml:"workers"`
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
func Config_parser(filePath string) (*Config, error) {
	filename, _ := filepath.Abs(filePath)
	yamlFile, err := os.ReadFile(filename)

	if err != nil {
		return nil, errors.New("Config path error: No such file")
	}

	var config Config

	decoder := yaml.NewDecoder(bytes.NewReader(yamlFile))
	decoder.SetStrict(true)

	err = decoder.Decode(&config)
	if err != nil {
		return nil, errors.New("Parsing config file error: Wrong config structure")
	}

	err = envconfig.Process("", &config)
	if err != nil {
		return nil, errors.New("Parsing env variables error")
	}
	return &config, nil
}
