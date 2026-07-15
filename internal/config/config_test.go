package config

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

// Basic test to see if parsed data of a set yaml file parsed the exact way we want to
func TestBasicConfigParser(t *testing.T) {
	expectedConfig := Config{
		S3: S3{
			Endpoint:     "https://s3.example.local:9000",
			Region:       "us-east-1",
			AccessKey:    "configAccessKey",
			SecretKey:    "configSecretKey",
			UsePathStyle: true,
			Buckets: []Bucket{
				{
					Name:   "intercepted",
					Prefix: "2026/",
				},
			},
		},
		Dedup: Dedup{
			HashAlgo:        "sha256",
			MinSizeBytes:    4096,
			BlobPrefix:      "blobs/",
			Mode:            "report_only",
			DeleteOriginals: false,
		},
		Cache: Cache{
			Backend: "sqlite",
			Path:    "/var/lib/s3-dedup/state.db",
		},
		Schedule: Schedule{
			ScanInterval: "1h",
			Workers:      8,
		},
		Logging: Logging{
			Level: "info",
			File:  "/var/log/s3-dedup/service.log",
		},
	}
	resultConfig, err := ConfigParser("./test_config.yaml")
	isEqual := cmp.Equal(&expectedConfig, resultConfig)
	if !isEqual || err != nil {
		t.Error(`Config_parser basic test failed`)
	}
}

// Test to see if priority of envirionment variables is higher than values of config file
func TestEnvVarReading(t *testing.T) {
	s3key := "ThisMustBeS3EnvVar"
	s3secret := "ThisMustBeSecret"
	t.Setenv("S3_ACCESS_KEY", s3key)
	t.Setenv("S3_SECRET_KEY", s3secret)
	resultConfig, err := ConfigParser("./test_config.yaml")
	isEqualKey := s3key == resultConfig.S3.AccessKey
	isEqualSecret := s3secret == resultConfig.S3.SecretKey
	if !isEqualKey || !isEqualSecret || err != nil {
		t.Error("Reading env var failed")
	}
}

// Testing error handling for passing wrong path
func TestErrorNoSuchFile(t *testing.T) {
	resultConfig, err := ConfigParser("./no_such_file.yaml")
	if resultConfig != nil || err.Error() != "Config path error: No such file" {
		t.Error("Error handling with passing wrong file failed")
	}
}

// Testing error handling for passing wrong config structure
func TestWrongFileStructure(t *testing.T) {
	resultConfig, err := ConfigParser("./wrong_structure.yaml")
	if resultConfig != nil || err.Error() != "Parsing config file error: Wrong config structure" {
		t.Error("Error handling with passing a config with a wrong structure failed")
	}
}
