package configParser

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

// Basic test to see if parsed data of a set yaml file parsed the exact way we want to
func TestBasicConfigParser(t *testing.T) {
	expectedConfig := Config{
		S3: S3{
			Endpoint:       "https://s3.example.local:9000",
			Region:         "us-east-1",
			Access_key:     "configAccessKey",
			Secret_key:     "configSecretKey",
			Use_path_style: true,
			Buckets: []Bucket{
				{
					Name:   "intercepted",
					Prefix: "2026/",
				},
			},
		},
		Dedup: Dedup{
			Hash_algo:        "sha256",
			Min_size_bytes:   4096,
			Blob_prefix:      "blobs/",
			Mode:             "report_only",
			Delete_originals: false,
		},
		Cache: Cache{
			Backend: "sqlite",
			Path:    "/var/lib/s3-dedup/state.db",
		},
		Schedule: Schedule{
			Scan_interval: "1h",
			Workers:       8,
		},
		Logging: Logging{
			Level: "info",
			File:  "/var/log/s3-dedup/service.log",
		},
	}
	resultConfig, err := Config_parser("./test_config.yaml")
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
	resultConfig, err := Config_parser("./test_config.yaml")
	isEqualKey := s3key == resultConfig.S3.Access_key
	isEqualSecret := s3secret == resultConfig.S3.Secret_key
	if !isEqualKey || !isEqualSecret || err != nil {
		t.Error("Reading env var failed")
	}
}

// Testing error handling for passing wrong path
func TestErrorNoSuchFile(t *testing.T) {
	resultConfig, err := Config_parser("./no_such_file.yaml")
	if resultConfig != nil || err.Error() != "Config path error: No such file" {
		t.Error("Error handling with passing wrong file failed")
	}
}

// Testing error handling for passing wrong config structure
func TestWrongFileStructure(t *testing.T) {
	resultConfig, err := Config_parser("./wrong_structure.yaml")
	if resultConfig != nil || err.Error() != "Parsing config file error: Wrong config structure" {
		t.Error("Error handling with passing a config with a wrong structure failed")
	}
}
