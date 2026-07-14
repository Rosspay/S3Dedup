package report

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestWriteJsonBasic(t *testing.T) {
	report := Report{
		ScanStarted:      time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC),
		ScanFinished:     time.Date(2026, time.July, 1, 0, 5, 0, 0, time.UTC),
		ObjectsScanned:   5,
		UniqueBlobs:      3,
		DuplicatesFound:  2,
		BytesReclaimable: 200,
		BytesReclaimed:   0,
		ObjectsRelinked:  0,
		Errors:           1,
		Mode:             "report_only",
	}

	file := filepath.Join(t.TempDir(), "test.json")
	err := WriteJSON(file, report)
	if err != nil {
		t.Fatalf("WriteJSON fatal error: %v", err)
	}
	jsonFile, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("WriteJSON did't create a file: %v", err)
	}

	var gotReport Report

	decoder := json.NewDecoder(bytes.NewReader(jsonFile))
	err = decoder.Decode(&gotReport)
	if err != nil {
		t.Fatalf("Decoding file error: %v", err)
	}

	isEqual := cmp.Equal(&report, &gotReport)
	if !isEqual {
		t.Error("WriteJSON basic test failed")
	}
}

func TestWriteJsonNotJson(t *testing.T) {
	var report Report
	err := WriteJSON("notjson.txt", report)
	if err == nil {
		t.Fatal("Must be an error with wrong file extension")
	}
}

func TestWriteJsonPathDoesNotExist(t *testing.T) {
	var report Report
	err := WriteJSON("path/does/not/exist/test.json", report)
	if err == nil {
		t.Fatal("Must be an error creating a file")
	}
}
