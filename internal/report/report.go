package report

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Report struct {
	ScanStarted      time.Time `json:"scan_started"`
	ScanFinished     time.Time `json:"scan_finished"`
	ObjectsScanned   int64     `json:"objects_scanned"`
	UniqueBlobs      int64     `json:"unique_blobs"`
	DuplicatesFound  int64     `json:"duplicates_found"`
	BytesReclaimable int64     `json:"bytes_reclaimable"`
	BytesReclaimed   int64     `json:"bytes_reclaimed"`
	ObjectsRelinked  int64     `json:"objects_relinked"`
	Errors           int64     `json:"errors"`
	Mode             string    `json:"mode"`
}

func WriteJSON(path string, report Report) error {
	filename, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("Path does not exists")
	}
	ext := filepath.Ext(filename)
	if ext != ".json" {
		return fmt.Errorf("Report file must be in json format")
	}
	file, err := os.Create(filename)

	if err != nil {
		return fmt.Errorf("Error creating a file: %v", err)
	}

	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "\t")
	err = encoder.Encode(report)

	if err != nil {
		return fmt.Errorf("Error encoding to json: %v", err)
	}
	return nil
}

func ReadJSON(path string) (Report, error) {
	var report Report
	filename, _ := filepath.Abs(path)
	jsonFile, err := os.ReadFile(filename)

	if err != nil {
		return Report{}, fmt.Errorf("Report path error: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(jsonFile))
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&report)
	if err != nil {
		return Report{}, fmt.Errorf("Report structure error: %w", err)
	}

	return report, nil
}
