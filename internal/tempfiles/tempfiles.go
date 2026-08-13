package tempfiles

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	DownloadPattern = "s3-dedup-download-*.tmp"
	ScannerPattern  = "s3-dedup-scanner-*.tmp"
	commitPattern   = ".s3-dedup-commit-*.tmp"
)

func Create(directory, pattern string) (*os.File, error) {
	file, err := os.CreateTemp(directory, pattern)
	if err != nil {
		return nil, fmt.Errorf("create temporary file: %w", err)
	}
	return file, nil
}

func Commit(file *os.File, destination string) error {
	if file == nil {
		return errors.New("commit temporary file: file is nil")
	}
	if destination == "" {
		return errors.New("commit temporary file: destination is empty")
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync temporary file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temporary file: %w", err)
	}

	if _, err := os.Stat(destination); err == nil {
		return fmt.Errorf("destination %q already exists", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat destination %q: %w", destination, err)
	}

	if err := os.Rename(file.Name(), destination); err == nil {
		return nil
	}

	destinationDir := filepath.Dir(destination)
	staging, err := os.CreateTemp(destinationDir, commitPattern)
	if err != nil {
		return fmt.Errorf("create destination staging file: %w", err)
	}
	stagingName := staging.Name()
	committed := false
	defer func() {
		staging.Close()
		if !committed {
			os.Remove(stagingName)
		}
	}()

	source, err := os.Open(file.Name())
	if err != nil {
		return fmt.Errorf("open temporary file for move: %w", err)
	}
	_, copyErr := io.Copy(staging, source)
	closeSourceErr := source.Close()
	if copyErr != nil {
		return fmt.Errorf("copy temporary file to destination: %w", copyErr)
	}
	if closeSourceErr != nil {
		return fmt.Errorf("close temporary file after copy: %w", closeSourceErr)
	}
	if err := staging.Sync(); err != nil {
		return fmt.Errorf("sync destination staging file: %w", err)
	}
	if err := staging.Close(); err != nil {
		return fmt.Errorf("close destination staging file: %w", err)
	}
	if err := os.Rename(stagingName, destination); err != nil {
		return fmt.Errorf("move staging file to %q: %w", destination, err)
	}
	committed = true
	if err := os.Remove(file.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove source temporary file: %w", err)
	}
	return nil
}

func RemoveStale(
	directory string,
	pattern string,
	maxAge time.Duration,
	isActive func(string) bool,
) (int, error) {
	if maxAge <= 0 {
		return 0, errors.New("temporary file max age must be greater than zero")
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return 0, fmt.Errorf("read temporary directory %q: %w", directory, err)
	}

	cutoff := time.Now().Add(-maxAge)
	removed := 0
	var removeErrors []error
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		matched, err := filepath.Match(pattern, entry.Name())
		if err != nil {
			return removed, fmt.Errorf("match temporary file pattern %q: %w", pattern, err)
		}
		if !matched {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		if isActive != nil && isActive(path) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			removeErrors = append(removeErrors, fmt.Errorf("stat temporary file %q: %w", path, err))
			continue
		}
		if info.ModTime().After(cutoff) {
			continue
		}
		if err := os.Remove(path); err != nil {
			removeErrors = append(removeErrors, fmt.Errorf("remove temporary file %q: %w", path, err))
			continue
		}
		removed++
	}
	return removed, errors.Join(removeErrors...)
}
