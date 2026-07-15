package scanner

import (
	"context"
	"fmt"
	"io"
	"s3-dedup/internal/cache"
	"s3-dedup/internal/config"
	"s3-dedup/internal/hashing"
	"s3-dedup/internal/report"

	//"s3-dedup/internal/s3"
	"strconv"
	"time"

	"github.com/minio/minio-go/v6"
)

type S3Client interface {
	ListObjects(
		ctx context.Context,
		bucket string,
		prefix string,
		recursive bool,
		fn func(minio.ObjectInfo) error,
	) error

	GetObject(
		ctx context.Context,
		bucket string,
		key string,
	) (io.ReadCloser, error)
}

type Scanner struct {
	s3Client S3Client
	store    cache.Store
	config   *config.Config
}

func NewScanner(s3Client S3Client, store cache.Store, config *config.Config) *Scanner {
	return &Scanner{
		s3Client: s3Client,
		store:    store,
		config:   config,
	}
}

func (s *Scanner) ScanOnce(ctx context.Context) (report.Report, error) {
	var scanReport report.Report
	scanReport.ScanStarted = time.Now().UTC()
	scanReport.Mode = s.config.Dedup.Mode
	scanID := strconv.FormatInt(scanReport.ScanStarted.Unix(), 10)

	for _, bucket := range s.config.S3.Buckets {
		err := s.s3Client.ListObjects(ctx, bucket.Name, bucket.Prefix, true,
			func(info minio.ObjectInfo) error {
				processError := s.processObject(ctx, bucket.Name, info, scanID)
				if processError != nil {
					scanReport.Errors++
					fmt.Printf("Processing object %s/%s: %v", bucket.Name, info.Key, processError)
					return nil
				}
				scanReport.ObjectsScanned++
				return nil
			})
		if err != nil {
			scanReport.Errors++
			return scanReport, fmt.Errorf("Error listing objects in %q: %w", bucket.Name, err)
		}
		_, err = s.store.FinalizeScope(ctx, bucket.Name, bucket.Prefix, scanID)
		if err != nil {
			scanReport.Errors++
			return scanReport, fmt.Errorf("FinalizeScope for %q/%q: %w", bucket.Name, bucket.Prefix, err)
		}
	}
	stats, err := s.store.GetStats(ctx)
	if err != nil {
		scanReport.Errors++
		return scanReport, fmt.Errorf("GetStats error: %w", err)
	}

	scanReport.UniqueBlobs = stats.UniqueBlobs
	scanReport.DuplicatesFound = stats.DuplicatesFound
	scanReport.BytesReclaimable = stats.BytesReclaimable
	scanReport.ScanFinished = time.Now().UTC()

	return scanReport, nil
}

// processObject streams a single object's content and returns error if occured.
// It is a standalone function so that the deferred Close runs after every object
// (not at the end of the whole scan), keeping open connections bounded.
func (s *Scanner) processObject(ctx context.Context, bucket string,
	info minio.ObjectInfo, scanID string) error {
	err := s.store.MarkObjectSeen(ctx, bucket, info.Key, scanID)
	if err != nil {
		return fmt.Errorf("MarkObjectSeen error for %q: %w\n", info.Key, err)
	}

	if info.Size < s.config.Dedup.MinSizeBytes {
		return nil
	}

	obj, err := s.s3Client.GetObject(ctx, bucket, info.Key)
	if err != nil {
		return err
	}
	defer obj.Close()

	hash, err := hashing.HashReader(obj, s.config.Dedup.HashAlgo)
	if err != nil {
		return err
	}

	record := cache.ObjectRecord{
		Bucket:       bucket,
		Key:          info.Key,
		ETag:         info.ETag,
		Size:         info.Size,
		LastModified: info.LastModified,
		Hash:         hash,
		LastSeenScan: scanID,
	}

	err = s.store.RegisterObject(ctx, record)
	if err != nil {
		return err
	}

	return nil
}
