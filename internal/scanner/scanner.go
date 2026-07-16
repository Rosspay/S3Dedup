package scanner

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"s3-dedup/internal/cache"
	"s3-dedup/internal/config"
	"s3-dedup/internal/hashing"
	"s3-dedup/internal/report"
	"sync"
	"sync/atomic"

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

type objectJob struct {
	buket  string
	info   minio.ObjectInfo
	scanID string
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
	scanID := strconv.FormatInt(scanReport.ScanStarted.UnixNano(), 10)
	workers := s.config.Schedule.Workers
	if workers <= 0 {
		workers = 1
	}
	if workers > int64(runtime.NumCPU()) {
		workers = int64(runtime.NumCPU())
	}

	var objectsScanned atomic.Int64
	var processErrors atomic.Int64

	for _, bucket := range s.config.S3.Buckets {
		jobs := make(chan objectJob, workers)
		var wg sync.WaitGroup
		for i := 0; i < int(workers); i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for job := range jobs {
					processErr := s.processObject(ctx, job.buket, job.info, job.scanID)
					if processErr != nil {
						processErrors.Add(1)
						fmt.Printf("Processing object %s/%s: %v\n", job.buket, job.info.Key, processErr)
					}
				}
			}()
		}
		err := s.s3Client.ListObjects(ctx, bucket.Name, bucket.Prefix, true,
			func(info minio.ObjectInfo) error {
				objectsScanned.Add(1)
				err := s.store.MarkObjectSeen(ctx, bucket.Name, info.Key, scanID)
				if err != nil {
					processErrors.Add(1)
					return fmt.Errorf("MarkObjectSeen error for %q: %w\n", info.Key, err)
				}
				if info.Size < s.config.Dedup.MinSizeBytes {
					return nil
				}

				select {
				case jobs <- objectJob{
					buket:  bucket.Name,
					info:   info,
					scanID: scanID,
				}:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		if err != nil {
			scanReport.Errors++
			return scanReport, err
		}
		close(jobs)
		wg.Wait()

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
	scanReport.ObjectsScanned += objectsScanned.Load()
	scanReport.Errors += processErrors.Load()
	scanReport.UniqueBlobs = stats.UniqueBlobs
	scanReport.DuplicatesFound = stats.DuplicatesFound
	scanReport.BytesReclaimable = stats.BytesReclaimable

	return scanReport, nil
}

// processObject streams a single object's content and returns error if occured.
// It is a standalone function so that the deferred Close runs after every object
// (not at the end of the whole scan), keeping open connections bounded.
func (s *Scanner) processObject(ctx context.Context, bucket string,
	info minio.ObjectInfo, scanID string) error {
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
