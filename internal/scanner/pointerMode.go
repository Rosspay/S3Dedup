package scanner

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"s3-dedup/internal/cache"
	"s3-dedup/internal/pointer"
	"s3-dedup/internal/report"

	"github.com/minio/minio-go/v6"
)

type pointerPhase uint8

const (
	pointerDiscovery pointerPhase = iota
	pointerDeduplication
)

func (s *Scanner) scanPointer(
	ctx context.Context,
	scanReport *report.Report,
	atomics *atomicReportPart,
	workers int64,
	scanID string,
) error {
	if err := s.pointerLap(ctx, atomics, workers, scanID, pointerDiscovery); err != nil {
		return err
	}
	if err := s.pointerLap(ctx, atomics, workers, scanID, pointerDeduplication); err != nil {
		return err
	}

	if atomics.processErrors.Load() == 0 && scanReport.Errors == 0 {
		gcBytes, removedBlobs, err := s.collectGarbage(ctx)
		if err != nil {
			return fmt.Errorf("garbage collection: %w", err)
		}
		atomics.bytesReclaimed.Add(gcBytes)
		s.logging.Infof("Blobs removed: %d, bytes reclaimed: %d\n", removedBlobs, gcBytes)
	}

	stats, err := s.store.GetStats(ctx)
	if err != nil {
		return fmt.Errorf("GetStats error: %w", err)
	}
	scanReport.UniqueBlobs = stats.UniqueBlobs
	scanReport.DuplicatesFound = stats.DuplicatesFound
	scanReport.BytesReclaimable = stats.BytesReclaimable
	return nil
}

func (s *Scanner) pointerLap(
	ctx context.Context,
	atomics *atomicReportPart,
	workers int64,
	scanID string,
	phase pointerPhase,
) error {
	jobs := make(chan objectJob, workers)
	var workersWG sync.WaitGroup
	for i := 0; i < int(workers); i++ {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			for job := range jobs {
				var processErr error
				switch phase {
				case pointerDiscovery:
					processErr = s.processObject(ctx, job.buket, job.info, job.scanID)
				case pointerDeduplication:
					var reclaimed int64
					var relinked bool
					reclaimed, relinked, processErr = s.processObjectPointer(ctx, job.buket, job.info, job.scanID)
					if s.config.Dedup.DeleteOriginals && processErr == nil {
						if relinked {
							atomics.objectsRelinked.Add(1)
						}
						atomics.bytesReclaimed.Add(reclaimed)
					}
				}
				if processErr != nil {
					atomics.processErrors.Add(1)
					s.logging.Errorf("Processing object %s/%s: %v\n", job.buket, job.info.Key, processErr)
				}
			}
		}()
	}

	for _, bucket := range s.config.S3.Buckets {
		err := s.s3Client.ListObjects(ctx, bucket.Name, bucket.Prefix, true, func(info minio.ObjectInfo) error {
			if strings.HasPrefix(info.Key, s.config.Dedup.BlobPrefix) {
				return nil
			}

			if phase == pointerDiscovery {
				if err := s.store.MarkObjectSeen(ctx, bucket.Name, info.Key, scanID); err != nil {
					return fmt.Errorf("MarkObjectSeen error for %q: %w", info.Key, err)
				}
				if info.Size < s.config.Dedup.MinSizeBytes && info.ContentType != pointer.ContentPointerType {
					return s.store.UnregisterObject(ctx, bucket.Name, info.Key)
				}
				atomics.objectsScanned.Add(1)
			}

			status, err := s.store.GetObjectStatus(
				ctx,
				bucket.Name,
				info.Key,
				info.ETag,
				info.Size,
				s.config.Dedup.HashAlgo,
				info.LastModified,
			)
			if err != nil {
				return fmt.Errorf("GetObjectStatus %q/%q: %w", bucket.Name, info.Key, err)
			}

			switch phase {
			case pointerDiscovery:
				if status.Unchanged && stateRank(status.State) >= stateRank(cache.ObjectStateReported) {
					s.logging.Debugf("Object %s/%s skipped because already discovered\n", bucket.Name, info.Key)
					return nil
				}
			case pointerDeduplication:
				if !status.Unchanged {
					s.logging.Debugf("Object %s/%s skipped because it changed after discovery\n", bucket.Name, info.Key)
					return nil
				}
				if status.RefCount <= 1 {
					s.logging.Debugf("Object %s/%s skipped because ref_count = %d\n", bucket.Name, info.Key, status.RefCount)
					return nil
				}
				if stateRank(status.State) >= stateRank(desiredPointerState(s.config.Dedup.DeleteOriginals)) {
					s.logging.Debugf("Object %s/%s skipped because unchanged and ready\n", bucket.Name, info.Key)
					return nil
				}
			}

			select {
			case jobs <- objectJob{buket: bucket.Name, info: info, scanID: scanID}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if err != nil {
			close(jobs)
			workersWG.Wait()
			return fmt.Errorf("listing objects in %q: %w", bucket.Name, err)
		}
	}

	close(jobs)
	workersWG.Wait()
	s.clearBlobLocks()

	if phase == pointerDiscovery {
		for _, bucket := range s.config.S3.Buckets {
			if _, err := s.store.FinalizeScope(ctx, bucket.Name, bucket.Prefix, scanID); err != nil {
				return fmt.Errorf("FinalizeScope for %q/%q: %w", bucket.Name, bucket.Prefix, err)
			}
		}
	}
	return nil
}

func (s *Scanner) clearBlobLocks() {
	s.blobLocks.Range(func(key, _ any) bool {
		s.blobLocks.Delete(key)
		return true
	})
}
