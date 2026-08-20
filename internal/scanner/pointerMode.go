package scanner

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"s3-dedup/internal/cache"
	"s3-dedup/internal/pointer"
	"s3-dedup/internal/report"

	"github.com/minio/minio-go/v6"
)

const (
	discoveryBatchSize = 1000
	candidatePageSize  = 1000
)

func (s *Scanner) scanPointer(
	ctx context.Context,
	scanReport *report.Report,
	atomics *atomicReportPart,
	workers int64,
	scanID string,
) error {
	discoveryStarted := time.Now()
	if err := s.pointerDiscovery(ctx, atomics, workers, scanID); err != nil {
		return err
	}
	s.logging.Infof("Pointer discovery completed in %s\n", time.Since(discoveryStarted))

	deduplicationStarted := time.Now()
	if err := s.pointerDeduplication(ctx, atomics, workers, scanID); err != nil {
		return err
	}
	s.logging.Infof("Pointer deduplication completed in %s\n", time.Since(deduplicationStarted))

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

func (s *Scanner) pointerDiscovery(
	ctx context.Context,
	atomics *atomicReportPart,
	workers int64,
	scanID string,
) error {
	discoveryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	mutations := make(chan cache.DiscoveryMutation, discoveryBatchSize*2)
	writerErr := make(chan error, 1)
	go s.runDiscoveryWriter(discoveryCtx, cancel, mutations, writerErr)

	jobs := make(chan objectJob, workers)
	var workersWG sync.WaitGroup
	for i := 0; i < int(workers); i++ {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			for job := range jobs {
				record, err := s.discoverObject(discoveryCtx, job.buket, job.info, job.scanID)
				if err != nil {
					atomics.processErrors.Add(1)
					s.logging.Errorf("Discovering object %s/%s: %v\n", job.buket, job.info.Key, err)
					s.sendDiscoveryMutation(discoveryCtx, mutations, cache.DiscoveryMutation{
						Kind: cache.DiscoveryMarkSeen,
						ID: cache.ObjectID{
							Bucket: job.buket,
							Key:    job.info.Key,
							ScanID: job.scanID,
						},
					})
					continue
				}
				if !s.sendDiscoveryMutation(discoveryCtx, mutations, cache.DiscoveryMutation{
					Kind:   cache.DiscoveryRegister,
					Object: record,
				}) {
					return
				}
			}
		}()
	}

	var listErr error
	for _, bucket := range s.config.S3.Buckets {
		listErr = s.s3Client.ListObjects(discoveryCtx, bucket.Name, bucket.Prefix, true, func(info minio.ObjectInfo) error {
			if strings.HasPrefix(info.Key, s.config.Dedup.BlobPrefix) {
				return nil
			}
			atomics.objectsScanned.Add(1)

			if info.Size < s.config.Dedup.MinSizeBytes && info.ContentType != pointer.ContentPointerType {
				if !s.sendDiscoveryMutation(discoveryCtx, mutations, cache.DiscoveryMutation{
					Kind: cache.DiscoveryUnregister,
					ID: cache.ObjectID{
						Bucket: bucket.Name,
						Key:    info.Key,
					},
				}) {
					return discoveryCtx.Err()
				}
				return nil
			}

			status, err := s.store.GetObjectStatus(
				discoveryCtx,
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
			if status.Unchanged && stateRank(status.State) >= stateRank(cache.ObjectStateReported) {
				if !s.sendDiscoveryMutation(discoveryCtx, mutations, cache.DiscoveryMutation{
					Kind: cache.DiscoveryMarkSeen,
					ID: cache.ObjectID{
						Bucket: bucket.Name,
						Key:    info.Key,
						ScanID: scanID,
					},
				}) {
					return discoveryCtx.Err()
				}
				return nil
			}

			select {
			case jobs <- objectJob{buket: bucket.Name, info: info, scanID: scanID}:
				return nil
			case <-discoveryCtx.Done():
				return discoveryCtx.Err()
			}
		})
		if listErr != nil {
			break
		}
	}

	close(jobs)
	workersWG.Wait()
	close(mutations)
	batchErr := <-writerErr
	s.clearBlobLocks()

	if batchErr != nil {
		return batchErr
	}
	if listErr != nil {
		return fmt.Errorf("listing objects: %w", listErr)
	}
	for _, bucket := range s.config.S3.Buckets {
		if _, err := s.store.FinalizeScope(ctx, bucket.Name, bucket.Prefix, scanID); err != nil {
			return fmt.Errorf("FinalizeScope for %q/%q: %w", bucket.Name, bucket.Prefix, err)
		}
	}
	return nil
}

func (s *Scanner) runDiscoveryWriter(
	ctx context.Context,
	cancel context.CancelFunc,
	mutations <-chan cache.DiscoveryMutation,
	result chan<- error,
) {
	batch := make([]cache.DiscoveryMutation, 0, discoveryBatchSize)
	var batchErr error
	flush := func() {
		if len(batch) == 0 || batchErr != nil {
			return
		}
		if err := s.store.ApplyDiscoveryBatch(ctx, batch); err != nil {
			batchErr = fmt.Errorf("apply discovery batch: %w", err)
			cancel()
		}
		batch = batch[:0]
	}

	for mutation := range mutations {
		if batchErr != nil {
			continue
		}
		batch = append(batch, mutation)
		if len(batch) == cap(batch) {
			flush()
		}
	}
	flush()
	result <- batchErr
}

func (s *Scanner) sendDiscoveryMutation(
	ctx context.Context,
	mutations chan<- cache.DiscoveryMutation,
	mutation cache.DiscoveryMutation,
) bool {
	select {
	case mutations <- mutation:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Scanner) pointerDeduplication(
	ctx context.Context,
	atomics *atomicReportPart,
	workers int64,
	scanID string,
) error {
	jobs := make(chan objectJob, workers)
	var candidatesQueued int64
	var workersWG sync.WaitGroup
	for i := 0; i < int(workers); i++ {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			for job := range jobs {
				reclaimed, relinked, processErr := s.processObjectPointer(ctx, job.buket, job.info, job.scanID)
				if processErr != nil {
					atomics.processErrors.Add(1)
					s.logging.Errorf("Processing duplicate %s/%s: %v\n", job.buket, job.info.Key, processErr)
					continue
				}
				if s.config.Dedup.DeleteOriginals {
					if relinked {
						atomics.objectsRelinked.Add(1)
					}
					atomics.bytesReclaimed.Add(reclaimed)
				}
			}
		}()
	}

	for _, bucket := range s.config.S3.Buckets {
		afterKey := ""
		for {
			candidates, err := s.store.ListDedupCandidates(
				ctx,
				bucket.Name,
				bucket.Prefix,
				desiredPointerState(s.config.Dedup.DeleteOriginals),
				afterKey,
				candidatePageSize,
			)
			if err != nil {
				close(jobs)
				workersWG.Wait()
				return err
			}
			if len(candidates) == 0 {
				break
			}
			for _, candidate := range candidates {
				info := minio.ObjectInfo{
					Key:          candidate.Key,
					ETag:         candidate.ETag,
					Size:         candidate.Size,
					LastModified: candidate.LastModified,
				}
				select {
				case jobs <- objectJob{buket: candidate.Bucket, info: info, scanID: scanID}:
					candidatesQueued++
				case <-ctx.Done():
					close(jobs)
					workersWG.Wait()
					return ctx.Err()
				}
			}
			afterKey = candidates[len(candidates)-1].Key
		}
	}

	close(jobs)
	workersWG.Wait()
	s.clearBlobLocks()
	s.logging.Infof("Pointer deduplication candidates: %d\n", candidatesQueued)
	return nil
}

func (s *Scanner) clearBlobLocks() {
	s.blobLocks.Range(func(key, _ any) bool {
		s.blobLocks.Delete(key)
		return true
	})
}
