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
	discoveryBatchSize      = 1000
	discoveryLookupPageSize = 1000
	dedupBatchSize          = 100
	dedupBatchFlushInterval = 100 * time.Millisecond
	candidatePageSize       = 1000
)

type pointerDedupJob struct {
	candidate cache.DedupCandidate
	scanID    string
	completed *sync.WaitGroup
}

type pointerDedupResult struct {
	record    *cache.ObjectRecord
	reclaimed int64
	relinked  bool
	completed *sync.WaitGroup
}

type dedupBlobCoordinator struct {
	locks sync.Map
	ready sync.Map
}

func dedupBlobID(bucket, key string) string {
	return bucket + "\x00" + key
}

func (c *dedupBlobCoordinator) mutex(bucket, key string) *sync.Mutex {
	mu, _ := c.locks.LoadOrStore(dedupBlobID(bucket, key), &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func (c *dedupBlobCoordinator) readySize(bucket, key string) (int64, bool) {
	size, ok := c.ready.Load(dedupBlobID(bucket, key))
	if !ok {
		return 0, false
	}
	return size.(int64), true
}

func (c *dedupBlobCoordinator) markReady(bucket, key string, size int64) {
	c.ready.Store(dedupBlobID(bucket, key), size)
}

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

	scopes := make([]cache.Scope, 0, len(s.config.S3.Buckets))
	for _, bucket := range s.config.S3.Buckets {
		scopes = append(scopes, cache.Scope{Bucket: bucket.Name, Prefix: bucket.Prefix})
	}
	stats, err := s.store.GetStats(ctx, scopes...)
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
		page := make([]minio.ObjectInfo, 0, discoveryLookupPageSize)
		flushPage := func() error {
			if len(page) == 0 {
				return nil
			}
			err := s.processDiscoveryPage(
				discoveryCtx,
				bucket.Name,
				page,
				jobs,
				mutations,
				scanID,
			)
			page = page[:0]
			return err
		}

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

			page = append(page, info)
			if len(page) == cap(page) {
				return flushPage()
			}
			return nil
		})
		if listErr == nil {
			listErr = flushPage()
		}
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

func (s *Scanner) processDiscoveryPage(
	ctx context.Context,
	bucket string,
	objects []minio.ObjectInfo,
	jobs chan<- objectJob,
	mutations chan<- cache.DiscoveryMutation,
	scanID string,
) error {
	metadata := make([]cache.ObjectMetadata, len(objects))
	for index, object := range objects {
		metadata[index] = cache.ObjectMetadata{
			Key:          object.Key,
			ETag:         object.ETag,
			Size:         object.Size,
			LastModified: object.LastModified,
		}
	}
	statuses, err := s.store.GetObjectStatuses(
		ctx,
		bucket,
		metadata,
		s.config.Dedup.HashAlgo,
	)
	if err != nil {
		return fmt.Errorf("GetObjectStatuses %q: %w", bucket, err)
	}
	if len(statuses) != len(objects) {
		return fmt.Errorf(
			"GetObjectStatuses %q returned %d statuses for %d objects",
			bucket,
			len(statuses),
			len(objects),
		)
	}

	for index, object := range objects {
		status := statuses[index]
		if status.Unchanged &&
			stateRank(status.State) >= stateRank(cache.ObjectStateReported) {
			if !s.sendDiscoveryMutation(ctx, mutations, cache.DiscoveryMutation{
				Kind: cache.DiscoveryMarkSeen,
				ID: cache.ObjectID{
					Bucket: bucket,
					Key:    object.Key,
					ScanID: scanID,
				},
			}) {
				return ctx.Err()
			}
			continue
		}

		select {
		case jobs <- objectJob{buket: bucket, info: object, scanID: scanID}:
		case <-ctx.Done():
			return ctx.Err()
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
	jobs := make(chan pointerDedupJob, workers)
	results := make(chan pointerDedupResult, dedupBatchSize*2)
	writerDone := make(chan struct{})
	go s.runDedupWriter(ctx, results, atomics, writerDone)

	var candidatesQueued int64
	blobs := &dedupBlobCoordinator{}
	var workersWG sync.WaitGroup
	for i := 0; i < int(workers); i++ {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			for job := range jobs {
				result, processErr := s.processObjectPointer(ctx, job.candidate, job.scanID, blobs)
				if processErr != nil {
					atomics.processErrors.Add(1)
					s.logging.Errorf(
						"Processing duplicate %s/%s: %v\n",
						job.candidate.Bucket,
						job.candidate.Key,
						processErr,
					)
					job.completed.Done()
					continue
				}
				result.completed = job.completed
				select {
				case results <- result:
				case <-ctx.Done():
					job.completed.Done()
					return
				}
			}
		}()
	}

	shutdown := func() {
		close(jobs)
		workersWG.Wait()
		close(results)
		<-writerDone
		s.clearBlobLocks()
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
				shutdown()
				return err
			}
			if len(candidates) == 0 {
				break
			}

			var pageWG sync.WaitGroup
			for _, candidate := range candidates {
				pageWG.Add(1)
				select {
				case jobs <- pointerDedupJob{
					candidate: candidate,
					scanID:    scanID,
					completed: &pageWG,
				}:
					candidatesQueued++
				case <-ctx.Done():
					pageWG.Done()
					shutdown()
					return ctx.Err()
				}
			}
			pageWG.Wait()
			if err := ctx.Err(); err != nil {
				shutdown()
				return err
			}
			afterKey = candidates[len(candidates)-1].Key
		}
	}

	shutdown()
	s.logging.Infof("Pointer deduplication candidates: %d\n", candidatesQueued)
	return nil
}

func (s *Scanner) runDedupWriter(
	ctx context.Context,
	results <-chan pointerDedupResult,
	atomics *atomicReportPart,
	done chan<- struct{},
) {
	defer close(done)
	ticker := time.NewTicker(dedupBatchFlushInterval)
	defer ticker.Stop()

	batch := make([]pointerDedupResult, 0, dedupBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}

		records := make([]cache.ObjectRecord, len(batch))
		for index, result := range batch {
			records[index] = *result.record
		}
		if err := s.store.ApplyDedupBatch(ctx, records); err != nil {
			atomics.processErrors.Add(int64(len(batch)))
			s.logging.Errorf("Applying pointer deduplication batch of %d objects: %v\n", len(batch), err)
		} else {
			for _, result := range batch {
				s.applyDedupMetrics(atomics, result)
			}
		}
		for _, result := range batch {
			result.completed.Done()
		}
		batch = batch[:0]
	}

	for {
		select {
		case result, ok := <-results:
			if !ok {
				flush()
				return
			}
			if result.record == nil {
				s.applyDedupMetrics(atomics, result)
				result.completed.Done()
				continue
			}
			batch = append(batch, result)
			if len(batch) == cap(batch) {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (s *Scanner) applyDedupMetrics(atomics *atomicReportPart, result pointerDedupResult) {
	if !s.config.Dedup.DeleteOriginals {
		return
	}
	if result.relinked {
		atomics.objectsRelinked.Add(1)
	}
	atomics.bytesReclaimed.Add(result.reclaimed)
}

func (s *Scanner) clearBlobLocks() {
	s.blobLocks.Range(func(key, _ any) bool {
		s.blobLocks.Delete(key)
		return true
	})
}
