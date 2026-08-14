package scanner

import (
	"context"
	"fmt"
	"s3-dedup/internal/hashing"
	"s3-dedup/internal/pointer"
	"s3-dedup/internal/report"
	"strings"
	"sync"

	"github.com/minio/minio-go/v6"
)

type reportOnlyJob struct {
	bucket string
	info   minio.ObjectInfo
}

type reportOnlyItem struct {
	bucket      string
	key         string
	hash        string
	size        int64
	isPointer   bool
	shouldCount bool
	err         error
}

type reportOnlyGroup struct {
	size         int64
	objects      int64
	originals    int64
	materialized bool
}

type reportOnlySummary struct {
	groups map[string]*reportOnlyGroup
}

func (s *Scanner) scanReportOnly(
	ctx context.Context,
	scanReport *report.Report,
	atomics *atomicReportPart,
	workers int64,
) error {
	jobs := make(chan reportOnlyJob, workers)
	results := make(chan reportOnlyItem, workers)

	var workersWG sync.WaitGroup
	for i := 0; i < int(workers); i++ {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			for job := range jobs {
				item := s.inspectReportObject(ctx, job.bucket, job.info)
				select {
				case results <- item:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	summaryReady := make(chan reportOnlySummary, 1)
	go func() {
		groups := make(map[string]*reportOnlyGroup)
		for item := range results {
			if item.err != nil {
				atomics.processErrors.Add(1)
				s.logging.Errorf("Processing object %s/%s: %v\n", item.bucket, item.key, item.err)
				continue
			}
			if !item.shouldCount {
				atomics.objectsScanned.Add(-1)
				continue
			}

			group, exists := groups[item.hash]
			if !exists {
				group = &reportOnlyGroup{size: item.size}
				groups[item.hash] = group
			}
			if group.size != item.size {
				atomics.processErrors.Add(1)
				s.logging.Errorf(
					"Content hash %q has different sizes: %d and %d\n",
					item.hash,
					group.size,
					item.size,
				)
				continue
			}

			group.objects++
			if item.isPointer {
				group.materialized = true
			} else {
				group.originals++
			}
		}
		summaryReady <- reportOnlySummary{groups: groups}
	}()

	var listErr error
	for _, bucket := range s.config.S3.Buckets {
		listErr = s.s3Client.ListObjects(ctx, bucket.Name, bucket.Prefix, true, func(info minio.ObjectInfo) error {
			if strings.HasPrefix(info.Key, s.config.Dedup.BlobPrefix) {
				return nil
			}
			atomics.objectsScanned.Add(1)
			select {
			case jobs <- reportOnlyJob{bucket: bucket.Name, info: info}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if listErr != nil {
			break
		}
	}

	close(jobs)
	workersWG.Wait()
	close(results)
	summary := <-summaryReady

	if listErr != nil {
		return fmt.Errorf("listing objects: %w", listErr)
	}

	s.buildReportOnlyStats(ctx, scanReport, atomics, summary.groups)
	return nil
}

func (s *Scanner) inspectReportObject(
	ctx context.Context,
	bucket string,
	info minio.ObjectInfo,
) reportOnlyItem {
	item := reportOnlyItem{bucket: bucket, key: info.Key}

	before, err := s.s3Client.StatObject(ctx, bucket, info.Key)
	if err != nil {
		item.err = err
		return item
	}
	if before.Size < s.config.Dedup.MinSizeBytes && before.ContentType != pointer.ContentPointerType {
		return item
	}

	if before.ContentType == pointer.ContentPointerType {
		item.hash, item.size, item.err = s.inspectPointerForReport(ctx, bucket, before)
		item.isPointer = true
		item.shouldCount = item.err == nil
		return item
	}

	object, err := s.s3Client.GetObject(ctx, bucket, info.Key)
	if err != nil {
		item.err = err
		return item
	}
	hash, hashErr := hashing.HashReader(object, s.config.Dedup.HashAlgo)
	closeErr := object.Close()
	if hashErr != nil {
		item.err = hashErr
		return item
	}
	if closeErr != nil {
		item.err = closeErr
		return item
	}

	after, err := s.s3Client.StatObject(ctx, bucket, info.Key)
	if err != nil {
		item.err = err
		return item
	}
	if isObjectChanged(before, after) {
		item.err = fmt.Errorf("object changed while report was reading it")
		return item
	}

	item.hash = hash
	item.size = before.Size
	item.shouldCount = true
	return item
}

func (s *Scanner) inspectPointerForReport(
	ctx context.Context,
	bucket string,
	logicalInfo minio.ObjectInfo,
) (string, int64, error) {
	object, err := s.s3Client.GetObject(ctx, bucket, logicalInfo.Key)
	if err != nil {
		return "", 0, err
	}
	p, readErr := pointer.ReadPointer(object)
	closeErr := object.Close()
	if readErr != nil {
		return "", 0, readErr
	}
	if closeErr != nil {
		return "", 0, closeErr
	}
	if p.BlobKey != s.config.Dedup.BlobPrefix+p.Hash {
		return "", 0, fmt.Errorf("pointer blob key %q does not match hash", p.BlobKey)
	}

	logicalAfter, err := s.s3Client.StatObject(ctx, bucket, logicalInfo.Key)
	if err != nil {
		return "", 0, err
	}
	if isObjectChanged(logicalInfo, logicalAfter) {
		return "", 0, fmt.Errorf("pointer changed while report was reading it")
	}

	blobBefore, err := s.s3Client.StatObject(ctx, p.BlobBucket, p.BlobKey)
	if err != nil {
		return "", 0, err
	}
	if !comparePointerObject(p, blobBefore) {
		return "", 0, fmt.Errorf("pointer-object mismatch")
	}
	if p.HashAlgo == s.config.Dedup.HashAlgo {
		return p.Hash, p.Size, nil
	}

	blob, err := s.s3Client.GetObject(ctx, p.BlobBucket, p.BlobKey)
	if err != nil {
		return "", 0, err
	}
	hash, hashErr := hashing.HashReader(blob, s.config.Dedup.HashAlgo)
	closeErr = blob.Close()
	if hashErr != nil {
		return "", 0, hashErr
	}
	if closeErr != nil {
		return "", 0, closeErr
	}

	blobAfter, err := s.s3Client.StatObject(ctx, p.BlobBucket, p.BlobKey)
	if err != nil {
		return "", 0, err
	}
	if isObjectChanged(blobBefore, blobAfter) {
		return "", 0, fmt.Errorf("pointer blob changed while report was reading it")
	}
	return hash, p.Size, nil
}

func (s *Scanner) buildReportOnlyStats(
	ctx context.Context,
	scanReport *report.Report,
	atomics *atomicReportPart,
	groups map[string]*reportOnlyGroup,
) {
	for hash, group := range groups {
		scanReport.UniqueBlobs++
		if group.objects > 1 {
			scanReport.DuplicatesFound += group.objects - 1
		}
		if group.objects <= 1 || group.originals == 0 {
			continue
		}

		materialized := group.materialized
		if !materialized {
			blobKey := s.config.Dedup.BlobPrefix + hash
			blobInfo, err := s.s3Client.StatObject(ctx, s.config.Dedup.BlobBucket, blobKey)
			switch {
			case err == nil:
				if blobInfo.Size != group.size {
					atomics.processErrors.Add(1)
					s.logging.Errorf("Blob %s/%s has size %d, expected %d\n", s.config.Dedup.BlobBucket, blobKey, blobInfo.Size, group.size)
					continue
				}
				materialized = true
			case minio.ToErrorResponse(err).Code == "NoSuchKey":
			default:
				atomics.processErrors.Add(1)
				s.logging.Errorf("StatObject for blob %s/%s: %v\n", s.config.Dedup.BlobBucket, blobKey, err)
				continue
			}
		}

		if materialized {
			scanReport.BytesReclaimable += group.originals * group.size
		} else {
			scanReport.BytesReclaimable += (group.originals - 1) * group.size
		}
	}
}
