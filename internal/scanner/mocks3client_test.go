package scanner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v6"
)

// mocking S3 client for testing purposes
// so we can simulate basic behavior and errors
type mockS3Client struct {
	mu          sync.RWMutex
	objects     []minio.ObjectInfo
	contents    map[string]string
	stats       map[string]minio.ObjectInfo
	errors      map[string]error
	statErrors  map[string]error
	putErrors   map[string]error
	putCalls    map[string]int
	statCalls   map[string]int
	statHooks   map[string]func(*mockS3Client, int)
	listErr     error
	listHook    func(processed int)
	removeErrs  map[string]map[string]error
	removeCalls map[string][][]string
}

func (m *mockS3Client) ListObjects(
	ctx context.Context,
	bucket string,
	prefix string,
	recursive bool,
	fn func(minio.ObjectInfo) error,
) error {
	m.mu.RLock()
	listErr := m.listErr
	listHook := m.listHook
	objects := append([]minio.ObjectInfo(nil), m.objects...)
	m.mu.RUnlock()

	if listErr != nil {
		return listErr
	}
	for i, object := range objects {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !strings.HasPrefix(object.Key, prefix) {
			continue
		}
		if err := fn(object); err != nil {
			return err
		}
		if listHook != nil {
			listHook(i + 1)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

func (m *mockS3Client) GetObject(
	ctx context.Context,
	bucket string,
	key string,
) (io.ReadCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	id := objectID(bucket, key)
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.errors[id]; err != nil {
		return nil, err
	}
	content, ok := m.contents[id]
	if !ok {
		return nil, fmt.Errorf("object %q/%q not found", bucket, key)
	}
	return io.NopCloser(strings.NewReader(content)), nil
}

func (m *mockS3Client) PutObject(
	ctx context.Context,
	bucket string,
	objectName string,
	reader io.Reader,
	size int64,
	contentType string,
) (int64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	id := objectID(bucket, objectName)
	m.mu.Lock()
	if m.putCalls == nil {
		m.putCalls = make(map[string]int)
	}
	m.putCalls[id]++
	putErr := m.putErrors[id]
	m.mu.Unlock()
	if putErr != nil {
		return 0, putErr
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return 0, err
	}
	if int64(len(data)) != size {
		return 0, fmt.Errorf("put object %q/%q: read %d bytes, expected %d", bucket, objectName, len(data), size)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.contents == nil {
		m.contents = make(map[string]string)
	}
	if m.stats == nil {
		m.stats = make(map[string]minio.ObjectInfo)
	}
	m.contents[id] = string(data)
	info := minio.ObjectInfo{
		Key:          objectName,
		Size:         int64(len(data)),
		ETag:         fmt.Sprintf("etag-%s-%d", objectName, m.putCalls[id]),
		LastModified: time.Now().UTC(),
		ContentType:  contentType,
	}
	m.stats[id] = info
	for i := range m.objects {
		if m.objects[i].Key == objectName {
			m.objects[i] = info
			return int64(len(data)), nil
		}
	}
	m.objects = append(m.objects, info)
	return int64(len(data)), nil
}

func (m *mockS3Client) StatObject(
	ctx context.Context,
	bucket string,
	objectName string,
) (minio.ObjectInfo, error) {
	select {
	case <-ctx.Done():
		return minio.ObjectInfo{}, ctx.Err()
	default:
	}

	id := objectID(bucket, objectName)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.statCalls == nil {
		m.statCalls = make(map[string]int)
	}
	m.statCalls[id]++
	if hook := m.statHooks[id]; hook != nil {
		hook(m, m.statCalls[id])
	}
	if err := m.statErrors[id]; err != nil {
		return minio.ObjectInfo{}, err
	}
	if info, ok := m.stats[id]; ok {
		return info, nil
	}
	if content, ok := m.contents[id]; ok {
		return minio.ObjectInfo{
			Key:          objectName,
			Size:         int64(len(content)),
			ETag:         "etag-" + objectName,
			LastModified: time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC),
		}, nil
	}
	return minio.ObjectInfo{}, minio.ErrorResponse{
		Code:       "NoSuchKey",
		Message:    "object does not exist",
		BucketName: bucket,
		Key:        objectName,
		StatusCode: 404,
	}
}

func (m *mockS3Client) RemoveObjects(
	ctx context.Context,
	bucket string,
	keys []string,
) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.removeCalls == nil {
		m.removeCalls = make(map[string][][]string)
	}
	m.removeCalls[bucket] = append(
		m.removeCalls[bucket],
		append([]string(nil), keys...),
	)

	var deleted []string
	var errs []error
	for _, key := range keys {
		if err := m.removeErrs[bucket][key]; err != nil {
			errs = append(errs, err)
			continue
		}

		deleted = append(deleted, key)
		id := objectID(bucket, key)
		delete(m.contents, id)
		delete(m.stats, id)
		for i := 0; i < len(m.objects); i++ {
			if m.objects[i].Key != key {
				continue
			}
			m.objects = append(m.objects[:i], m.objects[i+1:]...)
			i--
		}
	}

	return deleted, errors.Join(errs...)
}

func (m *mockS3Client) content(id string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.contents[id]
}

func (m *mockS3Client) putCallCount(bucket, key string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.putCalls[objectID(bucket, key)]
}

func (m *mockS3Client) totalPutCalls() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var total int
	for _, calls := range m.putCalls {
		total += calls
	}
	return total
}

func (m *mockS3Client) countObjectsWithPrefix(bucket, prefix string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	idPrefix := objectID(bucket, prefix)
	var count int
	for id := range m.contents {
		if strings.HasPrefix(id, idPrefix) {
			count++
		}
	}
	return count
}
