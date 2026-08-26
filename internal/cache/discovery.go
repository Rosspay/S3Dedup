package cache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

func (s *SQLiteStore) ApplyDiscoveryBatch(ctx context.Context, mutations []DiscoveryMutation) error {
	if len(mutations) == 0 {
		return nil
	}
	for _, mutation := range mutations {
		if mutation.Kind == DiscoveryRegister {
			if err := validateObject(mutation.Object); err != nil {
				return err
			}
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("apply discovery batch: begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, mutation := range mutations {
		switch mutation.Kind {
		case DiscoveryRegister:
			if err := applyRegisterMutation(ctx, tx, mutation.Object); err != nil {
				return err
			}
		case DiscoveryMarkSeen:
			if err := applyMarkSeenMutation(ctx, tx, mutation.ID); err != nil {
				return err
			}
		case DiscoveryUnregister:
			if err := applyUnregisterMutation(ctx, tx, mutation.ID); err != nil {
				return err
			}
		default:
			return fmt.Errorf("apply discovery batch: unsupported mutation kind %d", mutation.Kind)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("apply discovery batch: commit: %w", err)
	}
	return nil
}

func (s *SQLiteStore) ApplyDedupBatch(ctx context.Context, objects []ObjectRecord) error {
	if len(objects) == 0 {
		return nil
	}
	for _, object := range objects {
		if err := validateObject(object); err != nil {
			return err
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("apply dedup batch: begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, object := range objects {
		if err := applyRegisterMutation(ctx, tx, object); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("apply dedup batch: commit: %w", err)
	}
	return nil
}

func applyRegisterMutation(ctx context.Context, tx *sql.Tx, object ObjectRecord) error {
	var oldBlobBucket string
	var oldHash string
	err := tx.QueryRowContext(ctx,
		`SELECT blob_bucket, blob_hash FROM objects WHERE bucket = ? AND object_key = ?`,
		object.Bucket,
		object.Key,
	).Scan(&oldBlobBucket, &oldHash)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		if err := incrementBlob(ctx, tx, object.BlobBucket, object.BlobKey, object.Hash, object.BlobSize); err != nil {
			return fmt.Errorf("register object %q/%q: %w", object.Bucket, object.Key, err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO objects (
				bucket, object_key, etag, size, last_modified, blob_bucket, blob_hash, hash_algo, last_seen_scan, object_state
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`,
			object.Bucket,
			object.Key,
			object.ETag,
			object.Size,
			formatObjectTime(object.LastModified),
			object.BlobBucket,
			object.Hash,
			object.HashAlgo,
			object.LastSeenScan,
			object.State,
		); err != nil {
			return fmt.Errorf("register object %q/%q: insert object: %w", object.Bucket, object.Key, err)
		}
	case err != nil:
		return fmt.Errorf("register object %q/%q: read current state: %w", object.Bucket, object.Key, err)
	case oldBlobBucket == object.BlobBucket && oldHash == object.Hash:
		if _, err := tx.ExecContext(ctx, `
			UPDATE objects
			SET etag = ?, size = ?, last_modified = ?, last_seen_scan = ?, object_state = ?, hash_algo = ?
			WHERE bucket = ? AND object_key = ?
		`,
			object.ETag,
			object.Size,
			formatObjectTime(object.LastModified),
			object.LastSeenScan,
			object.State,
			object.HashAlgo,
			object.Bucket,
			object.Key,
		); err != nil {
			return fmt.Errorf("register object %q/%q: update metadata: %w", object.Bucket, object.Key, err)
		}
	default:
		if err := incrementBlob(ctx, tx, object.BlobBucket, object.BlobKey, object.Hash, object.BlobSize); err != nil {
			return fmt.Errorf("register object %q/%q: %w", object.Bucket, object.Key, err)
		}
		if err := decrementBlob(ctx, tx, oldBlobBucket, oldHash); err != nil {
			return fmt.Errorf("register object %q/%q: %w", object.Bucket, object.Key, err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE objects
			SET etag = ?, size = ?, last_modified = ?, blob_bucket = ?, blob_hash = ?, last_seen_scan = ?, object_state = ?, hash_algo = ?
			WHERE bucket = ? AND object_key = ?
		`,
			object.ETag,
			object.Size,
			formatObjectTime(object.LastModified),
			object.BlobBucket,
			object.Hash,
			object.LastSeenScan,
			object.State,
			object.HashAlgo,
			object.Bucket,
			object.Key,
		); err != nil {
			return fmt.Errorf("register object %q/%q: update blob reference: %w", object.Bucket, object.Key, err)
		}
	}
	return nil
}

func applyMarkSeenMutation(ctx context.Context, tx *sql.Tx, id ObjectID) error {
	if id.Bucket == "" || id.Key == "" || id.ScanID == "" {
		return fmt.Errorf("mark object seen: bucket, key and scan ID are required")
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE objects
		SET last_seen_scan = ?
		WHERE bucket = ? AND object_key = ?
	`, id.ScanID, id.Bucket, id.Key); err != nil {
		return fmt.Errorf("mark object seen %q/%q: %w", id.Bucket, id.Key, err)
	}
	return nil
}

func applyUnregisterMutation(ctx context.Context, tx *sql.Tx, id ObjectID) error {
	if id.Bucket == "" || id.Key == "" {
		return fmt.Errorf("unregister object: bucket and key are required")
	}

	var blobBucket string
	var hash string
	err := tx.QueryRowContext(ctx, `
		SELECT blob_bucket, blob_hash
		FROM objects
		WHERE bucket = ? AND object_key = ?
	`, id.Bucket, id.Key).Scan(&blobBucket, &hash)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("unregister object %q/%q: read object: %w", id.Bucket, id.Key, err)
	}

	if _, err := tx.ExecContext(ctx, `
		DELETE FROM objects
		WHERE bucket = ? AND object_key = ?
	`, id.Bucket, id.Key); err != nil {
		return fmt.Errorf("unregister object %q/%q: delete object: %w", id.Bucket, id.Key, err)
	}
	if err := decrementBlob(ctx, tx, blobBucket, hash); err != nil {
		return fmt.Errorf("unregister object %q/%q: %w", id.Bucket, id.Key, err)
	}
	return nil
}

func (s *SQLiteStore) ListDedupCandidates(
	ctx context.Context,
	bucket string,
	prefix string,
	desiredState ObjectState,
	afterKey string,
	limit int,
) ([]DedupCandidate, error) {
	if bucket == "" {
		return nil, fmt.Errorf("list dedup candidates: bucket is empty")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("list dedup candidates: limit must be positive")
	}

	var stateCondition string
	switch desiredState {
	case ObjectStateBlobReady:
		stateCondition = `o.object_state = 'reported'`
	case ObjectStatePointer:
		stateCondition = `o.object_state IN ('reported', 'blob_ready')`
	default:
		return nil, fmt.Errorf("list dedup candidates: unsupported desired state %q", desiredState)
	}

	query := `
		SELECT
			o.bucket,
			o.blob_bucket,
			b.blob_key,
			o.object_key,
			o.etag,
			o.size,
			b.size,
			o.last_modified,
			o.blob_hash,
			o.hash_algo
		FROM objects AS o
		JOIN blobs AS b
		ON b.bucket = o.blob_bucket
		AND b.hash = o.blob_hash
		WHERE o.bucket = ?
		AND o.object_key > ?
		AND substr(o.object_key, 1, length(?)) = ?
		AND b.ref_count > 1
		AND ` + stateCondition + `
		ORDER BY o.object_key
		LIMIT ?
	`
	rows, err := s.db.QueryContext(ctx, query, bucket, afterKey, prefix, prefix, limit)
	if err != nil {
		return nil, fmt.Errorf("list dedup candidates for %q/%q: %w", bucket, prefix, err)
	}
	defer rows.Close()

	candidates := make([]DedupCandidate, 0, limit)
	for rows.Next() {
		var candidate DedupCandidate
		var lastModified string
		if err := rows.Scan(
			&candidate.Bucket,
			&candidate.BlobBucket,
			&candidate.BlobKey,
			&candidate.Key,
			&candidate.ETag,
			&candidate.Size,
			&candidate.BlobSize,
			&lastModified,
			&candidate.Hash,
			&candidate.HashAlgo,
		); err != nil {
			return nil, fmt.Errorf("scan dedup candidate: %w", err)
		}
		candidate.LastModified, err = time.Parse(sqliteTimeFormat, lastModified)
		if err != nil {
			return nil, fmt.Errorf("parse last modified for %q/%q: %w", candidate.Bucket, candidate.Key, err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dedup candidates for %q/%q: %w", bucket, prefix, err)
	}
	return candidates, nil
}
