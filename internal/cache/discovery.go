package cache

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const createDiscoveryRegisterBatchTable = `
	CREATE TEMP TABLE IF NOT EXISTS discovery_register_batch (
		bucket TEXT NOT NULL,
		object_key TEXT NOT NULL,
		etag TEXT NOT NULL,
		size INTEGER NOT NULL,
		last_modified TEXT NOT NULL,
		blob_bucket TEXT NOT NULL,
		blob_key TEXT NOT NULL,
		blob_hash TEXT NOT NULL,
		blob_size INTEGER NOT NULL,
		hash_algo TEXT NOT NULL,
		last_seen_scan TEXT NOT NULL,
		object_state TEXT NOT NULL,
		PRIMARY KEY (bucket, object_key)
	) WITHOUT ROWID
`

const createDiscoveryObjectIDBatchTable = `
	CREATE TEMP TABLE IF NOT EXISTS discovery_object_id_batch (
		bucket TEXT NOT NULL,
		object_key TEXT NOT NULL,
		scan_id TEXT NOT NULL,
		PRIMARY KEY (bucket, object_key)
	) WITHOUT ROWID
`

const upsertDiscoveryObjectsQuery = `
	INSERT INTO objects (
		bucket,
		object_key,
		etag,
		size,
		last_modified,
		blob_bucket,
		blob_hash,
		hash_algo,
		last_seen_scan,
		object_state
	)
	SELECT
		bucket,
		object_key,
		etag,
		size,
		last_modified,
		blob_bucket,
		blob_hash,
		hash_algo,
		last_seen_scan,
		object_state
	FROM discovery_register_batch
	WHERE true
	ON CONFLICT(bucket, object_key) DO UPDATE SET
		etag = excluded.etag,
		size = excluded.size,
		last_modified = excluded.last_modified,
		blob_bucket = excluded.blob_bucket,
		blob_hash = excluded.blob_hash,
		hash_algo = excluded.hash_algo,
		last_seen_scan = excluded.last_seen_scan,
		object_state = excluded.object_state
`

const markDiscoveryObjectsSeenQuery = `
	INSERT INTO objects (
		bucket,
		object_key,
		etag,
		size,
		last_modified,
		blob_bucket,
		blob_hash,
		hash_algo,
		last_seen_scan,
		object_state
	)
	SELECT
		o.bucket,
		o.object_key,
		o.etag,
		o.size,
		o.last_modified,
		o.blob_bucket,
		o.blob_hash,
		o.hash_algo,
		i.scan_id,
		o.object_state
	FROM discovery_object_id_batch AS i
	CROSS JOIN objects AS o
	ON o.bucket = i.bucket
	AND o.object_key = i.object_key
	WHERE true
	ON CONFLICT(bucket, object_key) DO UPDATE SET
		last_seen_scan = excluded.last_seen_scan
`

const decrementDiscoveryChangedBlobsQuery = `
	WITH decrements AS (
		SELECT
			o.blob_bucket AS bucket,
			o.blob_hash AS hash,
			COUNT(*) AS ref_count
		FROM discovery_register_batch AS r
		CROSS JOIN objects AS o
		ON o.bucket = r.bucket
		AND o.object_key = r.object_key
		WHERE o.blob_bucket <> r.blob_bucket
		OR o.blob_hash <> r.blob_hash
		GROUP BY o.blob_bucket, o.blob_hash
	)
	UPDATE blobs
	SET ref_count = blobs.ref_count - d.ref_count
	FROM decrements AS d
	WHERE d.bucket = blobs.bucket
	AND d.hash = blobs.hash
`

const decrementDiscoveryUnregisteredBlobsQuery = `
	WITH decrements AS (
		SELECT
			o.blob_bucket AS bucket,
			o.blob_hash AS hash,
			COUNT(*) AS ref_count
		FROM discovery_object_id_batch AS i
		CROSS JOIN objects AS o
		ON o.bucket = i.bucket
		AND o.object_key = i.object_key
		GROUP BY o.blob_bucket, o.blob_hash
	)
	UPDATE blobs
	SET ref_count = blobs.ref_count - d.ref_count
	FROM decrements AS d
	WHERE d.bucket = blobs.bucket
	AND d.hash = blobs.hash
`

type discoveryRegisterRow struct {
	Bucket       string `json:"bucket"`
	ObjectKey    string `json:"object_key"`
	ETag         string `json:"etag"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified"`
	BlobBucket   string `json:"blob_bucket"`
	BlobKey      string `json:"blob_key"`
	BlobHash     string `json:"blob_hash"`
	BlobSize     int64  `json:"blob_size"`
	HashAlgo     string `json:"hash_algo"`
	LastSeenScan string `json:"last_seen_scan"`
	ObjectState  string `json:"object_state"`
}

type discoveryObjectIDRow struct {
	Bucket    string `json:"bucket"`
	ObjectKey string `json:"object_key"`
	ScanID    string `json:"scan_id"`
}

type discoveryObjectKey struct {
	bucket string
	key    string
}

func (s *SQLiteStore) ApplyDiscoveryBatch(ctx context.Context, mutations []DiscoveryMutation) error {
	if len(mutations) == 0 {
		return nil
	}

	registers := make([]ObjectRecord, 0, len(mutations))
	marks := make([]ObjectID, 0, len(mutations))
	unregisters := make([]ObjectID, 0, len(mutations))
	seenObjects := make(map[discoveryObjectKey]struct{}, len(mutations))
	hasDuplicateObject := false
	for _, mutation := range mutations {
		var bucket string
		var key string
		switch mutation.Kind {
		case DiscoveryRegister:
			if err := validateObject(mutation.Object); err != nil {
				return err
			}
			bucket = mutation.Object.Bucket
			key = mutation.Object.Key
			registers = append(registers, mutation.Object)
		case DiscoveryMarkSeen:
			if mutation.ID.Bucket == "" || mutation.ID.Key == "" || mutation.ID.ScanID == "" {
				return fmt.Errorf("mark object seen: bucket, key and scan ID are required")
			}
			bucket = mutation.ID.Bucket
			key = mutation.ID.Key
			marks = append(marks, mutation.ID)
		case DiscoveryUnregister:
			if mutation.ID.Bucket == "" || mutation.ID.Key == "" {
				return fmt.Errorf("unregister object: bucket and key are required")
			}
			bucket = mutation.ID.Bucket
			key = mutation.ID.Key
			unregisters = append(unregisters, mutation.ID)
		default:
			return fmt.Errorf("apply discovery batch: unsupported mutation kind %d", mutation.Kind)
		}

		objectID := discoveryObjectKey{bucket: bucket, key: key}
		if _, exists := seenObjects[objectID]; exists {
			hasDuplicateObject = true
		}
		seenObjects[objectID] = struct{}{}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("apply discovery batch: begin transaction: %w", err)
	}
	defer tx.Rollback()

	if hasDuplicateObject {
		if err := applyDiscoveryMutationsSequential(ctx, tx, mutations); err != nil {
			return err
		}
	} else {
		if err := applyDiscoveryRegisterBatch(ctx, tx, registers); err != nil {
			return err
		}
		if err := applyDiscoveryMarkSeenBatch(ctx, tx, marks); err != nil {
			return err
		}
		if err := applyDiscoveryUnregisterBatch(ctx, tx, unregisters); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("apply discovery batch: commit: %w", err)
	}
	return nil
}

func applyDiscoveryMutationsSequential(
	ctx context.Context,
	tx *sql.Tx,
	mutations []DiscoveryMutation,
) error {
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
		}
	}
	return nil
}

func applyDiscoveryRegisterBatch(
	ctx context.Context,
	tx *sql.Tx,
	objects []ObjectRecord,
) error {
	if len(objects) == 0 {
		return nil
	}

	rows := make([]discoveryRegisterRow, len(objects))
	for index, object := range objects {
		rows[index] = discoveryRegisterRow{
			Bucket:       object.Bucket,
			ObjectKey:    object.Key,
			ETag:         object.ETag,
			Size:         object.Size,
			LastModified: formatObjectTime(object.LastModified),
			BlobBucket:   object.BlobBucket,
			BlobKey:      object.BlobKey,
			BlobHash:     object.Hash,
			BlobSize:     object.BlobSize,
			HashAlgo:     object.HashAlgo,
			LastSeenScan: object.LastSeenScan,
			ObjectState:  string(object.State),
		}
	}
	payload, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("apply discovery register batch: encode rows: %w", err)
	}
	if err := loadDiscoveryRegisterBatch(ctx, tx, payload); err != nil {
		return err
	}

	var referenceChanges int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM discovery_register_batch AS r
		LEFT JOIN objects AS o
		ON o.bucket = r.bucket
		AND o.object_key = r.object_key
		WHERE o.object_key IS NULL
		OR o.blob_bucket <> r.blob_bucket
		OR o.blob_hash <> r.blob_hash
	`).Scan(&referenceChanges); err != nil {
		return fmt.Errorf("apply discovery register batch: count reference changes: %w", err)
	}
	if referenceChanges == 0 {
		if err := upsertDiscoveryObjects(ctx, tx); err != nil {
			return fmt.Errorf("apply discovery register batch: update object metadata: %w", err)
		}
		return nil
	}

	var blobBucket string
	var blobHash string
	err = tx.QueryRowContext(ctx, `
		SELECT
			r.blob_bucket,
			r.blob_hash
		FROM discovery_register_batch AS r
		LEFT JOIN objects AS o
		ON o.bucket = r.bucket
		AND o.object_key = r.object_key
		WHERE (
			o.object_key IS NULL
			OR o.blob_bucket <> r.blob_bucket
			OR o.blob_hash <> r.blob_hash
		)
		GROUP BY r.blob_bucket, r.blob_hash
		HAVING MIN(r.blob_key) <> MAX(r.blob_key)
		OR MIN(r.blob_size) <> MAX(r.blob_size)
		LIMIT 1
	`).Scan(&blobBucket, &blobHash)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return fmt.Errorf("apply discovery register batch: validate incoming blobs: %w", err)
	default:
		return fmt.Errorf(
			"blob %q/%q metadata conflicts within discovery batch",
			blobBucket,
			blobHash,
		)
	}

	var storedBlobKey string
	var storedBlobSize int64
	var incomingBlobKey string
	var incomingBlobSize int64
	err = tx.QueryRowContext(ctx, `
		SELECT
			r.blob_bucket,
			r.blob_hash,
			b.blob_key,
			b.size,
			r.blob_key,
			r.blob_size
		FROM discovery_register_batch AS r
		LEFT JOIN objects AS o
		ON o.bucket = r.bucket
		AND o.object_key = r.object_key
		JOIN blobs AS b
		ON b.bucket = r.blob_bucket
		AND b.hash = r.blob_hash
		WHERE (
			o.object_key IS NULL
			OR o.blob_bucket <> r.blob_bucket
			OR o.blob_hash <> r.blob_hash
		)
		AND (b.blob_key <> r.blob_key OR b.size <> r.blob_size)
		LIMIT 1
	`).Scan(
		&blobBucket,
		&blobHash,
		&storedBlobKey,
		&storedBlobSize,
		&incomingBlobKey,
		&incomingBlobSize,
	)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return fmt.Errorf("apply discovery register batch: validate blobs: %w", err)
	case storedBlobKey != incomingBlobKey:
		return fmt.Errorf(
			"blob %q/%q key mismatch: stored %q, got %q",
			blobBucket,
			blobHash,
			storedBlobKey,
			incomingBlobKey,
		)
	default:
		return fmt.Errorf(
			"blob %q/%q size mismatch: stored %d, got %d",
			blobBucket,
			blobHash,
			storedBlobSize,
			incomingBlobSize,
		)
	}

	if _, err := tx.ExecContext(ctx, `
		WITH increments AS (
			SELECT
				r.blob_bucket AS bucket,
				r.blob_key AS blob_key,
				r.blob_hash AS hash,
				r.blob_size AS size,
				COUNT(*) AS ref_count
			FROM discovery_register_batch AS r
			LEFT JOIN objects AS o
			ON o.bucket = r.bucket
			AND o.object_key = r.object_key
			WHERE o.object_key IS NULL
			OR o.blob_bucket <> r.blob_bucket
			OR o.blob_hash <> r.blob_hash
			GROUP BY r.blob_bucket, r.blob_key, r.blob_hash, r.blob_size
		)
		INSERT INTO blobs (bucket, blob_key, hash, size, ref_count)
		SELECT bucket, blob_key, hash, size, ref_count
		FROM increments
		WHERE true
		ON CONFLICT(bucket, hash) DO UPDATE SET
			ref_count = blobs.ref_count + excluded.ref_count
	`); err != nil {
		return fmt.Errorf("apply discovery register batch: increment blobs: %w", err)
	}

	if _, err := tx.ExecContext(ctx, decrementDiscoveryChangedBlobsQuery); err != nil {
		return fmt.Errorf("apply discovery register batch: decrement blobs: %w", err)
	}

	if err := upsertDiscoveryObjects(ctx, tx); err != nil {
		return fmt.Errorf("apply discovery register batch: upsert objects: %w", err)
	}
	return nil
}

func upsertDiscoveryObjects(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, upsertDiscoveryObjectsQuery)
	return err
}

func applyDiscoveryMarkSeenBatch(
	ctx context.Context,
	tx *sql.Tx,
	objects []ObjectID,
) error {
	if len(objects) == 0 {
		return nil
	}
	payload, err := marshalDiscoveryObjectIDs(objects)
	if err != nil {
		return fmt.Errorf("apply discovery mark-seen batch: %w", err)
	}
	if err := loadDiscoveryObjectIDBatch(ctx, tx, payload); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, markDiscoveryObjectsSeenQuery); err != nil {
		return fmt.Errorf("apply discovery mark-seen batch: update objects: %w", err)
	}
	return nil
}

func applyDiscoveryUnregisterBatch(
	ctx context.Context,
	tx *sql.Tx,
	objects []ObjectID,
) error {
	if len(objects) == 0 {
		return nil
	}
	payload, err := marshalDiscoveryObjectIDs(objects)
	if err != nil {
		return fmt.Errorf("apply discovery unregister batch: %w", err)
	}
	if err := loadDiscoveryObjectIDBatch(ctx, tx, payload); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, decrementDiscoveryUnregisteredBlobsQuery); err != nil {
		return fmt.Errorf("apply discovery unregister batch: decrement blobs: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM objects
		WHERE (bucket, object_key) IN (
			SELECT bucket, object_key
			FROM discovery_object_id_batch
		)
	`); err != nil {
		return fmt.Errorf("apply discovery unregister batch: delete objects: %w", err)
	}
	return nil
}

func loadDiscoveryRegisterBatch(ctx context.Context, tx *sql.Tx, payload []byte) error {
	if _, err := tx.ExecContext(ctx, createDiscoveryRegisterBatchTable); err != nil {
		return fmt.Errorf("apply discovery register batch: create staging table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM discovery_register_batch`); err != nil {
		return fmt.Errorf("apply discovery register batch: clear staging table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO discovery_register_batch (
			bucket,
			object_key,
			etag,
			size,
			last_modified,
			blob_bucket,
			blob_key,
			blob_hash,
			blob_size,
			hash_algo,
			last_seen_scan,
			object_state
		)
		SELECT
			json_extract(value, '$.bucket'),
			json_extract(value, '$.object_key'),
			json_extract(value, '$.etag'),
			CAST(json_extract(value, '$.size') AS INTEGER),
			json_extract(value, '$.last_modified'),
			json_extract(value, '$.blob_bucket'),
			json_extract(value, '$.blob_key'),
			json_extract(value, '$.blob_hash'),
			CAST(json_extract(value, '$.blob_size') AS INTEGER),
			json_extract(value, '$.hash_algo'),
			json_extract(value, '$.last_seen_scan'),
			json_extract(value, '$.object_state')
		FROM json_each(?)
	`, payload); err != nil {
		return fmt.Errorf("apply discovery register batch: load staging table: %w", err)
	}
	return nil
}

func loadDiscoveryObjectIDBatch(ctx context.Context, tx *sql.Tx, payload []byte) error {
	if _, err := tx.ExecContext(ctx, createDiscoveryObjectIDBatchTable); err != nil {
		return fmt.Errorf("apply discovery object batch: create staging table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM discovery_object_id_batch`); err != nil {
		return fmt.Errorf("apply discovery object batch: clear staging table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO discovery_object_id_batch (bucket, object_key, scan_id)
		SELECT
			json_extract(value, '$.bucket'),
			json_extract(value, '$.object_key'),
			json_extract(value, '$.scan_id')
		FROM json_each(?)
	`, payload); err != nil {
		return fmt.Errorf("apply discovery object batch: load staging table: %w", err)
	}
	return nil
}

func marshalDiscoveryObjectIDs(objects []ObjectID) ([]byte, error) {
	rows := make([]discoveryObjectIDRow, len(objects))
	for index, object := range objects {
		rows[index] = discoveryObjectIDRow{
			Bucket:    object.Bucket,
			ObjectKey: object.Key,
			ScanID:    object.ScanID,
		}
	}
	payload, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("encode rows: %w", err)
	}
	return payload, nil
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
