package cache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const sqliteTimeFormat = "2006-01-02T15:04:05.999999999Z07:00"

type SQLiteStore struct {
	db *sql.DB
}

func formatObjectTime(value time.Time) string {
	return value.UTC().Truncate(time.Second).Format(sqliteTimeFormat)
}

func OpenSQLite(path string) (*SQLiteStore, error) {
	if path == "" {
		return nil, fmt.Errorf("SQLite cache: path is empty")
	}

	//Creating a directory if needed
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create sqlite cache directory %q: %w", dir, err)
		}
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("Open SQLite cach %q: %w", path, err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := &SQLiteStore{
		db: db,
	}
	err = store.initialize(context.Background())
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("Error initializing db: %w", err)
	}
	return store, nil
}

func (s *SQLiteStore) initialize(ctx context.Context) error {
	statements := []string{
		`PRAGMA foreign_keys = ON`,
		`PRAGMA journal_mode = WAL`,
		`PRAGMA busy_timeout = 5000`,
		`CREATE TABLE IF NOT EXISTS blobs (
			bucket TEXT NOT NULL,
			blob_key TEXT NOT NULL,
			hash TEXT NOT NULL,
			size INTEGER NOT NULL CHECK (size >= 0),
			ref_count INTEGER NOT NULL CHECK (ref_count >= 0),
			PRIMARY KEY (bucket, hash)
		)`,
		`CREATE TABLE IF NOT EXISTS objects (
			bucket TEXT NOT NULL,
			object_key TEXT NOT NULL,
			etag TEXT NOT NULL,
			size INTEGER NOT NULL CHECK (size >= 0),
			last_modified TEXT NOT NULL,
			blob_bucket TEXT NOT NULL,
			blob_hash TEXT NOT NULL,
			hash_algo TEXT NOT NULL,
			last_seen_scan TEXT NOT NULL,
			object_state TEXT NOT NULL CHECK (object_state = "reported" OR object_state = "blob_ready" OR object_state = "pointer"),
			PRIMARY KEY (bucket, object_key),
			FOREIGN KEY (blob_bucket, blob_hash) REFERENCES blobs(bucket, hash)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_objects_blob_bucket_blob_hash ON objects(blob_bucket, blob_hash)`,
	}

	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("initialize sqlite cache: %w", err)
		}
	}
	return nil
}

func (s *SQLiteStore) RegisterObject(ctx context.Context, object ObjectRecord) error {
	err := validateObject(object)
	if err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("register object %q/%q: begin transaction: %w", object.Bucket, object.Key, err)
	}
	defer tx.Rollback()

	var oldBlobBucket string
	var oldHash string
	err = tx.QueryRowContext(ctx,
		`SELECT blob_bucket, blob_hash FROM objects WHERE bucket = ? AND object_key = ?`,
		object.Bucket,
		object.Key).Scan(&oldBlobBucket, &oldHash)

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
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("register object %q/%q: commit transaction: %w", object.Bucket, object.Key, err)
	}
	return nil
}

func (s *SQLiteStore) UnregisterObject(
	ctx context.Context,
	bucket string,
	key string,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("unregister object %q/%q: begin transaction: %w", bucket, key, err)
	}
	defer tx.Rollback()

	var blobBucket string
	var hash string
	err = tx.QueryRowContext(ctx, `
        SELECT blob_bucket, blob_hash
        FROM objects
        WHERE bucket = ? AND object_key = ?
    `, bucket, key).Scan(&blobBucket, &hash)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("unregister object %q/%q: read object: %w", bucket, key, err)
	}

	if _, err := tx.ExecContext(ctx, `
        DELETE FROM objects
        WHERE bucket = ? AND object_key = ?
    `, bucket, key); err != nil {
		return fmt.Errorf("unregister object %q/%q: delete object: %w", bucket, key, err)
	}

	if err := decrementBlob(ctx, tx, blobBucket, hash); err != nil {
		return fmt.Errorf("unregister object %q/%q: %w", bucket, key, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("unregister object %q/%q: commit: %w", bucket, key, err)
	}
	return nil
}

func (s *SQLiteStore) GetObjectStatus(
	ctx context.Context,
	bucket string,
	key string,
	etag string,
	size int64,
	hashAlgo string,
	lastModified time.Time,
) (status ObjectStatus, err error) {
	const query = `
	SELECT o.object_state, b.ref_count 
	FROM objects AS o
	JOIN blobs AS b
	ON b.bucket = o.blob_bucket
	AND b.hash = o.blob_hash
	WHERE o.bucket = ?
	AND o.object_key = ? 
	AND o.etag = ?
	AND o.size = ?
	AND o.hash_algo = ?
	AND o.last_modified = ?
	`
	err = s.db.QueryRowContext(ctx, query, bucket, key, etag, size, hashAlgo, formatObjectTime(lastModified)).Scan(&status.State, &status.RefCount)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return ObjectStatus{}, nil
	case err != nil:
		return ObjectStatus{}, fmt.Errorf("GetObjectStatus %q/%q: %w", bucket, key, err)
	case status.State == "":
		return ObjectStatus{}, fmt.Errorf("GetObjectsStatus %q/%q: undefined object_state in cache", bucket, key)
	}
	status.Unchanged = true
	return status, nil
}

func validateObject(object ObjectRecord) error {
	switch {
	case object.BlobBucket == "":
		return fmt.Errorf("register object: blob bucket is empty")
	case object.BlobKey == "":
		return fmt.Errorf("register object: blob key is empty")
	case object.BlobSize < 0:
		return fmt.Errorf("register object %q/%q: size is negative", object.Bucket, object.Key)
	case object.Bucket == "":
		return fmt.Errorf("register object: bucket is empty")
	case object.Key == "":
		return fmt.Errorf("register object: key is empty")
	case object.Hash == "":
		return fmt.Errorf("register object %q/%q: hash is empty", object.Bucket, object.Key)
	case object.Size < 0:
		return fmt.Errorf("register object %q/%q: size is negative", object.Bucket, object.Key)
	case object.HashAlgo == "":
		return fmt.Errorf("register object %q/%q: hash_algo is empty", object.Bucket, object.Key)
	default:
		return nil
	}
}

func incrementBlob(ctx context.Context, tx *sql.Tx, bucket, blobKey string, hash string, size int64) error {
	var storedBlobKey string
	var storedSize int64
	err := tx.QueryRowContext(ctx,
		`SELECT blob_key, size FROM blobs 
		WHERE bucket = ?
		AND hash = ?`, bucket, hash).Scan(&storedBlobKey, &storedSize)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		//Insert new blob
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO blobs (bucket, blob_key, hash, size, ref_count) VALUES (?, ?, ?, ?, 1)`,
			bucket,
			blobKey,
			hash,
			size,
		); err != nil {
			return fmt.Errorf("insert blob %q: %w", hash, err)
		}
		return nil
	case storedBlobKey == blobKey && storedSize == size:
		// Update ref_count for blobs, that already was in cache
		row, err := tx.ExecContext(ctx,
			`UPDATE blobs SET ref_count = ref_count + 1 
			WHERE bucket = ? AND hash = ? AND blob_key = ?`,
			bucket,
			hash,
			blobKey,
		)
		rowsAffected, _ := row.RowsAffected()
		if err != nil {
			return fmt.Errorf("increment blob %q refcount: %w", hash, err)
		}
		if rowsAffected != 1 {
			return fmt.Errorf("increment blob %q refcount, no rows affected", hash)
		}
	case err != nil:
		return fmt.Errorf("read blob %q: %w", hash, err)
	case storedSize != size:
		return fmt.Errorf("blob %q size mismatch: stored %d, object %d", hash, storedSize, size)
	case storedBlobKey != blobKey:
		return fmt.Errorf("blob %q key mismatch: stored %q, got %q", hash, storedBlobKey, blobKey)

	}

	return nil
}

func decrementBlob(ctx context.Context, tx *sql.Tx, bucket string, hash string) error {
	result, err := tx.ExecContext(ctx,
		`UPDATE blobs SET ref_count = ref_count - 1 WHERE bucket = ? AND hash = ? AND ref_count > 0`,
		bucket,
		hash,
	)
	if err != nil {
		return fmt.Errorf("decrement blob %q refcount: %w", hash, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("decrement blob %q refcount: read affected rows: %w", hash, err)
	}
	if rows != 1 {
		return fmt.Errorf("decrement blob %q refcount: blob missing or refcount is zero", hash)
	}
	return nil
}

// Getting required stats for report
func (s *SQLiteStore) GetStats(ctx context.Context, scopes ...Scope) (Stats, error) {
	where, args, err := statsScopeFilter(scopes)
	if err != nil {
		return Stats{}, err
	}
	query := `
		WITH groups AS (
			SELECT
				o.blob_bucket,
				o.blob_hash,
				b.size,
				COUNT(*) AS object_count,
				SUM(CASE
					WHEN o.object_state <> 'pointer' THEN 1
					ELSE 0
				END) AS original_count,
				SUM(CASE
					WHEN o.object_state IN ('blob_ready', 'pointer') THEN 1
					ELSE 0
				END) AS materialized_count
			FROM objects AS o
			JOIN blobs AS b
			ON b.bucket = o.blob_bucket
			AND b.hash = o.blob_hash
			` + where + `
			GROUP BY o.blob_bucket, o.blob_hash, b.size
		)
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE
				WHEN object_count > 1 THEN object_count - 1
				ELSE 0
			END), 0),
			COALESCE(SUM(CASE
				WHEN object_count <= 1 OR original_count = 0 THEN 0
				WHEN materialized_count > 0 THEN original_count * size
				ELSE (original_count - 1) * size
			END), 0)
		FROM groups
	`
	var stats Stats
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(
		&stats.UniqueBlobs,
		&stats.DuplicatesFound,
		&stats.BytesReclaimable,
	); err != nil {
		return Stats{}, fmt.Errorf("Error getting stats: %w", err)
	}
	return stats, nil
}

func statsScopeFilter(scopes []Scope) (string, []interface{}, error) {
	if len(scopes) == 0 {
		return "", nil, nil
	}

	conditions := make([]string, 0, len(scopes))
	args := make([]interface{}, 0, len(scopes)*3)
	for _, scope := range scopes {
		if scope.Bucket == "" {
			return "", nil, fmt.Errorf("get stats: scope bucket is empty")
		}
		conditions = append(conditions, `(o.bucket = ? AND substr(o.object_key, 1, length(?)) = ?)`)
		args = append(args, scope.Bucket, scope.Prefix, scope.Prefix)
	}
	return "WHERE " + strings.Join(conditions, " OR "), args, nil
}

// Marking object anyway
func (s *SQLiteStore) MarkObjectSeen(ctx context.Context, bucket, key, scanID string) error {
	const query = `
	UPDATE objects
	SET last_seen_scan = ?
	WHERE bucket = ? AND object_key = ?
	`
	result, err := s.db.ExecContext(ctx, query, scanID, bucket, key)
	if err != nil {
		return fmt.Errorf("Error marking an object: %w", err)
	}
	_, err = result.RowsAffected()
	if err != nil {
		return fmt.Errorf("Object %q/%q is not marked: %w", bucket, key, err)
	}
	return nil
}

// Getting rid of deleted objects from cache and updating ref_count for blobs
func (s *SQLiteStore) FinalizeScope(ctx context.Context, bucket, prefix, scanID string) (removed int64, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("Finilazing scope for %q/%q during scan %q: begin transaction: %w", bucket, prefix, scanID, err)
	}
	defer tx.Rollback()

	// Hash and number of objects that hasn't been discovered during this scan
	rows, err := tx.QueryContext(ctx,
		`SELECT blob_bucket, blob_hash, COUNT(*) 
	FROM objects 
	WHERE bucket = ? 
	AND substr(object_key, 1, length(?)) = ?
	AND last_seen_scan <> ?
	GROUP BY blob_bucket, blob_hash`, bucket, prefix, prefix, scanID,
	)
	if err != nil {
		return 0, fmt.Errorf("FinalizeScope query failed: %w", err)
	}
	type blobID struct {
		bucket string
		hash   string
	}
	var blobsCount = make(map[blobID]int)
	for rows.Next() {
		var bucket string
		var hash string
		var cnt int
		err := rows.Scan(&bucket, &hash, &cnt)
		if err != nil {
			return 0, fmt.Errorf("row scan failed: %w", err)
		}
		blobsCount[blobID{bucket: bucket, hash: hash}] = cnt
	}
	// Updating ref_count for blobs
	for key, value := range blobsCount {
		_, err := tx.ExecContext(ctx,
			`UPDATE blobs
		SET ref_count = ref_count - ?
		WHERE bucket = ? AND hash = ?`, value, key.bucket, key.hash)
		if err != nil {
			return 0, fmt.Errorf("Error updating ref_count for %q: %w", key, err)
		}
	}
	//Deleting old objects, that hasn't been discovered during this scan
	res, err := tx.ExecContext(ctx,
		`DELETE FROM objects
	WHERE bucket = ?
	AND substr(object_key, 1, length(?)) = ?
	AND last_seen_scan <> ?`, bucket, prefix, prefix, scanID)
	if err != nil {
		return 0, fmt.Errorf("Error deleting old objects in cache: %w", err)
	}
	removed, err = res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("Error extracting rows affected: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("FinalizeScope of %q/%q for scan %q: commit transaction: %w", bucket, prefix, scanID, err)
	}
	return removed, nil
}

func (s *SQLiteStore) ListUnreferencedBlobs(ctx context.Context, bucket string) (blobList []BlobRecord, err error) {
	const query = `
	SELECT bucket, blob_key, hash, size 
	FROM blobs
	WHERE ref_count = 0 AND bucket = ?
	`

	rows, err := s.db.QueryContext(ctx, query, bucket)
	if err != nil {
		return nil, fmt.Errorf("ListUnreferencedBlobs query error: %w", err)
	}

	for rows.Next() {
		var bucket, key, hash string
		var size int64
		err := rows.Scan(&bucket, &key, &hash, &size)
		if err != nil {
			return nil, fmt.Errorf("ListUnreferencedBlobs rows scan error: %w", err)
		}

		blobList = append(blobList, BlobRecord{bucket, key, hash, size})
	}

	return blobList, err
}

func (s *SQLiteStore) DeleteUnreferencedBlob(
	ctx context.Context,
	bucket string,
	hash string,
) error {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM blobs
		WHERE bucket = ? AND hash = ? AND ref_count = 0
	`, bucket, hash)
	if err != nil {
		return fmt.Errorf("delete unreferenced blob %q/%q: %w", bucket, hash, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read deleted blob count: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("blob %q/%q is missing or referenced again", bucket, hash)
	}
	return nil
}

// Closing store
func (s *SQLiteStore) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("Error closing SQLite: %w", err)
	}
	return nil
}
