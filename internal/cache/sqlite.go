package cache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	db *sql.DB
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
			hash TEXT PRIMARY KEY,
			size INTEGER NOT NULL CHECK (size >= 0),
			ref_count INTEGER NOT NULL CHECK (ref_count >= 0)
		)`,
		`CREATE TABLE IF NOT EXISTS objects (
			bucket TEXT NOT NULL,
			object_key TEXT NOT NULL,
			etag TEXT NOT NULL,
			size INTEGER NOT NULL CHECK (size >= 0),
			last_modified TEXT NOT NULL,
			blob_hash TEXT NOT NULL,
			last_seen_scan TEXT NOT NULL,
			PRIMARY KEY (bucket, object_key),
			FOREIGN KEY (blob_hash) REFERENCES blobs(hash)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_objects_blob_hash ON objects(blob_hash)`,
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

	var oldHash string
	err = tx.QueryRowContext(ctx,
		`SELECT blob_hash FROM objects WHERE bucket = ? AND object_key = ?`,
		object.Bucket,
		object.Key).Scan(&oldHash)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		if err := incrementBlob(ctx, tx, object.Hash, object.Size); err != nil {
			return fmt.Errorf("register object %q/%q: %w", object.Bucket, object.Key, err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO objects (
				bucket, object_key, etag, size, last_modified, blob_hash, last_seen_scan
			) VALUES (?, ?, ?, ?, ?, ?, ?)
		`,
			object.Bucket,
			object.Key,
			object.ETag,
			object.Size,
			object.LastModified.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
			object.Hash,
			object.LastSeenScan,
		); err != nil {
			return fmt.Errorf("register object %q/%q: insert object: %w", object.Bucket, object.Key, err)
		}
	case err != nil:
		return fmt.Errorf("register object %q/%q: read current state: %w", object.Bucket, object.Key, err)
	case oldHash == object.Hash:
		if _, err := tx.ExecContext(ctx, `
			UPDATE objects
			SET etag = ?, size = ?, last_modified = ?, last_seen_scan = ?
			WHERE bucket = ? AND object_key = ?
		`,
			object.ETag,
			object.Size,
			object.LastModified.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
			object.LastSeenScan,
			object.Bucket,
			object.Key,
		); err != nil {
			return fmt.Errorf("register object %q/%q: update metadata: %w", object.Bucket, object.Key, err)
		}
	default:
		if err := incrementBlob(ctx, tx, object.Hash, object.Size); err != nil {
			return fmt.Errorf("register object %q/%q: %w", object.Bucket, object.Key, err)
		}
		if err := decrementBlob(ctx, tx, oldHash); err != nil {
			return fmt.Errorf("register object %q/%q: %w", object.Bucket, object.Key, err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE objects
			SET etag = ?, size = ?, last_modified = ?, blob_hash = ?, last_seen_scan = ?
			WHERE bucket = ? AND object_key = ?
		`,
			object.ETag,
			object.Size,
			object.LastModified.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
			object.Hash,
			object.LastSeenScan,
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

func validateObject(object ObjectRecord) error {
	switch {
	case object.Bucket == "":
		return fmt.Errorf("register object: bucket is empty")
	case object.Key == "":
		return fmt.Errorf("register object: key is empty")
	case object.Hash == "":
		return fmt.Errorf("register object %q/%q: hash is empty", object.Bucket, object.Key)
	case object.Size < 0:
		return fmt.Errorf("register object %q/%q: size is negative", object.Bucket, object.Key)
	default:
		return nil
	}
}

func incrementBlob(ctx context.Context, tx *sql.Tx, hash string, size int64) error {
	var storedSize int64
	err := tx.QueryRowContext(ctx, `SELECT size FROM blobs WHERE hash = ?`, hash).Scan(&storedSize)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO blobs (hash, size, ref_count) VALUES (?, ?, 1)`,
			hash,
			size,
		); err != nil {
			return fmt.Errorf("insert blob %q: %w", hash, err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("read blob %q: %w", hash, err)
	case storedSize != size:
		return fmt.Errorf("blob %q size mismatch: stored %d, object %d", hash, storedSize, size)
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE blobs SET ref_count = ref_count + 1 WHERE hash = ?`,
		hash,
	); err != nil {
		return fmt.Errorf("increment blob %q refcount: %w", hash, err)
	}
	return nil
}

func decrementBlob(ctx context.Context, tx *sql.Tx, hash string) error {
	result, err := tx.ExecContext(ctx,
		`UPDATE blobs SET ref_count = ref_count - 1 WHERE hash = ? AND ref_count > 0`,
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

func (s *SQLiteStore) GetStats(ctx context.Context) (Stats, error) {
	const query = `
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN object_count > 1 THEN object_count - 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN object_count > 1 THEN (object_count - 1) * size ELSE 0 END), 0)
		FROM (
			SELECT o.blob_hash, COUNT(*) AS object_count, b.size AS size
			FROM objects AS o
			JOIN blobs AS b ON b.hash = o.blob_hash
			GROUP BY o.blob_hash, b.size
		)
	`
	var stats Stats
	if err := s.db.QueryRowContext(ctx, query).Scan(
		&stats.UniqueBlobs,
		&stats.DuplicatesFound,
		&stats.BytesReclaimable,
	); err != nil {
		return Stats{}, fmt.Errorf("Error getting stats: %w", err)
	}
	return stats, nil
}

func (s *SQLiteStore) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("Error closing SQLite: %w", err)
	}
	return nil
}
