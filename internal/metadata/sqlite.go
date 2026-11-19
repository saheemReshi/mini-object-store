package metadata

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"time"
)

//go:embed schema.sql
var sqliteSchema string

// sqliteSchemaVersion is stored in PRAGMA user_version. Bump it (and add an
// upgrade step in migrate) whenever schema.sql changes incompatibly.
const sqliteSchemaVersion = 1

// DefaultSQLiteOpTimeout bounds every Store call, including time spent waiting
// for the single database connection, so lock contention cannot block forever.
const DefaultSQLiteOpTimeout = 10 * time.Second

// SQLiteStore implements Store on a single SQLite database file.
//
// Concurrency model: the pool is limited to ONE connection. database/sql then
// serializes every transaction in this process, which makes the compare-and-swap
// in UpdateChunkReplicas trivially atomic and avoids SQLITE_BUSY upgrade
// deadlocks between deferred transactions. Callers wait for that connection
// only until their context (capped at OpTimeout) expires. Because the only
// connection is held for the life of a transaction, methods must fully read and
// close one result set before issuing the next query.
type SQLiteStore struct {
	db        *sql.DB
	path      string
	OpTimeout time.Duration
}

var _ Store = (*SQLiteStore)(nil)

// OpenSQLiteStore opens (creating if needed) the database at path, applies the
// schema, and verifies that the file is usable. The parent directory must
// already exist or be creatable; errors are returned rather than falling back
// to another backend.
func OpenSQLiteStore(path string) (*SQLiteStore, error) {
	if path == "" {
		return nil, errors.New("sqlite: empty database path")
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("sqlite: create database directory %q: %w", dir, err)
		}
	}

	pragmas := url.Values{}
	for _, p := range []string{
		"foreign_keys(1)",
		"busy_timeout(5000)", // Bounded wait if another process holds a lock.
		"journal_mode(WAL)",
		"synchronous(FULL)", // A committed object survives power loss.
	} {
		pragmas.Add("_pragma", p)
	}

	db, err := sql.Open(sqliteDriverName, path+"?"+pragmas.Encode())
	if err != nil {
		return nil, fmt.Errorf("sqlite: open %q: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

	s := &SQLiteStore{db: db, path: path, OpTimeout: DefaultSQLiteOpTimeout}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: initialize %q: %w", path, err)
	}
	return s, nil
}

// Path returns the database file location.
func (s *SQLiteStore) Path() string { return s.path }

// Close releases the database. It is not part of Store.
func (s *SQLiteStore) Close() error { return s.db.Close() }

func (s *SQLiteStore) migrate(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		return err
	}
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version > sqliteSchemaVersion {
		return fmt.Errorf("database schema version %d is newer than supported version %d",
			version, sqliteSchemaVersion)
	}
	// Each statement is idempotent (IF NOT EXISTS), so reopening an existing
	// database never deletes or rewrites its contents.
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, sqliteSchema); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", sqliteSchemaVersion))
		return err
	})
}

// begin applies the per-operation timeout and reports cancellation before any
// database work, matching MemoryStore.
func (s *SQLiteStore) begin(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if err := ctx.Err(); err != nil {
		return ctx, func() {}, err
	}
	timeout := s.OpTimeout
	if timeout <= 0 {
		timeout = DefaultSQLiteOpTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	return ctx, cancel, nil
}

// withTx runs fn in one transaction. Any error (including from fn) rolls back,
// so a failed mutation leaves metadata unchanged.
func (s *SQLiteStore) withTx(ctx context.Context, fn func(*sql.Tx) error) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

const timeLayout = time.RFC3339Nano

func formatTime(t time.Time) string { return t.UTC().Format(timeLayout) }

func parseTime(v string) (time.Time, error) {
	t, err := time.Parse(timeLayout, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse stored timestamp %q: %w", v, err)
	}
	return t.UTC(), nil
}

func (s *SQLiteStore) CreateObject(ctx context.Context, obj Object) error {
	ctx, cancel, err := s.begin(ctx)
	defer cancel()
	if err != nil {
		return err
	}

	custom := obj.CustomMetadata
	if custom == nil {
		custom = map[string]string{}
	}
	customJSON, err := json.Marshal(custom)
	if err != nil {
		return fmt.Errorf("encode custom metadata: %w", err)
	}

	return s.withTx(ctx, func(tx *sql.Tx) error {
		// Checking inside the transaction is race-free: the single connection
		// serializes all writers, so no other insert can intervene.
		exists, err := objectExists(ctx, tx, obj.ID)
		if err != nil {
			return err
		}
		if exists {
			return ErrObjectExists
		}

		if _, err := tx.ExecContext(ctx, `
			INSERT INTO objects (id, name, size, content_type, created_at, modified_at,
			                     checksum, chunk_count, custom_metadata, deleting)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			obj.ID, obj.Name, obj.Size, obj.ContentType,
			formatTime(obj.CreatedAt), formatTime(obj.ModifiedAt),
			obj.Checksum, obj.ChunkCount, string(customJSON), boolToInt(obj.Deleting),
		); err != nil {
			return fmt.Errorf("insert object: %w", err)
		}

		for _, chunk := range obj.Chunks {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO chunks (object_id, chunk_index, id, size, checksum)
				VALUES (?, ?, ?, ?, ?)`,
				obj.ID, chunk.Index, chunk.ID, chunk.Size, chunk.Checksum,
			); err != nil {
				return fmt.Errorf("insert chunk %s: %w", chunk.ID, err)
			}
			if err := insertReplicas(ctx, tx, obj.ID, chunk.ID, chunk.Replicas); err != nil {
				return err
			}
		}
		return nil
	})
}

func insertReplicas(ctx context.Context, tx *sql.Tx, objectID, chunkID string, workers []string) error {
	for slot, workerID := range workers {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO replicas (object_id, chunk_id, slot, worker_id)
			VALUES (?, ?, ?, ?)`,
			objectID, chunkID, slot, workerID,
		); err != nil {
			return fmt.Errorf("insert replica %s/%s: %w", chunkID, workerID, err)
		}
	}
	return nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func objectExists(ctx context.Context, tx *sql.Tx, id string) (bool, error) {
	var one int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM objects WHERE id = ?`, id).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *SQLiteStore) GetObject(ctx context.Context, id string) (Object, error) {
	ctx, cancel, err := s.begin(ctx)
	defer cancel()
	if err != nil {
		return Object{}, err
	}

	var obj Object
	// A read transaction gives one consistent snapshot of the object row and
	// its manifest even if a writer commits between the three queries.
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		objects, err := loadObjects(ctx, tx, `WHERE id = ?`, id)
		if err != nil {
			return err
		}
		if len(objects) == 0 {
			return ErrObjectNotFound
		}
		obj = objects[0]
		return nil
	})
	return obj, err
}

func (s *SQLiteStore) ListObjects(ctx context.Context) ([]Object, error) {
	ctx, cancel, err := s.begin(ctx)
	defer cancel()
	if err != nil {
		return nil, err
	}

	var objects []Object
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		objects, err = loadObjects(ctx, tx, `WHERE deleting = 0`)
		return err
	})
	if err != nil {
		return nil, err
	}
	return objects, nil
}

// loadObjects reads matching objects (sorted by ID) plus their chunks and
// replicas, using three sequential queries whose result sets are each closed
// before the next is issued.
func loadObjects(ctx context.Context, tx *sql.Tx, where string, args ...any) ([]Object, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, name, size, content_type, created_at, modified_at,
		       checksum, chunk_count, custom_metadata, deleting
		FROM objects `+where+` ORDER BY id`, args...)
	if err != nil {
		return nil, err
	}
	objects := make([]Object, 0)
	index := make(map[string]int)
	for rows.Next() {
		var (
			obj               Object
			created, modified string
			customJSON        string
			deleting          int64
		)
		if err := rows.Scan(&obj.ID, &obj.Name, &obj.Size, &obj.ContentType,
			&created, &modified, &obj.Checksum, &obj.ChunkCount,
			&customJSON, &deleting); err != nil {
			rows.Close()
			return nil, err
		}
		if obj.CreatedAt, err = parseTime(created); err != nil {
			rows.Close()
			return nil, err
		}
		if obj.ModifiedAt, err = parseTime(modified); err != nil {
			rows.Close()
			return nil, err
		}
		obj.CustomMetadata = map[string]string{}
		if err := json.Unmarshal([]byte(customJSON), &obj.CustomMetadata); err != nil {
			rows.Close()
			return nil, fmt.Errorf("decode custom metadata of %s: %w", obj.ID, err)
		}
		if obj.CustomMetadata == nil { // Stored JSON "null".
			obj.CustomMetadata = map[string]string{}
		}
		obj.Deleting = deleting != 0
		obj.Chunks = make([]Chunk, 0)
		index[obj.ID] = len(objects)
		objects = append(objects, obj)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if len(objects) == 0 {
		return objects, nil
	}

	// Restrict the manifest queries to the same object filter via a subquery.
	subquery := `SELECT id FROM objects ` + where

	rows, err = tx.QueryContext(ctx, `
		SELECT object_id, id, chunk_index, size, checksum
		FROM chunks WHERE object_id IN (`+subquery+`)
		ORDER BY object_id, chunk_index`, args...)
	if err != nil {
		return nil, err
	}
	type chunkKey struct{ object, chunk string }
	chunkPos := make(map[chunkKey]int)
	for rows.Next() {
		var objectID string
		var chunk Chunk
		if err := rows.Scan(&objectID, &chunk.ID, &chunk.Index, &chunk.Size, &chunk.Checksum); err != nil {
			rows.Close()
			return nil, err
		}
		i, ok := index[objectID]
		if !ok {
			continue
		}
		chunkPos[chunkKey{objectID, chunk.ID}] = len(objects[i].Chunks)
		objects[i].Chunks = append(objects[i].Chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	rows, err = tx.QueryContext(ctx, `
		SELECT object_id, chunk_id, worker_id
		FROM replicas WHERE object_id IN (`+subquery+`)
		ORDER BY object_id, chunk_id, slot`, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var objectID, chunkID, workerID string
		if err := rows.Scan(&objectID, &chunkID, &workerID); err != nil {
			rows.Close()
			return nil, err
		}
		i, ok := index[objectID]
		if !ok {
			continue
		}
		pos, ok := chunkPos[chunkKey{objectID, chunkID}]
		if !ok {
			continue
		}
		objects[i].Chunks[pos].Replicas = append(objects[i].Chunks[pos].Replicas, workerID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return objects, nil
}

// objectState returns whether the object exists and is being deleted.
func objectState(ctx context.Context, tx *sql.Tx, id string) (deleting bool, err error) {
	var flag int64
	err = tx.QueryRowContext(ctx, `SELECT deleting FROM objects WHERE id = ?`, id).Scan(&flag)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrObjectNotFound
	}
	if err != nil {
		return false, err
	}
	return flag != 0, nil
}

func (s *SQLiteStore) PatchCustomMetadata(
	ctx context.Context,
	id string,
	patch map[string]string,
) (Object, error) {
	ctx, cancel, err := s.begin(ctx)
	defer cancel()
	if err != nil {
		return Object{}, err
	}

	var result Object
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		var customJSON string
		var deleting int64
		err := tx.QueryRowContext(ctx,
			`SELECT custom_metadata, deleting FROM objects WHERE id = ?`, id,
		).Scan(&customJSON, &deleting)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		if deleting != 0 {
			return ErrObjectDeleting
		}

		custom := map[string]string{}
		if err := json.Unmarshal([]byte(customJSON), &custom); err != nil {
			return fmt.Errorf("decode custom metadata of %s: %w", id, err)
		}
		if custom == nil {
			custom = map[string]string{}
		}
		for key, value := range patch {
			custom[key] = value
		}
		merged, err := json.Marshal(custom)
		if err != nil {
			return err
		}

		if _, err := tx.ExecContext(ctx,
			`UPDATE objects SET custom_metadata = ?, modified_at = ? WHERE id = ?`,
			string(merged), formatTime(time.Now()), id,
		); err != nil {
			return err
		}

		objects, err := loadObjects(ctx, tx, `WHERE id = ?`, id)
		if err != nil {
			return err
		}
		if len(objects) == 0 {
			return ErrObjectNotFound
		}
		result = objects[0]
		return nil
	})
	return result, err
}

func (s *SQLiteStore) MarkDeleting(ctx context.Context, id string) error {
	ctx, cancel, err := s.begin(ctx)
	defer cancel()
	if err != nil {
		return err
	}

	return s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE objects SET deleting = 1 WHERE id = ?`, id)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrObjectNotFound
		}
		return nil
	})
}

func (s *SQLiteStore) DeleteObject(ctx context.Context, id string) error {
	ctx, cancel, err := s.begin(ctx)
	defer cancel()
	if err != nil {
		return err
	}

	return s.withTx(ctx, func(tx *sql.Tx) error {
		// Delete children explicitly (not only via ON DELETE CASCADE) so the
		// manifest is removed even if foreign_keys were somehow disabled.
		if _, err := tx.ExecContext(ctx, `DELETE FROM replicas WHERE object_id = ?`, id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM chunks WHERE object_id = ?`, id); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE id = ?`, id)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrObjectNotFound // Rolls back the (empty) child deletes too.
		}
		return nil
	})
}

func (s *SQLiteStore) UpdateChunkReplicas(
	ctx context.Context,
	objectID string,
	chunkID string,
	expected []string,
	replacement []string,
) error {
	ctx, cancel, err := s.begin(ctx)
	defer cancel()
	if err != nil {
		return err
	}

	if len(replacement) != 2 || replacement[0] == replacement[1] {
		return ErrReplicaConflict
	}

	return s.withTx(ctx, func(tx *sql.Tx) error {
		// Existence, tombstone and placement are all checked in this one
		// transaction, so a stale repair cannot overwrite newer placement or
		// resurrect an object whose deletion has started.
		deleting, err := objectState(ctx, tx, objectID)
		if err != nil {
			return err
		}
		if deleting {
			return ErrObjectDeleting
		}

		var one int
		err = tx.QueryRowContext(ctx,
			`SELECT 1 FROM chunks WHERE object_id = ? AND id = ?`, objectID, chunkID,
		).Scan(&one)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrChunkNotFound
		}
		if err != nil {
			return err
		}

		rows, err := tx.QueryContext(ctx, `
			SELECT worker_id FROM replicas
			WHERE object_id = ? AND chunk_id = ? ORDER BY slot`, objectID, chunkID)
		if err != nil {
			return err
		}
		current := make([]string, 0, 2)
		for rows.Next() {
			var workerID string
			if err := rows.Scan(&workerID); err != nil {
				rows.Close()
				return err
			}
			current = append(current, workerID)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}

		if !slices.Equal(current, expected) {
			return ErrReplicaConflict
		}

		if _, err := tx.ExecContext(ctx,
			`DELETE FROM replicas WHERE object_id = ? AND chunk_id = ?`, objectID, chunkID,
		); err != nil {
			return err
		}
		return insertReplicas(ctx, tx, objectID, chunkID, replacement)
	})
}
