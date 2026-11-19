package metadata

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func openTestSQLite(t *testing.T, path string) *SQLiteStore {
	t.Helper()
	s, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// Restart tests use a real file, never an in-memory database.
func TestSQLitePersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nested", "dir", "metadata.db")

	s := openTestSQLite(t, path)
	live := sampleObject("obj-live", 3)
	tomb := sampleObject("obj-tomb", 2)
	empty := sampleObject("obj-empty", 0)
	for _, obj := range []Object{live, tomb, empty} {
		if err := s.CreateObject(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	patched, err := s.PatchCustomMetadata(ctx, live.ID, map[string]string{"owner": "bob"})
	if err != nil {
		t.Fatal(err)
	}
	chunk := live.Chunks[2]
	if err := s.UpdateChunkReplicas(ctx, live.ID, chunk.ID, chunk.Replicas, []string{"worker3", "worker1"}); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkDeleting(ctx, tomb.ID); err != nil {
		t.Fatal(err)
	}
	want := mustGet(t, s, live.ID)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openTestSQLite(t, path)
	defer s.Close()
	got := mustGet(t, s, live.ID)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("after reopen\n got: %+v\nwant: %+v", got, want)
	}
	if got.CustomMetadata["owner"] != "bob" || !got.ModifiedAt.Equal(patched.ModifiedAt) {
		t.Fatal("patch not persisted")
	}
	if !reflect.DeepEqual(got.Chunks[2].Replicas, []string{"worker3", "worker1"}) {
		t.Fatalf("placement not persisted: %v", got.Chunks[2].Replicas)
	}
	if tombGot := mustGet(t, s, tomb.ID); !tombGot.Deleting {
		t.Fatal("tombstone not persisted")
	}
	if e := mustGet(t, s, empty.ID); !reflect.DeepEqual(e, empty) {
		t.Fatal("empty object not persisted")
	}
	listed, err := s.ListObjects(ctx)
	if err != nil || len(listed) != 2 {
		t.Fatalf("list after reopen: %d %v", len(listed), err)
	}
	// An explicit DELETE retry completes after restart.
	if err := s.DeleteObject(ctx, tomb.ID); err != nil {
		t.Fatal(err)
	}
}

func countRows(t *testing.T, path, table string) int {
	t.Helper()
	db, err := sql.Open(sqliteDriverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSQLiteDeleteRemovesManifestRows(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "metadata.db")
	s := openTestSQLite(t, path)
	if err := s.CreateObject(ctx, sampleObject("keep", 2)); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateObject(ctx, sampleObject("gone", 3)); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteObject(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if n := countRows(t, path, "objects"); n != 1 {
		t.Fatalf("objects rows: %d", n)
	}
	if n := countRows(t, path, "chunks"); n != 2 {
		t.Fatalf("chunk rows: %d", n)
	}
	if n := countRows(t, path, "replicas"); n != 4 {
		t.Fatalf("replica rows: %d", n)
	}
}

// A failure part-way through publication must not leave a partial manifest.
func TestSQLiteFailedCreateLeavesNoRows(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "metadata.db")
	s := openTestSQLite(t, path)

	bad := sampleObject("bad", 3)
	bad.Chunks[2].Replicas = []string{"worker1", "worker1"} // Duplicate worker: rejected.
	if err := s.CreateObject(ctx, bad); err == nil {
		t.Fatal("duplicate replica worker accepted")
	}
	dupIndex := sampleObject("dup-index", 2)
	dupIndex.Chunks[1].Index = 0
	if err := s.CreateObject(ctx, dupIndex); err == nil {
		t.Fatal("duplicate chunk index accepted")
	}
	for _, id := range []string{"bad", "dup-index"} {
		if _, err := s.GetObject(ctx, id); !errors.Is(err, ErrObjectNotFound) {
			t.Fatalf("%s partially published: %v", id, err)
		}
	}
	s.Close()
	for _, table := range []string{"objects", "chunks", "replicas"} {
		if n := countRows(t, path, table); n != 0 {
			t.Fatalf("%s has %d leftover rows", table, n)
		}
	}
}

func TestSQLiteOpenErrors(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLiteStore(filepath.Join(file, "metadata.db")); err == nil {
		t.Fatal("opened database below a regular file")
	}
	if _, err := OpenSQLiteStore(""); err == nil {
		t.Fatal("opened empty path")
	}
	garbage := filepath.Join(dir, "garbage.db")
	if err := os.WriteFile(garbage, []byte(strings.Repeat("not a database ", 100)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLiteStore(garbage); err == nil {
		t.Fatal("opened a non-SQLite file")
	}
}

func TestSQLiteRejectsNewerSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")
	s := openTestSQLite(t, path)
	if _, err := s.db.Exec("PRAGMA user_version = 99"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if _, err := OpenSQLiteStore(path); err == nil || !strings.Contains(err.Error(), "newer") {
		t.Fatalf("newer schema: %v", err)
	}
}

func TestSQLiteSeparateFilesAreIndependent(t *testing.T) {
	ctx := context.Background()
	a := openTestSQLite(t, filepath.Join(t.TempDir(), "a.db"))
	defer a.Close()
	b := openTestSQLite(t, filepath.Join(t.TempDir(), "b.db"))
	defer b.Close()
	if err := a.CreateObject(ctx, sampleObject("obj", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.GetObject(ctx, "obj"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("databases share state: %v", err)
	}
}
