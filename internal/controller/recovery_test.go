package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"mini-object-store/internal/metadata"
)

func TestRepairRecoversUntrackedSource(t *testing.T) {
	c := newTestCluster(t)
	data := []byte("last surviving copy")
	obj := c.upload(t, data)
	chunk := obj.Chunks[0]
	for id, w := range c.workers {
		if !containsWorker(chunk.Replicas, id) {
			if err := os.WriteFile(filepath.Join(w.dir, chunk.ID), data, 0600); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	for _, id := range chunk.Replicas {
		if err := os.Remove(filepath.Join(c.workers[id].dir, chunk.ID)); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.service.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	obj, _ = c.meta.GetObject(context.Background(), obj.ID)
	c.checkReplicas(t, obj, data)
	c.checkDownload(t, obj, data)
}

func TestFailedRepairRollbackCanBeAdoptedSafely(t *testing.T) {
	c := newTestCluster(t)
	data := []byte("survives failed metadata update and rollback")
	obj := c.upload(t, data)
	chunk := obj.Chunks[0]
	if err := os.Remove(filepath.Join(c.workers[chunk.Replicas[0]].dir, chunk.ID)); err != nil {
		t.Fatal(err)
	}
	for id, w := range c.workers {
		if !containsWorker(chunk.Replicas, id) {
			w.faults.Store(failDelete)
		}
	}
	fs := &failingStore{Store: c.meta, failUpdate: true}
	c.service.store = fs
	if err := c.service.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.service.pendingCleanup) != 0 {
		t.Fatal("live-object repair queued unsafe delayed DELETE")
	}
	fs.failUpdate = false
	for _, w := range c.workers {
		w.faults.Store(0)
	}
	for i := 0; i < 2; i++ {
		if err := c.service.RepairOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		stored, _ := c.meta.GetObject(context.Background(), obj.ID)
		c.checkReplicas(t, stored, data)
		c.checkDownload(t, stored, data)
	}
}

func TestMetadataHTTPAndValidation(t *testing.T) {
	c := newTestCluster(t)
	obj := c.upload(t, []byte("metadata stays separate from data"))
	handler := NewHandler(c.registry, c.service)
	request := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	path := "/api/v1/objects/" + obj.ID
	get := request("GET", path+"/metadata", "")
	var got metadata.Object
	if get.Code != 200 || json.Unmarshal(get.Body.Bytes(), &got) != nil || got.CustomMetadata["owner"] != "alice" {
		t.Fatal("get metadata failed")
	}
	for _, body := range []string{`null`, `{}`, `{"custom_metadata":null}`, `{"custom_metadata":{"x":1}}`, `{"custom_metadata":{},"unknown":1}`, `{"custom_metadata":{}} {}`} {
		if w := request("PATCH", path+"/metadata", body); w.Code != 400 {
			t.Fatalf("patch accepted %q: %d", body, w.Code)
		}
	}
	patch := request("PATCH", path+"/metadata", `{"custom_metadata":{"course":"cloud"}}`)
	if patch.Code != 200 || json.Unmarshal(patch.Body.Bytes(), &got) != nil || got.CustomMetadata["owner"] != "alice" || got.CustomMetadata["course"] != "cloud" || got.ModifiedAt.Before(obj.ModifiedAt) {
		t.Fatal("patch did not merge metadata")
	}
	if w := request("GET", "/api/v1/objects", ""); w.Code != 200 || !bytes.Contains(w.Body.Bytes(), []byte(obj.ID)) {
		t.Fatal("list missing object")
	}
	for _, method := range []string{"GET", "PATCH"} {
		body := ""
		if method == "PATCH" {
			body = `{"custom_metadata":{}}`
		}
		if w := request(method, "/api/v1/objects/missing/metadata", body); w.Code != 404 {
			t.Fatalf("missing metadata %s: %d", method, w.Code)
		}
	}
	if w := request("POST", "/api/v1/objects", "not multipart"); w.Code != http.StatusBadRequest {
		t.Fatal("accepted invalid multipart")
	}
	if _, err := c.meta.GetObject(context.Background(), "missing"); !errors.Is(err, metadata.ErrObjectNotFound) {
		t.Fatal(err)
	}
	c.checkDownload(t, obj, []byte("metadata stays separate from data"))
}
