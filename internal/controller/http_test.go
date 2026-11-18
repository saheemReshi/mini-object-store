package controller

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestRegisterAndList(t *testing.T) {
	reg := NewRegistry()
	handler := NewHandler(reg)

	send := func(id, address string) int {
		t.Helper()

		data, _ := json.Marshal(map[string]string{
			"worker_id": id,
			"address":   address,
		})

		req := httptest.NewRequest(
			http.MethodPost,
			"/internal/workers/register",
			bytes.NewReader(data),
		)

		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		return resp.Code
	}

	if got := send("worker1", "http://localhost:8081"); got != 201 {
		t.Fatalf("new registration: %d", got)
	}

	if got := send("worker1", "http://localhost:8081"); got != 200 {
		t.Fatalf("duplicate registration: %d", got)
	}

	if got := send("worker1", "http://localhost:8082"); got != 409 {
		t.Fatalf("conflicting registration: %d", got)
	}

	if got := send("../../x", "http://localhost:8083"); got != 400 {
		t.Fatalf("invalid registration: %d", got)
	}

	if len(reg.List()) != 1 {
		t.Fatal("expected exactly one registered worker")
	}
}

func TestConcurrentRegistration(t *testing.T) {
	reg := NewRegistry()

	var wg sync.WaitGroup
	results := make(chan bool, 40)

	for i := 0; i < 40; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, created, err := reg.Register(
				"worker1",
				"http://localhost:8081",
			)
			if err != nil {
				t.Errorf("registration: %v", err)
			}

			results <- created
		}()
	}

	wg.Wait()
	close(results)

	createdCount := 0
	for created := range results {
		if created {
			createdCount++
		}
	}

	if createdCount != 1 || len(reg.List()) != 1 {
		t.Fatalf("created %d workers, want 1", createdCount)
	}
}

func TestWorkerHealth(t *testing.T) {
	registry := NewRegistry()

	// Register worker.
	_, _, err := registry.Register(
		"worker1",
		"http://localhost:8081",
	)
	if err != nil {
		t.Fatal(err)
	}

	if got := registry.List()[0].Status; got != "registered" {
		t.Fatalf("expected registered, got %s", got)
	}

	// First heartbeat makes it healthy.
	if err := registry.Heartbeat("worker1"); err != nil {
		t.Fatal(err)
	}

	if got := registry.List()[0].Status; got != "healthy" {
		t.Fatalf("expected healthy, got %s", got)
	}

	// Simulate a worker that hasn't contacted us recently.
	registry.mu.Lock()
	w := registry.workers["worker1"]
	w.LastContact = time.Now().Add(
		-WorkerFailureTimeout - time.Second,
	)
	registry.workers["worker1"] = w
	registry.mu.Unlock()

	if got := registry.List()[0].Status; got != "unavailable" {
		t.Fatalf("expected unavailable, got %s", got)
	}

	// A fresh heartbeat restores healthy status.
	if err := registry.Heartbeat("worker1"); err != nil {
		t.Fatal(err)
	}

	if got := registry.List()[0].Status; got != "healthy" {
		t.Fatalf("expected healthy, got %s", got)
	}

	// Unknown workers cannot send heartbeats.
	if err := registry.Heartbeat("unknown"); !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("expected ErrWorkerNotFound, got %v", err)
	}
}
