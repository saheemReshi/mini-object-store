package worker

import (
	"context"
	"errors"
	"mini-object-store/internal/controller"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWorkerRegistration(t *testing.T) {
	registry := controller.NewRegistry()

	server := httptest.NewServer(
		controller.NewHandler(registry, nil),
	)
	defer server.Close()

	client := &http.Client{}

	for i := 0; i < 2; i++ {
		err := Register(
			context.Background(),
			client,
			server.URL,
			"worker1",
			"http://localhost:8081",
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	if got := len(registry.List()); got != 1 {
		t.Fatalf("expected 1 worker, got %d", got)
	}
}

func TestHeartbeatClient(t *testing.T) {
	registry := controller.NewRegistry()

	server := httptest.NewServer(
		controller.NewHandler(registry, nil),
	)
	defer server.Close()

	client := &http.Client{}
	ctx := context.Background()

	// Heartbeat before registration must fail.
	err := Heartbeat(ctx, client, server.URL, "worker1")
	if !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("expected missing registration, got %v", err)
	}

	// Register.
	err = Register(
		ctx,
		client,
		server.URL,
		"worker1",
		"http://localhost:8081",
	)
	if err != nil {
		t.Fatal(err)
	}

	// Heartbeat should now succeed.
	err = Heartbeat(ctx, client, server.URL, "worker1")
	if err != nil {
		t.Fatal(err)
	}

	workers := registry.List()

	if len(workers) != 1 || workers[0].Status != "healthy" {
		t.Fatalf("unexpected workers: %+v", workers)
	}
}
