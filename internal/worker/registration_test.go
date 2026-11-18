package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"mini-object-store/internal/controller"
)

func TestWorkerRegistration(t *testing.T) {
	registry := controller.NewRegistry()

	server := httptest.NewServer(
		controller.NewHandler(registry),
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
