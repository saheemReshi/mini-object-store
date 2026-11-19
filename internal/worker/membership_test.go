package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"mini-object-store/internal/controller"
)

func TestMembershipReregistersAfterControllerRestart(t *testing.T) {
	var registry atomic.Pointer[controller.Registry]
	registry.Store(controller.NewRegistry())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		controller.NewHandler(registry.Load(), nil).ServeHTTP(w, r)
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunMembership(ctx, srv.Client(), srv.URL, "worker", "http://localhost:8999")
	}()
	defer func() { cancel(); <-done }()
	waitHealthy := func() {
		t.Helper()
		deadline := time.Now().Add(8 * time.Second)
		for {
			workers := registry.Load().List()
			if len(workers) == 1 && workers[0].Status == "healthy" {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("membership did not become healthy")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitHealthy()
	registry.Store(controller.NewRegistry())
	waitHealthy()
}
