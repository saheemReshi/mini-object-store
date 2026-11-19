package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestShutdownServerForcesCloseAfterDeadline(t *testing.T) {
	// Model a download holding an object read lock while repair waits for the
	// write lock. Canceling the HTTP request must release both operations.
	var object sync.RWMutex
	started := make(chan struct{})
	handlerDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		object.RLock()
		defer object.RUnlock()
		defer close(handlerDone)
		close(started)
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		if response, err := server.Client().Do(request); err == nil {
			response.Body.Close()
		}
	}()
	waitForSignal(t, started, "request to acquire its read lock")

	repairStarted := make(chan struct{})
	repairDone := make(chan struct{})
	go func() {
		close(repairStarted)
		object.Lock()
		object.Unlock()
		close(repairDone)
	}()
	waitForSignal(t, repairStarted, "repair to start waiting")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := shutdownServer(ctx, server.Config); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown should preserve its deadline error, got %v", err)
	}
	waitForSignal(t, handlerDone, "forced shutdown to cancel the active request")
	waitForSignal(t, repairDone, "repair to acquire the released object lock")
	waitForSignal(t, clientDone, "client to observe the closed connection")
}

func TestShutdownServerDrainsRequestsBeforeDeadline(t *testing.T) {
	started := make(chan struct{})
	finish := make(chan struct{})
	stopping := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-finish:
			_, _ = io.WriteString(w, "complete")
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(server.Close)
	server.Config.RegisterOnShutdown(func() { close(stopping) })

	type result struct {
		status int
		body   string
		err    error
	}
	clientDone := make(chan result, 1)
	go func() {
		response, err := server.Client().Get(server.URL)
		if err != nil {
			clientDone <- result{err: err}
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		clientDone <- result{status: response.StatusCode, body: string(body), err: err}
	}()
	waitForSignal(t, started, "request to start")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- shutdownServer(ctx, server.Config) }()
	waitForSignal(t, stopping, "graceful shutdown to begin")
	select {
	case response := <-clientDone:
		t.Fatalf("shutdown interrupted an active request before its deadline: %+v", response)
	default:
	}
	close(finish)
	select {
	case response := <-clientDone:
		if response.err != nil || response.status != http.StatusOK || response.body != "complete" {
			t.Fatalf("graceful response: %+v", response)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request did not finish within the grace period")
	}
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("graceful shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("graceful shutdown did not finish after the request completed")
	}
}

func waitForSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}
