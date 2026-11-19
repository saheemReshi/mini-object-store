package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"mini-object-store/internal/controller"
	"mini-object-store/internal/metadata"
)

const defaultMetadataDBPath = "./data/controller/metadata.db"

func main() {
	if err := run(); err != nil {
		log.Printf("controller: %v", err)
		os.Exit(1)
	}
}

// openStore selects the metadata backend. SQLite is the default; the in-memory
// store must be requested explicitly and is never used as a silent fallback.
func openStore() (metadata.Store, io.Closer, error) {
	backend := strings.ToLower(strings.TrimSpace(os.Getenv("METADATA_BACKEND")))
	switch backend {
	case "", "sqlite":
		path := os.Getenv("METADATA_DB_PATH")
		if path == "" {
			path = defaultMetadataDBPath
		}
		store, err := metadata.OpenSQLiteStore(path)
		if err != nil {
			return nil, nil, fmt.Errorf("open metadata database (METADATA_DB_PATH=%q): %w", path, err)
		}
		log.Printf("metadata: SQLite database %s", path)
		return store, store, nil
	case "memory":
		log.Printf("metadata: IN-MEMORY store; metadata is lost when the controller exits")
		return metadata.NewMemoryStore(), nopCloser{}, nil
	default:
		return nil, nil, fmt.Errorf("unknown METADATA_BACKEND %q (use sqlite or memory)", backend)
	}
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

func shutdownServer(ctx context.Context, server *http.Server) error {
	if err := server.Shutdown(ctx); err != nil {
		// Shutdown stops waiting when ctx expires but leaves active connections
		// open. Close them to cancel request contexts and release object locks
		// before run waits for a repair goroutine blocked behind those requests.
		return errors.Join(err, server.Close())
	}
	return nil
}

func run() error {
	addr := os.Getenv("CONTROLLER_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	metaStore, closer, err := openStore()
	if err != nil {
		return err
	}
	defer func() {
		if err := closer.Close(); err != nil {
			log.Printf("close metadata store: %v", err)
		}
	}()

	// SIGTERM is what `docker stop` sends; SIGINT is Ctrl+C.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	registry := controller.NewRegistry()
	workerClient := controller.NewWorkerClient()

	objectService := controller.NewObjectService(
		registry,
		metaStore,
		workerClient,
	)

	// Background replication and integrity repair, stopped before the store
	// is closed.
	var repair sync.WaitGroup
	repair.Add(1)
	go func() {
		defer repair.Done()
		objectService.RunRepair(ctx, 15*time.Second)
	}()

	server := &http.Server{
		Addr:              addr,
		Handler:           controller.NewHandler(registry, objectService),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       2 * time.Minute,
		IdleTimeout:       60 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("controller listening on %s", addr)
		serveErr <- server.ListenAndServe()
	}()

	select {
	case err = <-serveErr:
		// Listener failed (for example, port in use).
	case <-ctx.Done():
		log.Printf("controller shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		err = shutdownServer(shutdownCtx, server)
		cancel()
	}
	stop() // Also stops repair if the listener failed.
	repair.Wait()

	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}
