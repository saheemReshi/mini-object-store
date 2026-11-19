package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mini-object-store/internal/storage"
	"mini-object-store/internal/worker"
)

func main() {
	if err := run(); err != nil {
		log.Printf("worker: %v", err)
		os.Exit(1)
	}
}

func run() error {
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = "worker1"
	}

	dataDir := os.Getenv("WORKER_DATA_DIR")
	if dataDir == "" {
		dataDir = "./data/worker1"
	}

	addr := os.Getenv("WORKER_ADDR")
	if addr == "" {
		addr = ":8081"
	}

	controllerURL := os.Getenv("CONTROLLER_URL")
	if controllerURL == "" {
		controllerURL = "http://localhost:8080"
	}

	advertiseURL := os.Getenv("WORKER_ADVERTISE_URL")
	if advertiseURL == "" {
		return errors.New("WORKER_ADVERTISE_URL must be set")
	}

	store, err := storage.NewFileStore(dataDir)
	if err != nil {
		return err
	}

	// SIGTERM is what `docker stop` sends; SIGINT is Ctrl+C.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	server := &http.Server{
		Addr:              addr,
		Handler:           worker.NewHandler(store, workerID),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	client := &http.Client{}

	go worker.RunMembership(
		ctx,
		client,
		controllerURL,
		workerID,
		advertiseURL,
	)

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("worker %s listening on %s (data %s)", workerID, addr, dataDir)
		serveErr <- server.ListenAndServe()
	}()

	select {
	case err = <-serveErr:
	case <-ctx.Done():
		log.Printf("worker %s shutting down", workerID)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = server.Shutdown(shutdownCtx)
		cancel()
	}
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}
