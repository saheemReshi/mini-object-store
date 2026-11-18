package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"mini-object-store/internal/storage"
	"mini-object-store/internal/worker"
)

func main() {
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
		log.Fatal("WORKER_ADVERTISE_URL must be set")
	}

	store, err := storage.NewFileStore(dataDir)
	if err != nil {
		log.Fatal(err)
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           worker.NewHandler(store, workerID),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Registration runs independently of the HTTP server.
	go func() {
		client := &http.Client{
			Timeout: 3 * time.Second,
		}

		for {
			ctx, cancel := context.WithTimeout(
				context.Background(),
				3*time.Second,
			)

			err := worker.Register(
				ctx,
				client,
				controllerURL,
				workerID,
				advertiseURL,
			)
			cancel()

			if err == nil {
				log.Printf("worker %s registered", workerID)
				return
			}

			log.Printf("registration failed: %v; retrying", err)
			time.Sleep(2 * time.Second)
		}
	}()

	log.Printf("worker %s listening on %s", workerID, addr)
	log.Fatal(server.ListenAndServe())
}
