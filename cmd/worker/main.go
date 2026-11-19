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
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	client := &http.Client{}

	go worker.RunMembership(
		context.Background(),
		client,
		controllerURL,
		workerID,
		advertiseURL,
	)

	log.Printf("worker %s listening on %s", workerID, addr)
	log.Fatal(server.ListenAndServe())
}
