package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"mini-object-store/internal/storage"
	"mini-object-store/internal/worker"
)

func main() {
	dataDir := os.Getenv("WORKER_DATA_DIR")
	if dataDir == "" {
		dataDir = "./data/worker1"
	}

	addr := os.Getenv("WORKER_ADDR")
	if addr == "" {
		addr = ":8081"
	}

	store, err := storage.NewFileStore(dataDir)
	if err != nil {
		log.Fatal(err)
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           worker.NewHandler(store),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("worker listening on %s", addr)

	log.Fatal(server.ListenAndServe())
}
