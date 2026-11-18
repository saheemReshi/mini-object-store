package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"mini-object-store/internal/controller"
	"mini-object-store/internal/metadata"
)

func main() {
	addr := os.Getenv("CONTROLLER_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	registry := controller.NewRegistry()
	metaStore := metadata.NewMemoryStore()

	workerClient := controller.NewWorkerClient()

	objectService := controller.NewObjectService(
		registry,
		metaStore,
		workerClient,
	)

	server := &http.Server{
		Addr:              addr,
		Handler:           controller.NewHandler(registry, objectService),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("controller listening on %s", addr)
	log.Fatal(server.ListenAndServe())
}
