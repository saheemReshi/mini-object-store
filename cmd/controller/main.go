package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"mini-object-store/internal/controller"
)

func main() {
	addr := os.Getenv("CONTROLLER_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	registry := controller.NewRegistry()

	server := &http.Server{
		Addr:              addr,
		Handler:           controller.NewHandler(registry),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("controller listening on %s", addr)
	log.Fatal(server.ListenAndServe())
}
