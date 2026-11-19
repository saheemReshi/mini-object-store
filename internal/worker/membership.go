package worker

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"
)

const (
	heartbeatInterval = 2 * time.Second
	retryInterval     = 2 * time.Second
	requestTimeout    = 3 * time.Second
)

// RunMembership registers, sends heartbeats, and retries until ctx is canceled.
// A controller restart loses its registry; an unknown-worker heartbeat response
// sends this loop back through registration without restarting the worker.
func RunMembership(
	ctx context.Context,
	client *http.Client,
	controllerURL string,
	workerID string,
	advertiseURL string,
) {
	for {
		if ctx.Err() != nil {
			return
		}

		// Register (or re-register) with the controller.
		reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)

		err := Register(
			reqCtx,
			client,
			controllerURL,
			workerID,
			advertiseURL,
		)
		// Release each request's timer now rather than deferring it for the
		// lifetime of this long-running membership goroutine.
		cancel()

		if err != nil {
			log.Printf("registration failed: %v", err)

			if !waitOrDone(ctx, retryInterval) {
				return
			}
			continue
		}

		log.Printf("worker %s registered", workerID)

		// Maintain membership through periodic heartbeats.
		for {
			reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)

			err := Heartbeat(
				reqCtx,
				client,
				controllerURL,
				workerID,
			)
			cancel()

			if errors.Is(err, ErrNotRegistered) {
				log.Printf("controller lost registration for %s", workerID)
				break // Return to the outer registration loop.
			}

			if err != nil {
				log.Printf("heartbeat failed: %v", err)
			}

			if !waitOrDone(ctx, heartbeatInterval) {
				return
			}
		}
	}
}

func waitOrDone(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false

	case <-timer.C:
		return true
	}
}
