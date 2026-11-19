package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

var ErrNotRegistered = errors.New("controller does not recognize worker")

// Register advertises this worker's reachable HTTP base URL. Retrying the same
// ID/address pair is accepted; callers supply the request deadline through ctx.
func Register(
	ctx context.Context,
	client *http.Client,
	controllerURL string,
	id string,
	address string,
) error {
	payload, err := json.Marshal(struct {
		WorkerID string `json:"worker_id"`
		Address  string `json:"address"`
	}{
		WorkerID: id,
		Address:  address,
	})
	if err != nil {
		return err
	}

	endpoint := strings.TrimRight(controllerURL, "/") +
		"/internal/workers/register"

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		endpoint,
		bytes.NewReader(payload),
	)
	if err != nil {
		return fmt.Errorf("build registration request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated &&
		resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))

		return fmt.Errorf(
			"registration rejected (%s): %s",
			resp.Status,
			strings.TrimSpace(string(body)),
		)
	}

	return nil
}

// Heartbeat refreshes membership. ErrNotRegistered means the caller must register
// again, rather than continuing to send heartbeats to an empty controller registry.
func Heartbeat(
	ctx context.Context,
	client *http.Client,
	controllerURL string,
	workerID string,
) error {
	payload, err := json.Marshal(struct {
		WorkerID string `json:"worker_id"`
	}{
		WorkerID: workerID,
	})
	if err != nil {
		return err
	}

	endpoint := strings.TrimRight(controllerURL, "/") +
		"/internal/workers/heartbeat"

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		endpoint,
		bytes.NewReader(payload),
	)
	if err != nil {
		return fmt.Errorf("build heartbeat request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send heartbeat: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil

	case http.StatusNotFound:
		return ErrNotRegistered

	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf(
			"heartbeat rejected (%s): %s",
			resp.Status,
			strings.TrimSpace(string(body)),
		)
	}
}
