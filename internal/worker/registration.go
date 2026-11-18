package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

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
