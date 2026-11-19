package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"mini-object-store/internal/storage"
)

const workerRequestTimeout = 5 * time.Second

var (
	ErrChunkNotFound = errors.New("chunk not found")
	ErrChunkConflict = errors.New("chunk conflict")
	ErrChunkCorrupt  = errors.New("chunk integrity failure")
)

type WorkerClient struct {
	http *http.Client
}

func NewWorkerClient() *WorkerClient {
	return &WorkerClient{
		http: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
	}
}

// HTTPStatusError preserves an unexpected worker response.
type HTTPStatusError struct {
	Code int
	Body string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf(
		"worker returned HTTP %d: %s", e.Code, e.Body,
	)
}

func statusError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	return &HTTPStatusError{
		Code: resp.StatusCode,
		Body: strings.TrimSpace(string(body)),
	}
}

func chunkURL(address, id string) string {
	return strings.TrimRight(address, "/") +
		"/internal/chunks/" + url.PathEscape(id)
}

// PutChunk uploads one chunk to a worker.
func (c *WorkerClient) PutChunk(
	ctx context.Context,
	address, id string,
	data []byte,
	checksum [32]byte,
) error {
	if len(data) == 0 ||
		int64(len(data)) > storage.ChunkSize ||
		sha256.Sum256(data) != checksum {
		return ErrChunkCorrupt
	}
	// A transport may still read/close the request body after Do returns.
	// Own the bytes so Upload can safely reuse its chunk buffer on any result.
	data = bytes.Clone(data)

	ctx, cancel := context.WithTimeout(ctx, workerRequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPut,
		chunkURL(address, id),
		bytes.NewReader(data),
	)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Chunk-SHA256", hex.EncodeToString(checksum[:]))

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("PUT chunk on %s: %w", address, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated:
		return nil
	case http.StatusConflict:
		return fmt.Errorf("%w: %s", ErrChunkConflict, id)
	default:
		return statusError(resp)
	}
}

// GetChunk downloads one chunk and validates its integrity.
func (c *WorkerClient) GetChunk(
	ctx context.Context,
	address, id string,
	expectedSize int64,
	expectedHash [32]byte,
) ([]byte, error) {
	if expectedSize <= 0 || expectedSize > storage.ChunkSize {
		return nil, ErrChunkCorrupt
	}
	ctx, cancel := context.WithTimeout(ctx, workerRequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, chunkURL(address, id), nil,
	)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET chunk from %s: %w", address, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: %s", ErrChunkNotFound, id)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, statusError(resp)
	}

	// Never accept more than one chunk's worth of data.
	data, err := io.ReadAll(
		io.LimitReader(resp.Body, storage.ChunkSize+1),
	)
	if err != nil {
		return nil, fmt.Errorf("read chunk response: %w", err)
	}

	if int64(len(data)) != expectedSize ||
		sha256.Sum256(data) != expectedHash {
		return nil, fmt.Errorf("%w: %s", ErrChunkCorrupt, id)
	}

	return data, nil
}

// DeleteChunk is idempotent: 204 and 404 both mean absent.
func (c *WorkerClient) DeleteChunk(
	ctx context.Context,
	address, id string,
) error {
	ctx, cancel := context.WithTimeout(ctx, workerRequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx, http.MethodDelete, chunkURL(address, id), nil,
	)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("DELETE chunk on %s: %w", address, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent ||
		resp.StatusCode == http.StatusNotFound {
		return nil
	}

	return statusError(resp)
}

type WorkerInventory struct {
	WorkerID string              `json:"worker_id"`
	Chunks   []storage.ChunkInfo `json:"chunks"`
}

func (c *WorkerClient) ListChunks(
	ctx context.Context,
	address, workerID string,
) (WorkerInventory, error) {
	ctx, cancel := context.WithTimeout(ctx, workerRequestTimeout)
	defer cancel()

	endpoint := strings.TrimRight(address, "/") +
		"/internal/workers/" + url.PathEscape(workerID) + "/chunks"

	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, endpoint, nil,
	)
	if err != nil {
		return WorkerInventory{}, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return WorkerInventory{}, fmt.Errorf(
			"list chunks on %s: %w", address, err,
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return WorkerInventory{}, statusError(resp)
	}

	// Bound the inventory response to 8 MiB.
	const maxInventory = 8 << 20
	data, err := io.ReadAll(
		io.LimitReader(resp.Body, maxInventory+1),
	)
	if err != nil {
		return WorkerInventory{}, err
	}
	if len(data) > maxInventory {
		return WorkerInventory{}, errors.New("inventory too large")
	}

	var inventory WorkerInventory
	if err := json.Unmarshal(data, &inventory); err != nil {
		return WorkerInventory{}, err
	}
	if inventory.WorkerID != workerID {
		return WorkerInventory{}, errors.New("worker ID mismatch")
	}

	return inventory, nil
}
