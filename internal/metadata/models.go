package metadata

import (
	"time"
)

// Chunk records one ordered object segment and the worker IDs holding its copies.
// Checksum is a hex-encoded SHA-256 digest of that segment's bytes.
type Chunk struct {
	ID       string   `json:"chunk_id"`
	Index    int      `json:"index"`
	Size     int64    `json:"size"`
	Checksum string   `json:"checksum"`
	Replicas []string `json:"replicas"`
}

// Object combines user metadata with the ordered chunk manifest needed to rebuild
// the data. An empty object has zero chunks and the SHA-256 digest of empty input.
type Object struct {
	ID             string            `json:"object_id"`
	Name           string            `json:"name"`
	Size           int64             `json:"size"`
	ContentType    string            `json:"content_type"`
	CreatedAt      time.Time         `json:"created_at"`
	ModifiedAt     time.Time         `json:"last_modified"`
	Checksum       string            `json:"checksum"`
	ChunkCount     int               `json:"chunk_count"`
	Chunks         []Chunk           `json:"chunks"`
	CustomMetadata map[string]string `json:"custom_metadata"`
	// Deleting is internal retry state, retained until physical deletion succeeds.
	Deleting bool `json:"-"`
}
