package metadata

import (
	"time"
)

type Chunk struct {
	ID       string   `json:"chunk_id"`
	Index    int      `json:"index"`
	Size     int64    `json:"size"`
	Checksum string   `json:"checksum"`
	Replicas []string `json:"replicas"`
}

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
	Deleting       bool              `json:"-"`
}
