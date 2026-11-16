
package storage

import (
    "fmt"
    "os"
)

type FileStore struct {
    rootDir string
}

func NewFileStore(rootDir string) (*FileStore, error) {
    err := os.MkdirAll(rootDir, 0700)
    if err != nil {
        return nil, fmt.Errorf("create storage directory: %w", err)
    }

    return &FileStore{
        rootDir: rootDir,
    }, nil
}
