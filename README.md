# Mini Distributed Cloud Object Store

A distributed object storage system implemented in Go.

## Architecture
- 1 controller
- 4 storage workers
- 1 MB chunks
- Replication factor: 2
- HTTP REST APIs
- Local Go processes (Docker deployment remains to be added)

## Features

Multipart uploads, verified downloads with replica failover, metadata CRUD,
worker membership and failure detection, and background replica repair.
Metadata and failed-upload cleanup records are in memory. Restarting the
controller loses object metadata; worker chunk files remain on disk.

## Verification

Use the Go toolchain required by `go.mod`. Tests use temporary directories and
ephemeral localhost ports; they do not require four pre-existing worker services.

```bash
gofmt -w cmd internal tests
go test ./... -v -count=1
go test -race ./... -count=1
go vet ./...
```

The opt-in test below builds the actual binaries and starts one controller and
four workers. It takes about 50 seconds because it exercises the real heartbeat
and repair intervals. All child processes and data are cleaned up automatically.

```bash
OBJECTSTORE_PROCESS_E2E=1 go test ./tests -run '^TestMultiProcess$' -v -count=1 -timeout=120s
```

See [REVIEW.md](REVIEW.md) for the review findings, verification results,
remaining limitations, and the contract for future SQLite integration.
