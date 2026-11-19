# Mini Distributed Cloud Object Store

A Go implementation of a university Cloud Computing infrastructure project.
Clients upload arbitrary binary objects to a controller, which splits them into
chunks and stores replicated copies on four HTTP storage workers.

## Architecture

| Component | Current behavior |
| --- | --- |
| Controller | One process providing the client API, metadata, placement, membership, and repair |
| Workers | Four processes, each with its own directory of chunk files |
| Chunk size | 1 MB = 1,000,000 bytes; the final chunk may be smaller |
| Replication | Two copies of each chunk on distinct registered workers |
| Integrity | SHA-256 per chunk and for the complete object |
| Metadata | In-memory `metadata.Store` implementation; SQLite is not implemented |
| Deployment | Local binaries; Docker/Compose deployment remains to be added |

Upload metadata is published only after every chunk has two acknowledged copies.
Empty objects have zero chunks. Downloads try another replica if a worker fails or
returns corrupt data, then verify the reconstructed object in a temporary file
before returning bytes to the client.

Workers register automatically and send heartbeats every two seconds. A worker
is reported unavailable after more than six seconds without contact. The
controller runs a repair scan every 15 seconds to restore replication where a
verified source is available and remove stale extra copies from healthy workers.
Detection and repair also take time to complete worker HTTP requests.

**Controller metadata does not survive a restart.** Worker files remain on disk,
but existing objects cannot be retrieved through a new empty metadata store.
Workers re-register automatically; that does not reconstruct object metadata.

## Run locally

Requirements: Linux, Bash, Go 1.27.4 or later (see `go.mod`), and free ports
8080 through 8084. The API examples also use `curl`, Python 3, and GNU coreutils
(`sha256sum` and `cmp`). No external Go modules or database server are required.

Run all commands from the repository root. Build both binaries into the ignored
`tmp/` directory:

```bash
mkdir -p tmp/bin
go build -o tmp/bin/controller ./cmd/controller
go build -o tmp/bin/worker ./cmd/worker
```

Start the controller in one terminal:

```bash
CONTROLLER_ADDR=127.0.0.1:8080 ./tmp/bin/controller
```

Open four more terminals. In each one, run the following with `i` set to a
different value: **1, 2, 3, or 4**.

```bash
i=1
WORKER_ID="worker$i" \
WORKER_ADDR="127.0.0.1:$((8080 + i))" \
WORKER_DATA_DIR="./data/worker$i" \
CONTROLLER_URL="http://127.0.0.1:8080" \
WORKER_ADVERTISE_URL="http://127.0.0.1:$((8080 + i))" \
./tmp/bin/worker
```

Each process owns a separate directory. Reuse the same worker ID, address, and
directory when restarting it. Stop a process with Ctrl+C in its terminal; stopping
a worker does not remove its stored files. If you change ports, update the listen
addresses, advertised URLs, and client commands consistently.

In another terminal, check that all four workers report `healthy` before uploading:

```bash
curl --fail --silent --show-error http://127.0.0.1:8080/internal/workers \
  | python3 -m json.tool
```

Registration alone reports `registered`; the first successful heartbeat makes the
worker healthy. Uploads need at least two healthy workers. Completing deletion
uses the fixed four-worker deployment, including stale copies from earlier repair.

## Upload, download, and metadata examples

Run these examples in the same Bash terminal so `demo_dir` and `OBJECT_ID` remain
available. Create a random binary file larger than 2 MB in a temporary directory:

```bash
demo_dir=$(mktemp -d /tmp/mini-object-store-demo.XXXXXX)
python3 -c 'import os, pathlib, sys; pathlib.Path(sys.argv[1]).write_bytes(os.urandom(2_345_679))' \
  "$demo_dir/original.bin"
```

Upload the file with optional custom metadata. Upload metadata is a plain JSON
object whose values are strings. The response contains the generated object ID,
whole-object checksum, ordered chunks, and replica worker IDs.

```bash
curl --fail --silent --show-error \
  -F "file=@$demo_dir/original.bin;type=application/octet-stream" \
  -F 'metadata={"owner":"alice","course":"cloud"}' \
  http://127.0.0.1:8080/api/v1/objects \
  -o "$demo_dir/upload.json"

OBJECT_ID=$(python3 -c 'import json, sys; print(json.load(sys.stdin)["object_id"])' \
  < "$demo_dir/upload.json")
python3 -m json.tool "$demo_dir/upload.json"
```

Download and compare both the SHA-256 digests and the actual bytes. The two digest
values should match; `cmp` returns success without output when the files match.

```bash
curl --fail --silent --show-error \
  "http://127.0.0.1:8080/api/v1/objects/$OBJECT_ID" \
  -o "$demo_dir/downloaded.bin"
sha256sum "$demo_dir/original.bin" "$demo_dir/downloaded.bin"
cmp "$demo_dir/original.bin" "$demo_dir/downloaded.bin"
```

List objects, retrieve metadata, and merge a metadata patch without re-uploading
the data. PATCH uses a `custom_metadata` wrapper, unlike the upload form field.

```bash
curl --fail --silent --show-error http://127.0.0.1:8080/api/v1/objects \
  | python3 -m json.tool

curl --fail --silent --show-error \
  "http://127.0.0.1:8080/api/v1/objects/$OBJECT_ID/metadata" \
  | python3 -m json.tool

curl --fail --silent --show-error -X PATCH \
  -H 'Content-Type: application/json' \
  -d '{"custom_metadata":{"owner":"bob","experiment":"replication"}}' \
  "http://127.0.0.1:8080/api/v1/objects/$OBJECT_ID/metadata" \
  | python3 -m json.tool
```

Inspect physical chunk inventory by contacting a worker directly. For example,
worker1 listens on port 8081; inventory reports IDs and sizes, not verified hashes.

```bash
curl --fail --silent --show-error \
  http://127.0.0.1:8081/internal/workers/worker1/chunks \
  | python3 -m json.tool
```

Delete the demo object with all four workers running:

```bash
curl --fail --silent --show-error -i -X DELETE \
  "http://127.0.0.1:8080/api/v1/objects/$OBJECT_ID"
```

A successful deletion returns 204. If a worker cannot acknowledge removal, the
controller returns 503 and retains a deletion tombstone. The object is hidden from
listing, download, and metadata updates. Restore the worker and retry the same
DELETE; background repair does not resume tombstoned deletions automatically.
The temporary original, download, and upload response remain in `demo_dir` for
inspection after the distributed object is deleted.

## HTTP API

Client routes are served by the controller:

| Method | Route | Successful response |
| --- | --- | --- |
| POST | `/api/v1/objects` | 201; object metadata from multipart `file` and optional `metadata` fields |
| GET | `/api/v1/objects` | 200; `{"objects":[...]}` with object summaries |
| GET | `/api/v1/objects/{object_id}` | 200; original binary object |
| GET | `/api/v1/objects/{object_id}/metadata` | 200; complete object metadata |
| PATCH | `/api/v1/objects/{object_id}/metadata` | 200; updated metadata after merging `custom_metadata` |
| DELETE | `/api/v1/objects/{object_id}` | 204; all chunk copies removed and metadata deleted |

Errors use JSON with an `error` message. Missing objects return 404; unavailable
storage, corrupt/unrecoverable object data, and incomplete deletions return 503.
The upload request limit is 1 GiB including multipart overhead. File parts are
parsed/spooled before distribution, and the controller's request-read deadline
is two minutes.

Internal routes are for trusted controller/worker communication:

| Server | Method | Route |
| --- | --- | --- |
| Controller | POST | `/internal/workers/register` |
| Controller | POST | `/internal/workers/heartbeat` |
| Controller | GET | `/internal/workers` (diagnostic membership view) |
| Worker | PUT / GET / DELETE | `/internal/chunks/{chunk_id}` |
| Worker | GET | `/internal/workers/{worker_id}/chunks` |

Chunk PUT requires a known positive `Content-Length` no larger than 1,000,000 and
an `X-Chunk-SHA256` header containing the hex digest. Repeating the same valid PUT
is accepted; different bytes under an existing chunk ID return 409. A missing
chunk DELETE returns 404, which the controller worker client treats as successful
absence when retrying cleanup.

## Configuration

Controller environment:

| Variable | Default | Meaning |
| --- | --- | --- |
| `CONTROLLER_ADDR` | `:8080` | HTTP listen address |

Worker environment:

| Variable | Default | Meaning |
| --- | --- | --- |
| `WORKER_ID` | `worker1` | Unique stable worker ID |
| `WORKER_ADDR` | `:8081` | HTTP listen address |
| `WORKER_DATA_DIR` | `./data/worker1` | Chunk directory; set a distinct directory per worker |
| `CONTROLLER_URL` | `http://localhost:8080` | Controller base URL |
| `WORKER_ADVERTISE_URL` | Required | Worker URL reachable by the controller, with no trailing slash |

The advertised URL is separate from the listen address: a server may listen on
all interfaces, but the controller needs a concrete reachable hostname/address.
Do not register two IDs for the same worker endpoint. Current registration accepts
HTTP URLs and rejects an existing ID with a different address.

## Source organization

```text
cmd/controller/             Controller process setup
cmd/worker/                 Worker process setup
internal/controller/
  object_service.go        Service definition and dependency wiring
  object_upload.go         Chunking and replicated upload
  object_download.go       Verified reconstruction and replica failover
  object_delete.go         Tombstones and physical deletion
  object_metadata.go       Metadata reads, listing, and patching
  object_cleanup.go        Failed-upload cleanup and retries
  object_repair.go         Replica checking, repair, and reconciliation
  object_http.go           Client-facing object handlers
  membership_http.go       Worker registration/heartbeat handlers
  router.go                HTTP route registration
  http_response.go         Shared JSON responses
  registry.go              Worker membership and derived health
  workerclient.go          Controller-to-worker HTTP requests
  objectlocks.go           Reference-counted object locks
internal/metadata/
  models.go                Object and chunk data structures
  store.go                 Store interface and shared errors
  memory.go                In-memory implementation
internal/storage/          Chunk files, atomic publication, and chunk locks
internal/worker/           Chunk HTTP handlers and membership client
tests/process_test.go      Opt-in test of the actual distributed binaries
```

Per-object locks coordinate download reconstruction, metadata updates, deletion,
and repair. Per-chunk locks coordinate file operations. Both lock registries count
waiters as well as holders, and remove entries once no operation references them.
Workers publish verified chunks atomically and sync file contents and directory
changes; these locks apply within one process, not across workers sharing a disk.

## Tests

Tests use temporary directories and ephemeral localhost ports, so no pre-existing
controller or worker services are required. A sandbox must allow local listeners.
The race test requires a working C compiler and Go race-detector support.

```bash
gofmt -w cmd internal tests
go test ./... -v -count=1
go test -race ./... -count=1
go vet ./...
```

The regular suite covers boundary-size uploads, replica placement, corruption,
failover, metadata CRUD, lock lifetimes, concurrent operations, failed uploads,
repair rollback, and deletion retries. The real-process test is skipped unless
explicitly enabled:

```bash
OBJECTSTORE_PROCESS_E2E=1 go test ./tests -run '^TestMultiProcess$' -v -count=1 -timeout=120s
```

This test builds and starts one controller and four workers, uploads random binary
data larger than 2 MB, checks SHA-256 and physical replication, stops/restarts a
worker, verifies automatic repair, corrupts a replica, patches metadata, deletes
the object, and checks controller restart behavior. It takes about 50 seconds
because it uses real heartbeat and repair intervals. It manages only its own
processes and temporary data, and cleans them up automatically.

## Current limits and planned work

- SQLite persistence and Docker/Compose deployment are not implemented.
- Controller restart loses metadata, deletion tombstones, and pending failed-upload
  cleanup. Remaining worker files are not automatically erased or reconstructed
  into objects.
- Repair needs a surviving verified copy. Losing every copy of a chunk is
  unrecoverable; normal replication protects against one replica loss.
- Incomplete deletion requires an explicit DELETE retry. Permanent worker removal
  and changing a registered worker's address need an additional membership policy.
- Local locking and cancellation do not fence arbitrarily delayed distributed
  PUTs. Durable worker tombstones/generations and a persistent cleanup journal
  would be needed for a strict no-resurrection guarantee.
- Internal HTTP APIs assume a trusted deployment; authentication and TLS are not
  implemented. Each storage directory must belong to one worker process.

The future SQLite implementation should preserve `metadata.Store`, including
atomic publication, detached snapshots, tombstone filtering, and transactional
replica compare-and-swap. See [REVIEW.md](REVIEW.md) for the detailed correctness
review, verified results, remaining failure models, and persistence contract.
