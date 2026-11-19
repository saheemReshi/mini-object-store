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
| Metadata | SQLite database file (`metadata.Store`); an in-memory store remains for tests |
| Deployment | Local binaries, or Docker Compose with five containers and five named volumes |

Upload metadata is published only after every chunk has two acknowledged copies.
Empty objects have zero chunks. Downloads try another replica if a worker fails or
returns corrupt data, then verify the reconstructed object in a temporary file
before returning bytes to the client.

Workers register automatically and send heartbeats every two seconds. A worker
is reported unavailable after more than six seconds without contact. The
controller runs a repair scan every 15 seconds to restore replication where a
verified source is available and remove stale extra copies from healthy workers.
Detection and repair also take time to complete worker HTTP requests.

**Controller metadata survives restarts.** Object metadata, chunk manifests,
replica placement, custom metadata and deletion tombstones are stored in a SQLite
file (`METADATA_DB_PATH`). After a controller restart, workers re-register on
their next heartbeat and existing objects download normally. The worker registry
itself stays in memory and is rebuilt by that re-registration.

The in-memory backend (`METADATA_BACKEND=memory`) is still available for quick
experiments and is used by most unit tests. With it, **metadata is lost when the
controller exits**: worker files stay on disk but can no longer be retrieved.

SQLite driver choice: [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite),
a pure-Go translation of SQLite. It needs no C compiler or system SQLite library,
so both binaries build with `CGO_ENABLED=0` as static executables and the container
images stay simple. The trade-off is a larger dependency and somewhat slower
queries than the cgo driver, which is irrelevant at this project's scale.

## Run locally

Requirements: Linux, Bash, Go 1.27.4 or later (see `go.mod`), and free ports
8080 through 8084. The API examples also use `curl`, Python 3, and GNU coreutils
(`sha256sum` and `cmp`). No database server is required: SQLite runs inside the
controller process. The only external Go module is the SQLite driver.

After cloning, or whenever `go.sum` is missing driver entries, fetch modules once
(requires network access):

```bash
go mod tidy
```

Run all commands from the repository root. Build both binaries into the ignored
`tmp/` directory:

```bash
mkdir -p tmp/bin
go build -o tmp/bin/controller ./cmd/controller
go build -o tmp/bin/worker ./cmd/worker
```

Start the controller in one terminal:

```bash
CONTROLLER_ADDR=127.0.0.1:8080 \
METADATA_DB_PATH=./data/controller/metadata.db \
./tmp/bin/controller
```

The database file and its parent directory are created if missing. If the
location cannot be opened (for example, it is below a regular file or not
writable), the controller exits with an error naming `METADATA_DB_PATH`; it never
falls back to in-memory metadata. Stop the controller with Ctrl+C (or SIGTERM) to
close the database cleanly; SQLite's write-ahead log also makes an abrupt kill
safe for committed metadata.

Database paths are literal filenames: spaces and reserved characters such as
`?`, `#`, and `%` do not become SQLite connection parameters. Quote paths with
spaces when setting `METADATA_DB_PATH` in a shell.

On shutdown, the controller gives active HTTP requests up to 20 seconds to finish.
If that deadline expires, it closes active connections to cancel their requests
before waiting for repair to stop. This lets downloads release object locks that
repair may be waiting for; interrupted requests may need to be retried.

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

## Run with Docker Compose

Requirements: Docker Engine with the Compose v2 plugin (`docker compose`). The
images are built from source, so the first build needs network access to pull
`golang` and `alpine` base images and Go modules (`go.sum` must be up to date; see
`go mod tidy` above).

```bash
docker compose config -q          # validate compose.yaml
docker compose up --build -d --wait
docker compose ps                 # all five services should be "healthy"
curl --fail --silent --show-error http://127.0.0.1:8080/internal/workers | python3 -m json.tool
```

`--wait` blocks until every container's healthcheck passes. Wait for all four
workers to report `healthy` in `/internal/workers` before uploading. The
upload/download/metadata examples below work unchanged against the Compose stack
because the controller is published on host port 8080 (override with
`CONTROLLER_PORT=9090 docker compose up -d`).

| Service | Image target | Listens on | Advertised / internal URL | Volume |
| --- | --- | --- | --- | --- |
| `controller` | `controller` | `:8080` (published) | `http://controller:8080` | `controller_data:/data` (holds `metadata.db`, `-wal`, `-shm`) |
| `worker1`–`worker4` | `worker` | `:8081` (not published) | `http://workerN:8081` | `workerN_data:/data` (chunks in `/data/chunks`) |

Containers talk to each other by Compose service name; workers listen on all
interfaces inside their container so the controller can reach them. Each service
has its own named volume, so the same internal path and port are safe to reuse.
Containers run as an unprivileged `app` user, and the binaries are PID 1 so
`docker stop` delivers SIGTERM for a clean shutdown.

Stopping and starting:

```bash
docker compose stop               # stop containers, keep them
docker compose start
docker compose down               # remove containers and network; volumes are KEPT
docker compose up -d --wait       # new containers reattach to the same volumes
docker compose down -v            # ALSO deletes every volume, i.e. all stored objects
```

Never use `down -v` when you want data to survive. `docker volume ls` shows the
five `mini-object-store_*` volumes.

### Demonstrations with containers

Use an uploaded object's ID (see the examples below) in `OBJECT_ID`.

Worker failure and recovery. Downloads keep working from the other replica; within
about 25 seconds repair copies affected chunks to a spare worker. When the worker
returns, its disk is intact and its now-extra copies are removed by reconciliation:

```bash
docker compose stop worker2
curl -fsS "http://127.0.0.1:8080/api/v1/objects/$OBJECT_ID" | sha256sum
sleep 30
curl -fsS "http://127.0.0.1:8080/api/v1/objects/$OBJECT_ID/metadata" | python3 -m json.tool
docker compose start worker2
docker compose exec worker2 ls -l /data/chunks
```

Corruption. Overwrite the first bytes of a stored chunk (pick a chunk ID and one
of its replica workers from the metadata). The download still verifies, and the
next repair scan restores two verified copies:

```bash
CHUNK_ID=...; W=worker1
docker compose exec -T "$W" sh -c "printf '\377\377\377\377' | dd of=/data/chunks/$CHUNK_ID bs=1 count=4 conv=notrunc"
docker compose exec -T "$W" sha256sum "/data/chunks/$CHUNK_ID"
curl -fsS "http://127.0.0.1:8080/api/v1/objects/$OBJECT_ID" | sha256sum
```

Controller restart and container recreation. Metadata comes back from the
`controller_data` volume and chunks from the worker volumes:

```bash
docker compose restart controller
docker compose down && docker compose up -d --wait
curl -fsS "http://127.0.0.1:8080/api/v1/objects/$OBJECT_ID/metadata" | python3 -m json.tool
```

Logs: `docker compose logs -f controller` or `docker compose logs worker3`.

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
(With Compose, worker ports are not published; use
`docker compose exec worker1 wget -qO- http://127.0.0.1:8081/internal/workers/worker1/chunks`.)

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
The tombstone is stored in SQLite, so the retry also works after a controller
restart.
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
| `METADATA_BACKEND` | `sqlite` | `sqlite`, or `memory` for non-persistent metadata |
| `METADATA_DB_PATH` | `./data/controller/metadata.db` | SQLite database file; its directory also holds `-wal`/`-shm` files |

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
  sqlite.go, schema.sql    SQLite implementation and its schema
  sqlite_driver.go         Selected SQLite driver (modernc.org/sqlite)
  contract_test.go         Store contract tests run against both backends
internal/storage/          Chunk files, atomic publication, and chunk locks
internal/worker/           Chunk HTTP handlers and membership client
tests/process_test.go      Opt-in test of the actual distributed binaries
tests/compose_test.go      Opt-in test of the Docker Compose deployment
Dockerfile                 Multi-stage build with controller and worker targets
compose.yaml               One controller, four workers, five named volumes
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
OBJECTSTORE_PROCESS_E2E=1 go test ./tests -run '^TestMultiProcess$' -v -count=1 -timeout=5m
```

This test builds and starts one controller and four workers, uploads random binary
data larger than 2 MB, checks SHA-256 and physical replication, stops/restarts a
worker, verifies automatic repair, corrupts a replica, patches metadata, deletes
the object, and then checks SQLite persistence: after both a graceful (SIGTERM)
and a crash (SIGKILL) controller restart, workers re-register and the object's
metadata, patch and download are intact; repair still works; an incomplete
deletion's tombstone survives a restart and an explicit DELETE retry completes
it; and an unusable `METADATA_DB_PATH` fails startup. It takes about 65 seconds
because it uses real heartbeat and repair intervals. It manages only its own
processes, ports, database file and temporary data.

The metadata package runs one contract suite (`contract_test.go`) against both
MemoryStore and SQLite, plus SQLite-only tests that close and reopen a real
database file, check for leftover manifest rows, and reject unusable files.

The Compose test needs a Docker daemon and is also opt-in:

```bash
OBJECTSTORE_COMPOSE_E2E=1 go test ./tests -run '^TestComposeStack$' -v -count=1 -timeout=20m
```

It builds the images and starts the stack under a unique Compose project name and
a free host port, so it never touches your own `mini-object-store` containers or
volumes. It checks stable worker IDs, a >2 MB upload (ordered chunks, final
partial chunk, two distinct replicas, whole-object checksum), download, worker
stop with repair, worker restart with stale-copy reconciliation, a corrupted
replica, metadata CRUD, controller restart, full container recreation
(`down` + `up`), and an incomplete deletion that survives a controller restart
before an explicit retry removes every chunk. Cleanup runs `down -v` for the test
project only; set `OBJECTSTORE_COMPOSE_KEEP=1` to leave it running.

## Current limits and planned work

- Persistence covers controller metadata, not every in-flight operation. The
  failed-upload cleanup queue is still in memory: if the controller dies between
  writing chunks and publishing (or cleaning up) an upload, those chunks remain
  as unreferenced files. Startup deliberately does not erase unknown worker files.
- Tombstoned deletions are not resumed automatically after a restart; they are
  hidden and must be finished with an explicit DELETE retry.
- Repair needs a surviving verified copy. Losing every copy of a chunk is
  unrecoverable; normal replication protects against one replica loss.
- Permanent worker removal and changing a registered worker's address need an
  additional membership policy; deletion expects all four workers.
- Local locking and cancellation do not fence arbitrarily delayed distributed
  PUTs. Durable worker tombstones/generations and a persistent cleanup journal
  would be needed for a strict no-resurrection guarantee.
- The SQLite store uses one database connection, so metadata operations are
  serialized. That is ample for one controller and keeps compare-and-swap simple;
  it is not designed for several controllers sharing one database.
- Internal HTTP APIs assume a trusted deployment; authentication and TLS are not
  implemented. Each storage directory must belong to one worker process.

See [REVIEW.md](REVIEW.md) for the detailed correctness review, verified results,
remaining failure models, and the persistence contract.
