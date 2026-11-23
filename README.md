# Mini Distributed Cloud Object Store

A distributed object store written in Go for a Cloud Computing infrastructure
project. One controller splits binary objects into replicated chunks across four
storage workers, verifies data integrity, and repairs missing or corrupted copies.

For a complete presentation walkthrough with commands and expected results, see
[Demo.md](Demo.md). It includes worker failures, corruption, persistence, and
manually requested garbage collection.

## Features

- **Binary object storage:** multipart uploads, downloads, listing, and deletion
  for documents, images, media, archives, and other binary data.
- **Chunking and replication:** decimal 1 MB chunks (1,000,000 bytes), ordered
  reconstruction, a smaller final chunk, and two copies on distinct workers.
  Empty objects have zero chunks.
- **Integrity and failover:** SHA-256 for each chunk and the complete object;
  downloads try another replica when a copy is unavailable or corrupt.
- **Metadata:** unique object IDs, names, sizes, content types, timestamps,
  checksums, ordered chunk manifests, replica locations, and custom string
  key-value pairs. Custom metadata can be patched without uploading data again.
- **Persistent controller state:** SQLite stores object metadata, manifests,
  placements, and deletion tombstones across controller restarts.
- **Worker recovery:** automatic registration, heartbeats, failure detection,
  re-registration, and background restoration of replication.
- **Safe concurrent storage:** atomic file publication, per-object and per-chunk
  locks, and retryable cleanup of failed uploads and incomplete deletions.
- **Manual orphan reclamation:** explicitly requested startup GC with dry-run,
  grace periods, metadata revalidation, and reclaimed-storage reporting.
- **Docker deployment:** one controller, four workers, five persistent volumes,
  unprivileged containers, healthchecks, and graceful shutdown.

## Architecture

~~~mermaid
flowchart TD
    Client["Client: REST / multipart / binary"] -->|HTTP| Controller["Controller"]
    Controller --- Metadata["SQLite: objects, chunks, replicas, tombstones"]
    Controller -->|HTTP chunk operations| W1["Worker 1"]
    Controller -->|HTTP chunk operations| W2["Worker 2"]
    Controller -->|HTTP chunk operations| W3["Worker 3"]
    Controller -->|HTTP chunk operations| W4["Worker 4"]
    W1 --- V1["Worker 1 volume"]
    W2 --- V2["Worker 2 volume"]
    W3 --- V3["Worker 3 volume"]
    W4 --- V4["Worker 4 volume"]
~~~

### Controller

The controller owns the client API, replica placement, metadata, membership,
repair, and maintenance coordination. Clients use object IDs and never need to
contact workers or reconstruct chunks themselves.

Uploads rotate placement across healthy workers. Workers register automatically
and send heartbeats every two seconds; more than six seconds without contact
marks a worker unavailable. Registration alone is not enough for upload placement:
a successful heartbeat makes the worker healthy.

The registry is in memory. Following a controller restart, workers re-register
and rebuild membership while object metadata remains in SQLite.

### Workers and persistent storage

Each worker stores immutable chunks as individual files in its own directory.
A chunk PUT validates its declared size and SHA-256 before publication. Workers
stage writes, sync file contents, atomically publish the chunk, and sync directory
changes. Repeating an identical PUT is accepted; conflicting contents return 409.

Compose gives every worker its own named volume mounted at /data, with chunk
files in /data/chunks. Worker ports are internal to the Compose network; only the
controller is published to the host.

### Metadata

The metadata package separates models and the Store interface from MemoryStore
and SQLiteStore. SQLite persists objects, ordered chunks, and replica placements
using transactions, foreign keys, a write-ahead log, and full synchronous commits.
Replica updates compare the expected placement and reject deleting objects in
the same transaction.

The pure-Go modernc.org/sqlite driver allows static container binaries without a
C compiler or a separate database server. The connection pool has one connection,
which serializes metadata transactions for this single-controller deployment.

MemoryStore remains available for tests and experiments. Selecting it explicitly
discards metadata on controller exit; worker files alone cannot reconstruct the
lost object catalog.

## Data flow and consistency

| Operation | Behavior |
| --- | --- |
| Upload | Generate an object ID, chunk and hash the input, replicate each chunk to two distinct healthy workers, then publish the complete manifest. Failed attempts are queued for cleanup. |
| Download | Read chunks in manifest order, verify each replica response, fail over as needed, then verify the reconstructed size and whole-object SHA-256 in a temporary file before streaming to the client. |
| Metadata patch | Merge custom string key-value pairs and update the modification time without rewriting object data. |
| Delete | Persist a tombstone, remove tracked and stale copies from all four workers, then remove metadata. Incomplete deletion returns 503 and retains the tombstone for an explicit DELETE retry. |
| Repair | Every 15 seconds, verify replicas, restore two copies where a valid source exists, commit placement changes, and reconcile stale extra copies on returning workers. |
| Manual GC | Before enabling object APIs, compare worker inventories with SQLite, wait a grace period, refresh inventories, and recheck ownership before deleting confirmed orphans. |

Per-object RWMutex locks coordinate reconstruction, metadata operations, deletion,
and repair. Per-chunk RWMutex locks serialize worker file operations. Both lock
registries reference-count holders and waiters so entries are removed only when
no operation still uses them. These are process-local locks: each worker directory
must have one owning process.

A separate maintenance lock excludes object operations and repair during GC.
Downloads release their object lock after producing a verified temporary snapshot;
streaming that snapshot can safely continue after the distributed object is deleted.

## Manual startup GC

GC runs only when an operator places a gc.request file beside the metadata
database, or at GC_REQUEST_PATH. Ordinary restarts and worker reconnects do not
trigger collection.

A request selects dry-run or delete mode, a grace period, and a worker-discovery
timeout. Defaults are dry-run, 60 seconds, and 60 seconds. An empty request is also
a dry-run. GC requires an existing SQLite database and four healthy workers.

GC preserves chunks whose owning object exists, including deletion tombstones and
untracked files of live objects. It leaves unfamiliar chunk names and temporary
staging files untouched. Database errors never count as evidence that data is
unreferenced.

During maintenance, object APIs return JSON 503 with Retry-After. Membership and
heartbeats stay available, so workers can register and Compose can start them.
Repair starts after maintenance succeeds. Docker healthchecks cover membership
availability; they do not imply that object APIs are ready during GC.

Successful completion removes and syncs the request file before opening client
traffic. Failures or interruption before completion retain the request; Docker's
restart policy may retry it. Logs report candidates, preserved files, deletions,
estimated reclaimed bytes, deferred candidates, and failures. See
[the GC walkthrough](Demo.md#9-gc-dry-run) for the request format and commands.

## HTTP APIs

### Client API: controller

| Method | Endpoint | Success |
| --- | --- | --- |
| POST | /api/v1/objects | 201; multipart file plus optional metadata JSON string map |
| GET | /api/v1/objects | 200; object ID, name, size, and content type summaries |
| GET | /api/v1/objects/{object_id} | 200; original binary object and its Content-Type |
| GET | /api/v1/objects/{object_id}/metadata | 200; system metadata and custom metadata |
| PATCH | /api/v1/objects/{object_id}/metadata | 200; merge a JSON custom_metadata string map |
| DELETE | /api/v1/objects/{object_id} | 204 after physical deletion completes |

Upload metadata is a plain string map in the multipart metadata field. PATCH
expects a custom_metadata wrapper, as shown in Demo.md. Errors have a JSON error
message: invalid requests return 400, missing objects 404, conflicts 409, internal
failures 500, and unavailable storage or unfinished deletion 503. Unsupported
methods return JSON 405 with an Allow header. Oversized uploads return 413.

### Internal API: trusted controller/worker communication

| Server | Method | Endpoint |
| --- | --- | --- |
| Controller | POST | /internal/workers/register |
| Controller | POST | /internal/workers/heartbeat |
| Controller | GET | /internal/workers |
| Worker | PUT / GET / DELETE | /internal/chunks/{chunk_id} |
| Worker | GET | /internal/workers/{worker_id}/chunks |

Chunk PUT requires a positive Content-Length of at most 1,000,000 and an
X-Chunk-SHA256 header. Inventory lists IDs and sizes; it does not prove integrity.
The controller verifies chunk contents against its metadata when downloading or
repairing.

## Deployment and configuration

Docker Engine with Compose is the primary demonstration environment. The first
build needs network access for base images and Go modules. A basic start is:

~~~bash
docker compose up --build -d --wait
docker compose ps
~~~

The controller is exposed on host port 8080 by default; CONTROLLER_PORT overrides
the Compose host port. Workers communicate using stable service names such as
http://worker1:8081. All five containers run as an unprivileged app user.

Named volumes hold the SQLite database (including WAL/SHM files) and worker data.
Container recreation preserves these volumes; removing volumes destroys the stored
data. [Demo.md](Demo.md) uses an isolated project and documents cleanup separately.

### Controller environment

| Variable | Default | Meaning |
| --- | --- | --- |
| CONTROLLER_ADDR | :8080 | HTTP listen address |
| METADATA_BACKEND | sqlite | Persistent SQLite, or explicitly selected memory |
| METADATA_DB_PATH | ./data/controller/metadata.db | Database path; parent directory is created when needed for normal startup |
| GC_REQUEST_PATH | gc.request beside the database | Optional manual maintenance request; Compose uses /data/gc.request |

### Worker environment

| Variable | Default | Meaning |
| --- | --- | --- |
| WORKER_ID | worker1 | Stable, unique worker identity |
| WORKER_ADDR | :8081 | HTTP listen address |
| WORKER_DATA_DIR | ./data/worker1 | Chunk directory; use a separate directory per worker |
| CONTROLLER_URL | http://localhost:8080 | Controller address reachable by the worker |
| WORKER_ADVERTISE_URL | Required | Worker HTTP address reachable by the controller; no trailing slash |

Advertised addresses differ from listen addresses: listening on all interfaces
does not give the controller a routable worker address. Registration rejects
duplicate advertised address strings and changing an existing ID's address.

## Source organization

~~~text
cmd/controller/          Startup, SQLite selection, GC requests, HTTP maintenance gate
cmd/worker/              Worker startup and shutdown
internal/controller/
  object_service.go     Service definition and dependency wiring
  object_upload.go      Chunking, hashing, and replicated upload
  object_download.go    Verified reconstruction and replica failover
  object_delete.go      Tombstones and physical deletion
  object_metadata.go    Metadata retrieval, listing, and patching
  object_cleanup.go     Failed-upload cleanup and retries
  object_gc.go          Explicit startup orphan reclamation
  object_repair.go      Verification, repair, and stale-copy reconciliation
  object_http.go        Client object HTTP handlers
  membership_http.go    Registration and heartbeat HTTP handlers
  router.go             Route registration
  http_response.go      Shared JSON response handling
  registry.go           Membership and derived worker health
  workerclient.go       Controller-to-worker HTTP requests
  objectlocks.go        Reference-counted object locks
internal/metadata/      Models, Store interface, memory/SQLite implementations, schema
internal/storage/       Chunk files, atomic publication, and reference-counted locks
internal/worker/        Chunk/inventory HTTP handlers and membership client
tests/                  Opt-in real-process and Docker Compose integration tests
Dockerfile              Multi-stage controller and worker images
compose.yaml            Five services and five persistent volumes
Demo.md                 Sequential demonstration commands and expected observations
~~~

## Verification

Requirements for development: Linux, Go 1.27.4 or later, and a working C compiler
for the race detector. Tests use temporary files and localhost listeners.

~~~bash
go test ./... -v -count=1
go test -race ./... -count=1
go vet ./...
~~~

The suite covers chunk boundaries, SHA-256 failures, concurrency, lock lifetimes,
placement, failover, repair, metadata contracts, SQLite persistence, deletion
retries, and GC safety. Real-process and Docker tests are opt-in; their commands
and scope are in [Demo.md](Demo.md#14-automated-verification). They include actual
controller crashes and interruption/recovery of requested GC.

## Limits

- The failed-upload cleanup queue is in memory. A crash can leave published
  orphan chunks; explicit manual GC can reclaim eligible files.
- Incomplete deletions require an explicit DELETE retry, including after restart.
- Repair needs a surviving verified copy. Losing both copies of a chunk is
  unrecoverable.
- This is a fixed four-worker, single-controller system. Deletion and requested
  GC require all four workers; there is no permanent worker-removal policy.
- A valid but incorrect metadata database cannot be identified automatically.
  Operators must select the matching database and worker volumes before GC.
- Local locks do not fence arbitrarily delayed distributed PUTs. Strict
  no-resurrection guarantees would require durable generations/tombstones and an
  operation journal.
- Internal APIs assume a trusted network. Authentication, TLS, and multiple
  controllers sharing a database are outside the current scope.
- Upload requests are limited to 1 GiB including multipart overhead. The
  controller parses/spools multipart input before distribution and reconstructs
  downloads into temporary files, so sufficient temporary disk space is required.
  Request reads have a two-minute deadline; shutdown allows 20 seconds to drain
  requests before force-closing active connections.
