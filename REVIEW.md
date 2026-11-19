# Correctness review and verification

Reviewed against the supplied assignment and the complete Go source/test tree on
2026-11-19. No dependencies, database implementation, or Git history changes were
introduced. The existing formatting change in
`internal/worker/registration_test.go` was preserved.

## Actual architecture

`cmd/controller` wires a registry, `metadata.Store`, a worker HTTP client, and an
object service. It runs repair every 15 seconds. `cmd/worker` serves a `FileStore`
and maintains registration/heartbeats every two seconds. Worker availability is
derived from last contact, with a six-second failure threshold.

Upload HTTP requests are parsed as multipart data (spooled to temporary disk for
large uploads), then read in 1,000,000-byte chunks. Two replica requests run in
parallel for each chunk; the next chunk waits for both. Objects are published to
metadata only after all writes succeed. Empty objects have no physical chunks.

Downloads verify each chunk and the complete object into a temporary file before
sending any object bytes to the caller. Per-object locks serialize mutation and
repair against reconstruction. Once reconstruction finishes, deletion can proceed
while the caller reads its independent temporary snapshot.

Workers stage chunk data, validate size/SHA-256, sync the file, hard-link the
immutable chunk into place, remove the temporary name, and sync the directory.
Both lock tables count holders and waiters before acquiring their RWMutex and
remove entries only after releasing the mutex and dropping the last reference.
That reference-counting algorithm was already correct and was preserved.

An open chunk descriptor remains attached to its inode after unlink on Linux.
Thus `Get` only needs to hold the read lock through opening the immutable file;
it does not need to retain a lock until the network transfer finishes.

## Findings, ordered by severity

| Severity | Finding | Resolution |
| --- | --- | --- |
| High | An HTTP transport can retain a request body after `Do` returns. The next upload iteration could reuse the backing byte buffer while transport work still reads it. | Each worker PUT owns a bounded copy of its bytes. A retaining-transport regression test verifies independence after an early error. |
| High | Failed-upload cleanup was forgotten when a worker could not acknowledge deletion. | Retain pending targets in a synchronized in-memory queue and retry during repair. Each pass has a cleanup time budget; normal metadata is never published for failed uploads. |
| High | Repair abandoned a chunk if both recorded copies were gone, even when a surviving untracked copy existed. | Search healthy workers for a verified source and publish repaired placement atomically. |
| High | Reads and retry verification followed symlinks or could block on a FIFO. Existing-file hashing was unbounded. | Open without following symlinks, open nonblocking before checking file type, require regular files, and bound verification reads. The storage directory itself must remain trusted. |
| High | A staged PUT was outside the chunk lock and had no cancellation check before publication. A cleanup DELETE could overtake staging. | Serialize staging/publication with deletion; check request cancellation before staging and publication. This does not provide distributed operation fencing; see limitations. |
| High | Two worker IDs could advertise the same endpoint and satisfy the logical replication count using one server. | Reject duplicate advertised address strings. DNS/IP aliases and physical machine independence still require correct deployment configuration. |
| Medium | Repair required a spare worker and could not restore a corrupt/missing replica with only the two original healthy workers available. | Prefer a spare, but allow deleting a verified corrupt copy and restoring it from the good source. Never overwrite a valid conflicting chunk through normal PUT. |
| Medium | Repair left corrupt/extra copies on disk, including stale replicas on returning workers. | After two copies are verified and recorded, delete untracked copies on healthy workers. Later scans retry after workers return. |
| Medium | Repair rollback only handled a successful replacement followed by metadata failure. An unsuccessful PUT could still have published a replica. | Track ambiguous destinations for immediate rollback. Never delete restored original locations after a metadata error. Failed rollbacks remain discoverable during reconciliation. Live-object repairs never enqueue delayed deletes, which could otherwise destroy an adopted replica. |
| Medium | Deletion stopped before doing any physical work when any worker was unhealthy. | Delete reachable copies and retain the tombstone until all four registered workers acknowledge absence. A retry completes the deletion. |
| Medium | Registry snapshots exposed the internal heartbeat pointer. | Copy the pointed-to timestamp before returning records. |
| Medium | Download failover discarded caller cancellation; upload replication formatting discarded nested causes. | Preserve cancellation and wrapped errors. Per-worker timeouts still allow failover when the parent context remains live. |
| Medium | Body uploads had no server read deadline. | Set worker read/write deadlines to 15 seconds, controller request reads to two minutes, and idle deadlines to one minute. |
| Low | Retrying an unlink after directory-sync failure could report durable absence without retrying the sync. | Sync the directory before acknowledging an already-absent chunk. |
| Low | Worker redirects could silently change the replica destination; invalid expected chunk sizes were accepted by the client API. | Reject redirects and expected sizes outside 1..1,000,000. |

No compilation errors or incorrect imports were found. No reference-counted-lock
cleanup defect or race was found in the original lock algorithm. The new tests
cover lock waiters, contention, and map cleanup without replacing that design.

## Changed files

| Files | Purpose |
| --- | --- |
| `cmd/controller/main.go`, `cmd/worker/main.go` | Bound HTTP request reads and idle connections; bound worker response writes. |
| `internal/controller/upload.go` | Retry orphan cleanup, check cancellation before reading each chunk, preserve error causes. |
| `internal/controller/download.go` | Stop failover promptly on parent cancellation and return its cause. |
| `internal/controller/workerclient.go` | Own upload body bytes; reject redirects and invalid expected sizes. |
| `internal/controller/registry.go` | Endpoint uniqueness and detached heartbeat snapshots. |
| `internal/controller/repair.go` | Recover untracked sources, support repair without a spare, reconcile stale replicas, and roll back uncertain writes. |
| `internal/controller/objects.go` | Make progress on partial deletion while retaining deletion intent. |
| `internal/storage/filestore.go` | Safe regular-file reads, bounded verification, serialized/context-aware staging, absent-delete directory sync. |
| `internal/worker/http.go` | Pass the HTTP request context to storage writes. |
| `internal/metadata/store.go` | Document existing Store semantics; method signatures are unchanged. |
| `internal/controller/correctness_test.go`, `integration_test.go`, `recovery_test.go` | Client ownership/cancellation, lock lifecycle, metadata API validation, fault-injected four-worker integration and recovery. |
| `internal/storage/correctness_test.go` | Invalid data, unsafe paths/files, cancellation, waiting lock lifetime, concurrent PUT/GET/DELETE/List. |
| `internal/metadata/store_test.go` | Deep copies, concurrent replica CAS, tombstone rules, and cancellation. |
| `internal/worker/membership_test.go`, `validation_test.go` | Re-registration after controller state loss and rejected HTTP chunk writes. |
| `tests/process_test.go` | Opt-in real binary/process test with automatic cleanup. |
| `README.md`, `REVIEW.md` | Accurate verification commands, review results, and limitations. |

## Verification

The first uncached full-suite attempt inside the sandbox failed because localhost
socket creation was blocked. Storage and metadata tests passed in that attempt.
HTTP tests were then run with the approved localhost-listener permission.

Final verification against the completed source passed:

| Command | Actual result |
| --- | --- |
| `gofmt -w cmd internal tests` | Completed successfully. |
| `go test ./... -v -count=1 -cover` | All regular tests passed. The real-process test was explicitly skipped in this invocation because it is opt-in. |
| `go test -race ./... -count=1` | All regular tests passed; no race reports. The opt-in process test is run separately. |
| `go vet ./...` | Exit 0; no findings. |
| `git diff --check` | Exit 0; no whitespace errors. |
| `OBJECTSTORE_PROCESS_E2E=1 go test ./tests -run '^TestMultiProcess$' -v -count=1 -timeout=120s` | Passed twice; final run completed in 48.78 seconds after rebuilding the final binaries. |

Final statement coverage: controller 85.9%, metadata 90.5%, storage 85.7%, worker
74.8%. Command entrypoints have no unit-test coverage; they were exercised by the
real-process test. Its child binaries were ordinary builds, so their execution is
not included in the coverage percentages or instrumented by the parent race run.

No requested Go checks remain unrun. Docker/container and actual power-loss/disk
fault tests were not performed, as detailed below.

The real-process test passed its first run in 47.31 seconds. Its final run used
SHA-256 `b9c020346a02f5167da8e6ffeac87b61a836008755214892da44295abd483a64`
for all three verified downloads. It built the actual
binaries, started a controller and four workers, and verified:

1. All workers become healthy through real membership requests.
2. A 2,345,679-byte random binary object forms three chunks, with two distinct
   replicas per chunk distributed across all four workers.
3. SHA-256 matches before failure, with one worker stopped, and with one replica
   intentionally corrupted (same-sized corruption, not just truncation).
4. Automatic failure repair changes metadata to two working replicas.
5. A restarted worker retains its disk and stale copies are reconciled away.
6. Automatic corruption repair restores two valid physical copies.
7. Metadata retrieval, listing, and merging patches succeed.
8. Deletion removes all object data from all four directories; subsequent GET is
   404.
9. Restarting the controller loses metadata as designed, workers re-register, and
   their existing data remains on disk.

The regular suite covers empty/small/exactly-1-MB/multi-chunk uploads; partial
chunks and ordering; replica placement and verification; unavailable/corrupt
replica failover; background repair; no-spare repair; recovery from an untracked
source; failed metadata publication; failed repair rollback and safe adoption;
failed uploads and input errors; incomplete deletion/retry; concurrent
download/delete/repair; chunk operations; locks; and metadata CRUD.

## Remaining limits and untested failure models

- **Partial restart persistence:** with the SQLite backend, object metadata,
  placement and deletion tombstones survive restarts (see "SQLite and container
  deployment" below). Registry entries are rebuilt by worker re-registration.
  Pending failed-upload cleanup is still in memory, so a controller crash during
  an upload can orphan chunks. Startup deliberately does not erase unknown worker
  files; that would destroy potentially recoverable data.
- **No distributed fencing:** cancellation can race publication, or a delayed PUT
  may start after cleanup DELETE. Local locking/context checks cannot eliminate
  this failure model. Durable worker tombstones or generation tokens plus a
  persistent upload/cleanup journal are needed for a hard no-resurrection
  guarantee. The tests verify lost acknowledgements after publication and retries,
  not every possible delayed-message ordering.
- **Manual deletion retry:** a tombstoned object is hidden from listing and cannot
  be downloaded/patched/repaired. DELETE must be retried with its ID. Store listing
  excludes tombstones, so the repair loop cannot enumerate and resume them.
- **Fixed membership:** deletion still requires four registered workers and sends
  DELETE to all of them to catch stale copies. Permanent worker removal, changed
  addresses, and replacing a worker under a new ID need an explicit membership
  policy. Registration compares address strings, not DNS aliases or physical
  failure domains. Internal APIs assume a trusted local deployment.
- **Replication factor two:** loss/corruption of every surviving copy is
  unrecoverable. Repair reports per-chunk trouble through logs; a successful
  `RepairOnce` return means the scan ran, not that all objects are healthy.
- **Bounded but coarse operations:** worker RPCs have five-second deadlines;
  cleanup attempts use ten-second budgets. RWMutex acquisition is not context
  interruptible. A generic blocking `io.Reader` cannot be interrupted by checking
  context alone. HTTP body deadlines bound network reads in the binaries.
- **Capacity and latency:** HTTP uploads are limited to 1 GiB including multipart
  overhead and parsed before distribution. Downloads use a full temporary file so
  corrupt data is never sent as a partial successful response. Slow uploads must
  fit the configured two-minute read deadline. Upload placement uses a healthy
  snapshot; a worker failure aborts and cleans that upload rather than retrying on
  another worker within the same request.
- **Crash/disk behavior:** hard-link publication and directory fsync ordering were
  reviewed, but actual power loss, ENOSPC, fsync failures, inode exhaustion, and
  disk hardware errors were not fault-injected. Abrupt worker death during staging
  can leave hidden temporary files; these are excluded from inventory and are not
  automatically swept. Only one worker process should own each storage directory.
- **Deployment:** a Dockerfile and `compose.yaml` now exist (see below). Both
  binaries shut down their HTTP servers on SIGTERM/SIGINT and the controller stops
  repair and closes the database first; there is still no coordinated
  cluster-wide shutdown.

## SQLite and container deployment (roadmap milestones 1–5)

### Decisions

- **Driver:** `modernc.org/sqlite` (pure Go). Chosen before the container build
  so that `CGO_ENABLED=0` works: both binaries are static, the build needs no C
  toolchain or `libsqlite3`, and the runtime image needs no C library. A cgo
  driver would have required a matching build/runtime libc.
- **Schema:** `internal/metadata/schema.sql`, embedded and applied idempotently
  (`CREATE ... IF NOT EXISTS`) on every start; version recorded in
  `PRAGMA user_version`, and a database from a newer schema is refused. Tables:
  `objects` (custom metadata as JSON, `deleting` flag), `chunks` (unique index and
  chunk ID within an object), `replicas` (ordered `slot`, unique worker per
  chunk). Timestamps are RFC 3339 with nanoseconds in UTC.
- **Concurrency:** one pooled connection, so all transactions in the controller
  are serialized. Compare-and-swap in `UpdateChunkReplicas` (existence, tombstone,
  ordered placement, replacement) runs in one transaction; result sets are closed
  before the next query in the same transaction. Every Store call is bounded by
  a 10-second operation timeout (including the wait for the connection), and
  `busy_timeout` bounds waits on locks held by another process. Write
  transactions never span worker HTTP requests.
- **Durability:** WAL journal with `synchronous=FULL`; the whole `/data`
  directory is the volume so `-wal`/`-shm` files persist with the database.
- **Atomicity:** every mutation is one transaction that rolls back on any error,
  including a canceled context; duplicate IDs are detected inside the
  transaction. `DeleteObject` removes replica and chunk rows explicitly as well
  as via `ON DELETE CASCADE`.
- **Controller wiring:** `METADATA_BACKEND` (`sqlite` default, `memory` only when
  requested) and `METADATA_DB_PATH` (default `./data/controller/metadata.db`).
  Open/schema failures stop startup; there is no silent fallback.
- **Images:** one multi-stage Dockerfile with `controller` and `worker` targets on
  Alpine, non-root user, exec-form entrypoint (signals reach the binary),
  busybox `wget` healthchecks. Compose: five services, five named volumes,
  service-name URLs, `restart: unless-stopped`, workers start after the
  controller is healthy and still retry registration on their own.

### Verification

| Check | Result |
| --- | --- |
| `go test ./internal/metadata -race` | Contract suite passes for MemoryStore and SQLite (round trip incl. empty objects and ns timestamps, duplicates/missing IDs, detached snapshots, sorted listing hiding tombstones, patch merge, tombstone rejection, ordered CAS, 8-way concurrent CAS with exactly one winner, canceled operations change nothing). SQLite-only: close/reopen of a real file keeps objects, patches, placement and tombstones; deletion leaves no manifest rows; rejected creates leave no rows; unusable/garbage/newer-schema files fail to open. |
| `go test ./... -count=1`, `go test -race ./... -count=1`, `go vet ./...`, `gofmt -l` | All pass; no race reports; no findings. |
| `OBJECTSTORE_PROCESS_E2E=1 ... TestMultiProcess` | Passed (~65 s). Original scenarios plus: graceful and SIGKILL controller restarts with metadata, patch and SHA-256 download intact; repair after restart; incomplete deletion tombstone persisted across restart and completed by DELETE retry; unusable `METADATA_DB_PATH` rejected at startup. |
| `docker compose config -q` | Valid. |
| `OBJECTSTORE_COMPOSE_E2E=1 ... TestComposeStack` | Written to cover every scenario in roadmap milestone 5; must be run on a machine with a Docker daemon. |

The tests above were run in an environment without network access or a Docker
daemon: the Go checks used a temporary stand-in for the SQLite driver (the same
SQL running on SQLite 3.53 through a minimal local binding), and images were not
built. After `go mod tidy`, re-run the Go checks with the real driver and run
`docker compose up --build` and the Compose test before submission.

### Recovery follow-ups

| Follow-up | Status |
| --- | --- |
| Tombstones persisted; explicit DELETE retry after restart | Implemented and tested. |
| Automatic resumption of pending deletions | Not implemented. Needs a separate recovery listing; `ListObjects` deliberately hides tombstones and `Store` was not changed. |
| Durable failed-upload journal | Not implemented. `pendingCleanup` is still in memory; a controller crash during an upload can leave unreferenced chunk files. |
| Delayed PUT fencing | Not implemented. Needs worker-side generations or tombstones; metadata persistence cannot order remote writes. |

Persistence therefore does **not** make every upload or deletion crash-safe.

## Suggested commit messages

- `fix: preserve chunk integrity and reconcile replica recovery`
- `test: cover object-store failure recovery and real process lifecycle`

For the SQLite and deployment work:

1. `feat(metadata): add transactional SQLite store and contract tests`
2. `feat(controller): persist metadata across controller restarts`
3. `build: add controller and worker container images`
4. `feat(deploy): add Compose stack with persistent volumes`
5. `test: verify container failure recovery and database persistence`
6. `docs: document SQLite setup and container demonstrations`
