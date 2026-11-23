# Demo guide

Run the numbered steps **in order, in one Bash terminal, from the repository
root**. Each step says what to do and what you should observe.

The long setup and verification functions are now in
[scripts/demo_helpers.sh](scripts/demo_helpers.sh). You load them once; you do not
need to copy their implementation. The actual storage, failure, corruption, and
GC actions remain visible below.

You need Docker with Compose, curl, jq, Python 3, and GNU coreutils. Allow about
5–10 minutes after the first image build. If a command reports an error or a wait
times out, stop and inspect the logs before moving on.

| Steps | What you demonstrate |
| --- | --- |
| 1 | One controller and four healthy workers |
| 2–5 | Upload, chunking, replication, verified downloads, boundary sizes, concurrency, metadata, and JSON errors |
| 6–8 | Worker failure, automatic repair, corruption recovery, and persistent storage |
| 9–10 | Manual GC: dry-run, maintenance responses, late workers, crash recovery, and no automatic repeat |
| 11–13 | Persistent deletion tombstones, GC protection, deletion retry, and physical cleanup |
| 14 and appendices | Automated checks, local startup, and cancelling a pending GC request |

## 1. Start the system

Load the helpers, create an isolated demo session, and start the containers:

~~~bash
source scripts/demo_helpers.sh
demo_init

dc up --build -d --wait
wait_for "four workers healthy" all_healthy
wait_for "object API ready" api_ready
dc ps
~~~

**Expected:** five healthy containers.

The helper prints a unique project name, an available host port, and a temporary
directory. It leaves other Compose projects alone. Keep this terminal open:
later commands reuse these values.

| Name | Meaning |
| --- | --- |
| dc | Short for docker compose targeting only this demo project |
| API | Controller URL |
| DEMO_DIR | Host directory containing input files, downloads, and responses |
| OBJECT_ID | Main uploaded object's ID, set in step 2 |
| meta | Fetch metadata for the main object |
| inventory worker1 | List chunk files recorded by worker1 |
| wait_for | Retry a readiness/check command for up to three minutes |

If Docker reports that the chosen port was taken, clean up this project using
step 13 and start a new session. The free-port probe cannot reserve the port
through Docker startup.

## 2. Upload a binary file

Create the demo files, then upload the large one with custom metadata:

~~~bash
make_demo_files

curl -fsS -D "$DEMO_DIR/upload.headers" \
    -F "file=@$DEMO_DIR/original.bin;type=application/octet-stream" \
    -F 'metadata={"owner":"alice","course":"cloud"}' \
    "$API/api/v1/objects" -o "$DEMO_DIR/upload.json"

OBJECT_ID=$(jq -er '.object_id' "$DEMO_DIR/upload.json")
cat "$DEMO_DIR/upload.headers"
jq '{object_id, name, size, content_type, created_at, last_modified,
     checksum, chunk_count, custom_metadata}' "$DEMO_DIR/upload.json"
check_upload_manifest
~~~

**Expected:** HTTP 201 and a new object ID. The file has 2,345,679 bytes, so its
three ordered chunks must contain **1,000,000 + 1,000,000 + 345,679 bytes**.
The helper checks these sizes and the whole-object SHA-256.

## 3. Show distribution and two physical replicas

Display the chunk locations, then inspect each worker's inventory:

~~~bash
meta | jq '.chunks[] | {index, chunk_id, size, replicas}'

for w in worker1 worker2 worker3 worker4; do
    echo "$w"
    inventory "$w" | jq .
done

wait_for "two verified physical copies per chunk" verify_replicas
~~~

**Expected:** all four workers participate, and each chunk has two copies on
different workers.

verify_replicas reads the actual file hashes from worker containers and compares
them with metadata. It also rejects extra copies; it does not just trust the
replica list.

## 4. Download files and check concurrency

Download the main object and compare it with the original:

~~~bash
curl -fsS -D "$DEMO_DIR/download.headers" \
    "$API/api/v1/objects/$OBJECT_ID" -o "$DEMO_DIR/download.bin"

cat "$DEMO_DIR/download.headers"
sha256sum "$DEMO_DIR/original.bin" "$DEMO_DIR/download.bin"
cmp "$DEMO_DIR/original.bin" "$DEMO_DIR/download.bin"
~~~

**Expected:** HTTP 200, the original content type and size, matching SHA-256
values, and no output from cmp.

Now run the boundary-size and concurrent-download demonstrations:

~~~bash
demo_boundaries
demo_parallel_downloads
~~~

**Expected:** the boundary table shows an empty file with zero chunks, small text
with one chunk, and an exactly 1 MB file with one chunk. Each downloads unchanged.
Four simultaneous downloads of the main object also match the original.

## 5. Read and update metadata; show errors

List objects and retrieve the main object's complete metadata:

~~~bash
curl -fsS "$API/api/v1/objects" | jq .
meta | jq .
~~~

Patch custom metadata without uploading the data again:

~~~bash
curl -fsS -X PATCH -H 'Content-Type: application/json' \
    -d '{"custom_metadata":{"owner":"bob","experiment":"replication"}}' \
    "$API/api/v1/objects/$OBJECT_ID/metadata" | jq .

check_metadata_patch
check_download after-patch
~~~

**Expected:** owner changes to bob, course remains cloud, experiment is added,
and last_modified changes. The checksum and downloaded bytes remain unchanged.
Upload accepts a plain metadata map; PATCH uses the custom_metadata wrapper.

Show the three intentional error cases:

~~~bash
demo_errors
~~~

**Expected:** JSON errors for a missing object (404), unsupported method (405,
with an Allow header), and a non-string metadata value (400).

## 6. Stop a worker and watch repair

Pick a worker holding one of the main object's replicas, then stop it:

~~~bash
VICTIM=$(meta | jq -r '.chunks[0].replicas[0]')
dc stop "$VICTIM"

check_download worker-down
wait_for "worker failure detected" worker_unavailable
curl -fsS "$API/internal/workers" | jq .
~~~

**Expected:** the download still matches, and the stopped worker becomes
unavailable after more than six seconds without contact.

Wait for new replicas on the remaining workers, then bring the old worker back:

~~~bash
wait_for "replication restored without the stopped worker" verify_replicas "$VICTIM"
meta | jq '.chunks[] | {chunk_id, replicas}'

dc start "$VICTIM"
wait_for "returning worker healthy" all_healthy
wait_for "stale extra copies removed" verify_replicas
~~~

**Expected:** every chunk again has two verified copies. Repair uses a spare
worker while the victim is down, then removes stale extra copies when it returns.
The repair loop runs every 15 seconds; the checks wait for the result.

## 7. Corrupt a replica and watch recovery

Select a recorded replica. Overwrite its first four bytes without changing its
length; this exercises checksum detection.

~~~bash
CHUNK_ID=$(meta | jq -r '.chunks[0].chunk_id')
CORRUPT_WORKER=$(meta | jq -r '.chunks[0].replicas[0]')
EXPECTED_SHA=$(meta | jq -r '.chunks[0].checksum')

dc exec -T "$CORRUPT_WORKER" sh -c \
    'printf "\377\377\377\377" | dd of="$1" bs=1 count=4 conv=notrunc 2>/dev/null' \
    sh "/data/chunks/$CHUNK_ID"

ACTUAL_SHA=$(dc exec -T "$CORRUPT_WORKER" sha256sum "/data/chunks/$CHUNK_ID" | cut -d ' ' -f 1)
echo "Expected:  $EXPECTED_SHA"
echo "Corrupted: $ACTUAL_SHA"
[[ "$ACTUAL_SHA" != "$EXPECTED_SHA" ]]

check_download corrupt-replica
wait_for "two valid replicas restored" verify_replicas
dc logs --no-color --tail 30 controller
~~~

**Expected:** the physical replica's hash changes, but the download still matches.
Logs show detection and repair. Repair may fix the replica before the download
reaches it; either way, the valid copy protects the object.

## 8. Prove SQLite and worker volumes survive restarts

Save metadata, kill the controller, and start it again:

~~~bash
meta > "$DEMO_DIR/before-restart.json"

dc kill -s SIGKILL controller
dc start controller
wait_for "workers re-registered" all_healthy
wait_for "controller ready" api_ready

check_metadata_persisted
check_download after-crash
~~~

**Expected:** the full metadata snapshot and downloaded bytes are unchanged.

Now remove and recreate the containers while keeping their volumes:

~~~bash
dc down
dc up -d --wait
wait_for "recreated workers healthy" all_healthy
wait_for "recreated controller ready" api_ready

check_download recreated
wait_for "physical replicas persisted" verify_replicas
meta | jq '{object_id, custom_metadata, checksum}'
~~~

**Expected:** object data and custom metadata survive container recreation.
**Do not add -v to dc down here:** that would delete the volumes.

## 9. GC dry-run

Create two orphan files: chunk files with no owning object metadata. This is a
controlled crash-state fixture, not a claim that a real upload was interrupted
at that exact point. Automated tests cover the upload failure boundary.

~~~bash
ORPHAN_CHUNK=obj-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee-0
for w in worker1 worker2; do
    dc exec -T "$w" sh -c 'printf orphan > "$1"' sh "/data/chunks/$ORPHAN_CHUNK"
done
ORPHAN_OWNER="${ORPHAN_CHUNK%-0}"
expect_http 404 "$API/api/v1/objects/$ORPHAN_OWNER/metadata"
~~~

Request a dry-run. The arguments are mode, grace period, and worker-wait timeout:

~~~bash
dc stop controller
request_gc dry-run 5s 60s
dc start controller

show_maintenance
finish_gc

for w in worker1 worker2; do
    dc exec -T "$w" sha256sum "/data/chunks/$ORPHAN_CHUNK"
done
dc logs --no-color --tail 20 controller
~~~

**Expected:** object APIs temporarily return JSON 503 with Retry-After, while
membership still responds. Logs show two candidates, zero deletions, and zero
reclaimed bytes. Both orphan files remain; the completed request file is removed.

These are shortened demo timings. Defaults are dry-run, a 60-second grace period,
and a 60-second worker-wait timeout. An empty request file also means dry-run.

## 10. Delete orphans and recover interrupted GC

Stop worker4 so GC must wait. This creates a predictable maintenance window.
Complete the next block within the five-minute worker-wait timeout.

~~~bash
dc stop controller worker4
request_gc delete 5s 300s
dc start controller

show_maintenance
dc exec -T controller cat /data/gc.request
~~~

Kill the controller while its request is pending, then restart it and the worker:

~~~bash
dc kill -s SIGKILL controller
dc start controller
show_maintenance
dc exec -T controller cat /data/gc.request

dc start worker4
wait_for "late worker registered" all_healthy
finish_gc
~~~

Verify that the two orphan files disappeared and the committed object survived:

~~~bash
for w in worker1 worker2; do
    dc exec -T "$w" sh -c 'test ! -e "$1"' sh "/data/chunks/$ORPHAN_CHUNK"
done
dc logs --no-color --tail 25 controller
check_download after-gc
wait_for "committed replicas preserved" verify_replicas
~~~

**Expected:** the request survives SIGKILL, includes the late worker, and reclaims
two six-byte orphan files: **12 estimated bytes**. Normal object data is preserved.

Finally, show that ordinary restarts and worker reconnects do not request GC:

~~~bash
RUNS_BEFORE=$(gc_request_count)
dc restart controller
wait_for "controller workers healthy" all_healthy
wait_for "controller ready" api_ready
dc restart worker1
wait_for "reconnected worker healthy" all_healthy
check_gc_not_repeated "$RUNS_BEFORE"
~~~

## 11. Delete with a worker down, then retry

Stop a replica worker and attempt deletion:

~~~bash
VICTIM=$(meta | jq -r '.chunks[0].replicas[0]')
dc stop "$VICTIM"

expect_http 503 "$API/api/v1/objects/$OBJECT_ID" -X DELETE
check_object_hidden
~~~

**Expected:** deletion removes reachable copies but returns 503 and retains a
tombstone: a saved "deletion in progress" record. The object disappears from listing.

Restart the controller to prove that this deletion state is persistent:

~~~bash
dc restart controller
wait_for "object listing available" api_ready
expect_http 503 "$API/api/v1/objects/$OBJECT_ID/metadata"

dc start "$VICTIM"
wait_for "all workers returned" all_healthy
~~~

Before retrying DELETE, show that GC preserves the incomplete deletion:

~~~bash
dc stop controller
request_gc delete 2s 60s
dc start controller
finish_gc
wait_for "workers healthy" all_healthy

expect_http 503 "$API/api/v1/objects/$OBJECT_ID/metadata"
dc logs --no-color --tail 15 controller
~~~

**Expected:** GC preserves the tombstoned chunks. Repair does not resurrect the
object or automatically finish deletion.

Now explicitly retry DELETE:

~~~bash
expect_http 204 "$API/api/v1/objects/$OBJECT_ID" -X DELETE
expect_http 404 "$API/api/v1/objects/$OBJECT_ID"
~~~

**Expected:** deletion completes with no response body, and subsequent GET returns
404.

## 12. Verify all physical data was removed

Delete the three boundary-test objects, then check every worker:

~~~bash
delete_boundary_objects
check_empty_store
~~~

**Expected:** an empty object catalog and empty chunk directories on all four
workers. Original files and downloaded evidence remain in DEMO_DIR.

## 13. Clean up the demo deployment

This deliberately deletes **only this demo project's containers and volumes**,
including its stored data and local image tags:

~~~bash
dc down -v --rmi local --remove-orphans
echo "Host evidence remains in $DEMO_DIR"
~~~

Use the same command if you stop the presentation early. The project name printed
in step 1 lets you identify its resources if the original terminal is lost.

## 14. Automated verification

These checks are separate from the presentation. They require Go 1.27.4 or later;
the race detector also requires a working C compiler.

~~~bash
go test ./... -v -count=1
go test -race ./... -count=1
go vet ./...
~~~

Run the opt-in real-process and Docker integration tests:

~~~bash
OBJECTSTORE_PROCESS_E2E=1 go test ./tests -run '^TestMultiProcess$' -v -count=1 -timeout=5m
OBJECTSTORE_COMPOSE_KEEP=0 OBJECTSTORE_COMPOSE_E2E=1 go test ./tests -run '^TestComposeStack$' -v -count=1 -timeout=20m
~~~

These tests include internal chunk validation, lock lifetimes, failed uploads,
metadata races, cancellation, repair, persistence, and interrupted GC. Each
integration test owns its temporary processes, ports, and data; the Compose test
cleans its own project.

## Local development without Docker

This is an alternative to the Docker walkthrough, not its next step.
Use free ports 8080–8084, or adjust all listen and advertise URLs consistently.

Build the binaries:

~~~bash
mkdir -p tmp/bin
go build -o tmp/bin/controller ./cmd/controller
go build -o tmp/bin/worker ./cmd/worker
~~~

Start the controller in one terminal:

~~~bash
CONTROLLER_ADDR=127.0.0.1:8080 \
METADATA_DB_PATH=./data/controller/metadata.db \
./tmp/bin/controller
~~~

In four other terminals, run this with i set to 1, 2, 3, and 4:

~~~bash
i=1
WORKER_ID="worker$i" \
WORKER_ADDR="127.0.0.1:$((8080 + i))" \
WORKER_DATA_DIR="./data/worker$i" \
CONTROLLER_URL=http://127.0.0.1:8080 \
WORKER_ADVERTISE_URL="http://127.0.0.1:$((8080 + i))" \
./tmp/bin/worker
~~~

Use Ctrl+C to stop these processes. Reuse worker identities and directories on
restart. Client URLs use http://127.0.0.1:8080; the Docker-specific helpers above
do not apply in local mode.

## Cancel a pending GC request

Use this only on a deployment you have not cleaned up. First inspect the logs
and restore missing workers or SQLite access. If you choose to cancel the request:

~~~bash
dc stop controller
dc run --rm -T --no-deps --entrypoint rm controller /data/gc.request
dc start controller
wait_for "controller ready after cancellation" api_ready
~~~

GC failures retain the request and stop the controller; Docker may retry it.
Always use the SQLite volume belonging to these workers: GC cannot recognize
a different but valid database as the wrong one.

## What the helpers do

The [helper source](scripts/demo_helpers.sh) is plain Bash and can be inspected.
It performs the same checks as the longer original walkthrough.

| Helper | Purpose |
| --- | --- |
| make_demo_files | Create random binary, empty, text, and exact-size input files |
| meta / inventory | Fetch object metadata / one worker's chunk inventory |
| check_download | Download the main object, compare bytes, and print its SHA-256 |
| demo_boundaries / demo_parallel_downloads | Verify boundary-size uploads / four simultaneous downloads |
| verify_replicas | Check two distinct placements, actual file hashes, and absence of extra replicas |
| expect_http | Run a request, print its JSON/status, and verify the expected status; 204 must have no body |
| request_gc | Atomically write the requested mode and timings into the controller volume |
| show_maintenance / finish_gc | Show 503 and live membership / wait for API readiness and request-file removal |
| check_metadata_persisted / check_metadata_patch | Verify the saved metadata snapshot / merged patch and unchanged checksum |
| check_gc_not_repeated / check_object_hidden | Verify no new GC run / a tombstone hidden from listing |
| delete_boundary_objects / check_empty_store | Delete the other demo objects / inspect the empty catalog and worker directories |

Helper checks print OK only after passing. Waits have deadlines and print
controller logs on timeout; they do not replace a failed check with a fixed sleep.
