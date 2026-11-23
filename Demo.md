# Project demonstration

This walkthrough demonstrates the implemented features in presentation order.
Read [README.md](README.md) for the architecture and consistency model.

Use **one Bash terminal**, starting in the repository root, and run the numbered
sections in order. Keep its variables and functions until cleanup. The main
walkthrough uses Docker; local development instructions are at the end.

Requirements: Docker Engine with Compose, curl, jq, Python 3, and GNU coreutils
(sha256sum, cmp, date, and stat). The first build needs network access. Allow
roughly 5–10 minutes after building, including real failure-detection and repair
intervals. Every wait below has a deadline; a failed check means stop and inspect
the logs rather than continue presenting an unverified result.

## 1. Create an isolated deployment

A unique Compose project gives this demo its own containers, network, volumes,
and image tags. An OS-selected host port avoids the usual 8080 conflict.

~~~bash
set -o pipefail
command -v docker curl jq python3 sha256sum cmp
docker compose version

DEMO_PROJECT="mos-demo-$(date +%s)-$RANDOM"
DEMO_DIR=$(mktemp -d /tmp/mini-object-store-demo.XXXXXX)
CONTROLLER_PORT=$(python3 - <<'PY'
import socket
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PY
)
export CONTROLLER_PORT
API="http://127.0.0.1:$CONTROLLER_PORT"

dc() { docker compose -p "$DEMO_PROJECT" -f compose.yaml "$@"; }
printf 'Project: %s\nAPI: %s\nEvidence files: %s\n' "$DEMO_PROJECT" "$API" "$DEMO_DIR"

dc config -q
dc up --build -d --wait
dc ps
~~~

The temporary port reservation ends before Docker binds; if another process takes
it in that interval, choose a new free port and recreate this demo project.

Define reusable checks. These functions poll actual readiness and physical
replicas instead of assuming a fixed sleep is long enough.

~~~bash
wait_for() {
    local description="$1"
    shift
    local deadline=$((SECONDS + 180))
    until "$@"; do
        if (( SECONDS >= deadline )); then
            printf 'Timed out: %s\n' "$description" >&2
            dc logs --no-color --tail 50 controller
            return 1
        fi
        sleep 1
    done
    printf 'OK: %s\n' "$description"
}

all_healthy() {
    curl -fsS --max-time 5 "$API/internal/workers" 2>/dev/null |
        jq -e '(.workers | length) == 4 and all(.workers[]; .status == "healthy")' >/dev/null
}

api_ready() {
    curl -fsS --max-time 5 "$API/api/v1/objects" 2>/dev/null |
        jq -e 'has("objects")' >/dev/null
}

meta() {
    curl -fsS --max-time 15 "$API/api/v1/objects/$OBJECT_ID/metadata"
}

inventory() {
    dc exec -T "$1" wget -qO- "http://127.0.0.1:8081/internal/workers/$1/chunks"
}

maintenance_active() {
    local status
    status=$(curl -sS --max-time 5 -D "$DEMO_DIR/maintenance.headers" \
        -o "$DEMO_DIR/maintenance.json" -w '%{http_code}' "$API/api/v1/objects") || return 1
    [[ "$status" == 503 ]] &&
        jq -e '.error | contains("garbage collection")' "$DEMO_DIR/maintenance.json" >/dev/null
}

request_gc() {
    printf '{"mode":"%s","grace_period":"%s","worker_wait_timeout":"%s"}\n' "$1" "$2" "$3" |
        dc run --rm -T --no-deps --entrypoint sh controller \
            -c 'cat > /data/gc.request.tmp && mv /data/gc.request.tmp /data/gc.request'
}

verify_replicas() {
    local excluded="${1:-}" manifest chunk expected first second w record holders
    manifest=$(meta) || return 1
    jq -e '(.chunks | length) == .chunk_count and
        all(.chunks[]; (.replicas | length) == 2 and .replicas[0] != .replicas[1])' \
        <<<"$manifest" >/dev/null || return 1

    while IFS=$'\t' read -r chunk expected first second; do
        [[ "$first" != "$excluded" && "$second" != "$excluded" ]] || return 1
        holders=0
        for w in worker1 worker2 worker3 worker4; do
            [[ "$w" == "$excluded" ]] && continue
            record=$(dc exec -T "$w" sh -c \
                'if [ -f "$1" ]; then sha256sum "$1"; else printf "absent\n"; fi' \
                sh "/data/chunks/$chunk") || return 1
            if [[ "$record" != absent ]]; then
                [[ "${record%% *}" == "$expected" ]] || return 1
                [[ "$w" == "$first" || "$w" == "$second" ]] || return 1
                ((holders += 1))
            fi
        done
        [[ "$holders" == 2 ]] || return 1
    done < <(jq -r '.chunks[] | [.chunk_id, .checksum, .replicas[0], .replicas[1]] | @tsv' <<<"$manifest")
}

wait_for "four workers healthy" all_healthy
wait_for "object API ready" api_ready
curl -fsS "$API/internal/workers" | jq .
~~~

**Show:** one controller and four distinct healthy worker IDs. Containers share
the internal worker port safely because they have separate network identities
and volumes.

## 2. Upload binary data with custom metadata

Create a 2,345,679-byte random object. It must become three chunks of 1,000,000,
1,000,000, and 345,679 bytes.

~~~bash
python3 - "$DEMO_DIR/original.bin" <<'PY'
import os
import pathlib
import sys
pathlib.Path(sys.argv[1]).write_bytes(os.urandom(2_345_679))
PY

ORIGINAL_SHA=$(sha256sum "$DEMO_DIR/original.bin")
ORIGINAL_SHA="${ORIGINAL_SHA%% *}"

curl -fsS -D "$DEMO_DIR/upload.headers" \
    -F "file=@$DEMO_DIR/original.bin;type=application/octet-stream" \
    -F 'metadata={"owner":"alice","course":"cloud"}' \
    "$API/api/v1/objects" -o "$DEMO_DIR/upload.json"

cat "$DEMO_DIR/upload.headers"
jq . "$DEMO_DIR/upload.json"
OBJECT_ID=$(jq -er '.object_id' "$DEMO_DIR/upload.json")

jq -e --arg sha "$ORIGINAL_SHA" \
    '.size == 2345679 and .chunk_count == 3 and .checksum == $sha and
     [.chunks[].size] == [1000000, 1000000, 345679]' "$DEMO_DIR/upload.json"
~~~

**Show:** HTTP 201, generated object ID, timestamps, content type, whole-object
checksum, custom metadata, and ordered chunk records.

## 3. Inspect chunk distribution and physical replication

~~~bash
meta | jq -r '.chunks[] | [.index, .chunk_id, .size, (.replicas | join(", "))] | @tsv'
meta | jq '[.chunks[].replicas[]] | unique'

for w in worker1 worker2 worker3 worker4; do
    printf '\n%s inventory:\n' "$w"
    inventory "$w" | jq .
done

wait_for "exactly two physical, SHA-256 verified copies per chunk" verify_replicas
~~~

**Show:** all four workers participate in this three-chunk upload; every chunk has
two copies on distinct workers. The verification function reads actual file
hashes and checks them against metadata, rather than trusting the placement list.

## 4. Download integrity, boundary sizes, and concurrency

~~~bash
curl -fsS -D "$DEMO_DIR/download.headers" \
    "$API/api/v1/objects/$OBJECT_ID" -o "$DEMO_DIR/download.bin"
cat "$DEMO_DIR/download.headers"
sha256sum "$DEMO_DIR/original.bin" "$DEMO_DIR/download.bin"
cmp "$DEMO_DIR/original.bin" "$DEMO_DIR/download.bin"
~~~

**Show:** HTTP 200, application/octet-stream, the original size, and matching
hashes/bytes. cmp succeeds silently.

Exercise empty, small text, and exactly 1 MB objects as well. The main object
already covers a multi-chunk file with a partial final chunk.

~~~bash
python3 - "$DEMO_DIR" <<'PY'
import os
import pathlib
import sys
root = pathlib.Path(sys.argv[1])
(root / "empty.bin").write_bytes(b"")
(root / "small.txt").write_text("Cloud computing demo\n")
(root / "exact-1mb.bin").write_bytes(os.urandom(1_000_000))
PY

: > "$DEMO_DIR/boundary-ids.txt"
for name in empty.bin small.txt exact-1mb.bin; do
    type=application/octet-stream
    [[ "$name" == small.txt ]] && type=text/plain
    curl -fsS -F "file=@$DEMO_DIR/$name;type=$type" \
        "$API/api/v1/objects" -o "$DEMO_DIR/$name.json"
    id=$(jq -er '.object_id' "$DEMO_DIR/$name.json")
    printf '%s\n' "$id" >> "$DEMO_DIR/boundary-ids.txt"
    size=$(stat -c %s "$DEMO_DIR/$name")
    chunks=$(((size + 999999) / 1000000))
    jq -e --argjson size "$size" --argjson chunks "$chunks" \
        '.size == $size and .chunk_count == $chunks' "$DEMO_DIR/$name.json"
    curl -fsS "$API/api/v1/objects/$id" -o "$DEMO_DIR/$name.download"
    cmp "$DEMO_DIR/$name" "$DEMO_DIR/$name.download"
done

pids=()
for i in 1 2 3 4; do
    curl -fsS "$API/api/v1/objects/$OBJECT_ID" -o "$DEMO_DIR/concurrent-$i.bin" &
    pids+=("$!")
done
for pid in "${pids[@]}"; do wait "$pid"; done
for i in 1 2 3 4; do cmp "$DEMO_DIR/original.bin" "$DEMO_DIR/concurrent-$i.bin"; done
printf 'All boundary uploads and concurrent downloads matched.\n'
~~~

## 5. List, retrieve, and patch metadata; inspect JSON errors

Upload uses a plain metadata map. PATCH uses a custom_metadata wrapper.

~~~bash
curl -fsS "$API/api/v1/objects" | jq .
meta | jq .

curl -fsS -X PATCH -H 'Content-Type: application/json' \
    -d '{"custom_metadata":{"owner":"bob","experiment":"replication"}}' \
    "$API/api/v1/objects/$OBJECT_ID/metadata" | jq .

meta | jq -e --arg sha "$ORIGINAL_SHA" \
    '.custom_metadata.owner == "bob" and .custom_metadata.course == "cloud" and
     .custom_metadata.experiment == "replication" and .checksum == $sha'
curl -fsS "$API/api/v1/objects/$OBJECT_ID" -o "$DEMO_DIR/after-patch.bin"
cmp "$DEMO_DIR/original.bin" "$DEMO_DIR/after-patch.bin"
~~~

**Show:** metadata was merged, last_modified changed, and the object data/checksum
stayed the same.

For intentional errors, omit curl's -f so the JSON response can be inspected:

~~~bash
status=$(curl -sS -o "$DEMO_DIR/missing.json" -w '%{http_code}' \
    "$API/api/v1/objects/does-not-exist")
[[ "$status" == 404 ]]
jq . "$DEMO_DIR/missing.json"

status=$(curl -sS -D "$DEMO_DIR/method.headers" -o "$DEMO_DIR/method.json" \
    -w '%{http_code}' -X PUT "$API/api/v1/objects/$OBJECT_ID")
[[ "$status" == 405 ]]
cat "$DEMO_DIR/method.headers"
jq . "$DEMO_DIR/method.json"

status=$(curl -sS -o "$DEMO_DIR/invalid.json" -w '%{http_code}' \
    -X PATCH -H 'Content-Type: application/json' \
    -d '{"custom_metadata":{"owner":123}}' "$API/api/v1/objects/$OBJECT_ID/metadata")
[[ "$status" == 400 ]]
jq . "$DEMO_DIR/invalid.json"
~~~

**Show:** meaningful JSON errors and an Allow header on 405.

## 6. Stop a worker, download through failover, and restore replication

Choose a worker that definitely holds a replica of the main object.

~~~bash
VICTIM=$(meta | jq -r '.chunks[0].replicas[0]')
printf 'Stopping %s\n' "$VICTIM"
dc stop "$VICTIM"

curl -fsS --max-time 45 "$API/api/v1/objects/$OBJECT_ID" -o "$DEMO_DIR/worker-down.bin"
cmp "$DEMO_DIR/original.bin" "$DEMO_DIR/worker-down.bin"

worker_unavailable() {
    curl -fsS "$API/internal/workers" |
        jq -e --arg id "$VICTIM" \
            'any(.workers[]; .worker_id == $id and .status == "unavailable")' >/dev/null
}
wait_for "heartbeat failure detected" worker_unavailable
curl -fsS "$API/internal/workers" | jq .

wait_for "replication restored on the three running workers" verify_replicas "$VICTIM"
meta | jq '.chunks[] | {chunk_id, replicas}'

dc start "$VICTIM"
wait_for "returning worker healthy" all_healthy
wait_for "returning worker stale copies reconciled" verify_replicas
~~~

**Show:** download succeeds while one replica is unavailable; repair places a new
copy on a spare worker. After the old worker returns, reconciliation removes
extra copies until each chunk again has exactly two physical replicas.
Detection takes over six seconds without contact; repair runs every 15 seconds,
plus HTTP request time. The waits observe completion rather than promise an
exact repair time.

## 7. Corrupt a replica and verify detection and repair

Select a current recorded replica. This intentionally modifies only a chunk in
this isolated demo's volume; the original file remains in DEMO_DIR.

~~~bash
CHUNK_ID=$(meta | jq -r '.chunks[0].chunk_id')
CORRUPT_WORKER=$(meta | jq -r '.chunks[0].replicas[0]')
EXPECTED_CHUNK_SHA=$(meta | jq -r '.chunks[0].checksum')

dc exec -T "$CORRUPT_WORKER" sh -c \
    'printf "\377\377\377\377" | dd of="$1" bs=1 count=4 conv=notrunc 2>/dev/null' \
    sh "/data/chunks/$CHUNK_ID"

ACTUAL_CHUNK_SHA=$(dc exec -T "$CORRUPT_WORKER" sha256sum "/data/chunks/$CHUNK_ID")
ACTUAL_CHUNK_SHA="${ACTUAL_CHUNK_SHA%% *}"
printf 'Expected: %s\nCorrupted: %s\n' "$EXPECTED_CHUNK_SHA" "$ACTUAL_CHUNK_SHA"
[[ "$ACTUAL_CHUNK_SHA" != "$EXPECTED_CHUNK_SHA" ]]

curl -fsS "$API/api/v1/objects/$OBJECT_ID" -o "$DEMO_DIR/corrupt-replica.bin"
cmp "$DEMO_DIR/original.bin" "$DEMO_DIR/corrupt-replica.bin"
wait_for "corruption repaired to two verified physical replicas" verify_replicas
dc logs --no-color --tail 50 controller
~~~

**Show:** the physical file has a different SHA-256 but the client still receives
the original bytes. Logs show the invalid replica and repair. Repair may run
before the download reaches that replica; either path detects the bad copy.
The same-length overwrite exercises checksum checking rather than only a length
mismatch.

## 8. Persist data across controller crash and container recreation

~~~bash
meta > "$DEMO_DIR/before-restart.json"

dc kill -s SIGKILL controller
dc start controller
wait_for "workers re-registered after controller crash" all_healthy
wait_for "object API ready after controller crash" api_ready

meta > "$DEMO_DIR/after-restart.json"
cmp <(jq -S . "$DEMO_DIR/before-restart.json") <(jq -S . "$DEMO_DIR/after-restart.json")
curl -fsS "$API/api/v1/objects/$OBJECT_ID" -o "$DEMO_DIR/after-crash.bin"
cmp "$DEMO_DIR/original.bin" "$DEMO_DIR/after-crash.bin"

dc down
dc up -d --wait
wait_for "recreated containers healthy" all_healthy
wait_for "recreated controller ready" api_ready
curl -fsS "$API/api/v1/objects/$OBJECT_ID" -o "$DEMO_DIR/recreated.bin"
cmp "$DEMO_DIR/original.bin" "$DEMO_DIR/recreated.bin"
wait_for "physical replicas survived container recreation" verify_replicas
meta | jq '{object_id, custom_metadata, checksum, created_at, last_modified}'
~~~

**Show:** SQLite survives SIGKILL, custom metadata survives, and named volumes
preserve chunks even when containers are removed. Do not add -v to dc down here:
that would destroy the demo's data.

## 9. GC dry-run

Seed two orphan replicas with a controller-format chunk name. This is an explicit
crash-state fixture representing files published without an object metadata
commit; it does not claim that we interrupted a real upload at that instant.
The upload failure boundary is exercised by the automated tests.

~~~bash
ORPHAN_CHUNK=obj-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee-0
for w in worker1 worker2; do
    dc exec -T "$w" sh -c 'printf orphan > "$1"' sh "/data/chunks/$ORPHAN_CHUNK"
done
ORPHAN_OWNER="${ORPHAN_CHUNK%-0}"
status=$(curl -sS -o "$DEMO_DIR/orphan-metadata.json" -w '%{http_code}' \
    "$API/api/v1/objects/$ORPHAN_OWNER/metadata")
[[ "$status" == 404 ]]

dc stop controller
request_gc dry-run 5s 60s
dc start controller

wait_for "maintenance rejects object requests" maintenance_active
cat "$DEMO_DIR/maintenance.headers"
jq . "$DEMO_DIR/maintenance.json"
curl -fsS "$API/internal/workers" | jq .

wait_for "dry-run completed" api_ready
dc exec -T controller sh -c 'test ! -e /data/gc.request'
for w in worker1 worker2; do
    dc exec -T "$w" sha256sum "/data/chunks/$ORPHAN_CHUNK"
done
dc logs --no-color --tail 40 controller
~~~

**Show:** object requests receive JSON 503 with Retry-After while membership
remains available. Logs report two candidates and zero deleted bytes. Both
orphan files remain. Successful dry-run consumes only its own request; it never
authorizes a later destructive run.

The request file accepts mode, grace_period, and worker_wait_timeout. The defaults
are dry-run and 60 seconds for both durations; an empty request is a dry-run.
Production-sized grace periods can be longer than the shortened demo settings.

## 10. Requested deletion, late worker, and interrupted-GC recovery

Keep worker4 stopped so GC waits for registration. This provides a visible
maintenance window without depending on catching a particular instant in a
short grace period. Complete these steps within the five-minute discovery timeout.

~~~bash
dc stop controller worker4
request_gc delete 5s 300s
dc start controller

wait_for "requested GC waiting for the fourth worker" maintenance_active
dc exec -T controller cat /data/gc.request
curl -fsS "$API/internal/workers" | jq .

dc kill -s SIGKILL controller
dc start controller
wait_for "interrupted GC request resumed" maintenance_active
dc exec -T controller cat /data/gc.request

dc start worker4
wait_for "late worker registered" all_healthy
wait_for "destructive GC completed" api_ready

dc exec -T controller sh -c 'test ! -e /data/gc.request'
for w in worker1 worker2; do
    dc exec -T "$w" sh -c 'test ! -e "$1"' sh "/data/chunks/$ORPHAN_CHUNK"
done
dc logs --no-color --tail 70 controller
curl -fsS "$API/api/v1/objects/$OBJECT_ID" -o "$DEMO_DIR/after-gc.bin"
cmp "$DEMO_DIR/original.bin" "$DEMO_DIR/after-gc.bin"
wait_for "GC preserved the committed object's replicas" verify_replicas
~~~

**Show:** the request survives SIGKILL, a late worker is included, and GC reclaims
two six-byte files (12 estimated bytes). The committed object remains intact.
A missing worker, failed inventory, SQLite error, or a newly observed orphan
without a full grace period fails the pass and retains the request. Docker may
restart and retry that outstanding request.

To prove a completed request does not run on ordinary restart/reconnect:

~~~bash
GC_REQUESTS_BEFORE=$(dc logs --no-color controller |
    jq -Rs '[splits("\n") | select(contains("gc: requested"))] | length')

dc restart controller
wait_for "ordinary restart healthy" all_healthy
wait_for "ordinary restart ready" api_ready
dc restart worker1
wait_for "worker reconnect healthy" all_healthy

GC_REQUESTS_AFTER=$(dc logs --no-color controller |
    jq -Rs '[splits("\n") | select(contains("gc: requested"))] | length')
[[ "$GC_REQUESTS_BEFORE" == "$GC_REQUESTS_AFTER" ]]
printf 'GC request count stayed at %s.\n' "$GC_REQUESTS_AFTER"
~~~

## 11. Incomplete deletion, persistent tombstones, and explicit retry

Stop a replica worker before deleting the main object. Expected failure responses
are captured without curl -f.

~~~bash
VICTIM=$(meta | jq -r '.chunks[0].replicas[0]')
dc stop "$VICTIM"

status=$(curl -sS -X DELETE -o "$DEMO_DIR/delete-pending.json" -w '%{http_code}' \
    "$API/api/v1/objects/$OBJECT_ID")
[[ "$status" == 503 ]]
jq . "$DEMO_DIR/delete-pending.json"

curl -fsS "$API/api/v1/objects" |
    jq -e --arg id "$OBJECT_ID" 'all(.objects[]; .object_id != $id)'

dc restart controller
wait_for "object listing available with one worker down" api_ready
status=$(curl -sS -o "$DEMO_DIR/tombstone.json" -w '%{http_code}' \
    "$API/api/v1/objects/$OBJECT_ID/metadata")
[[ "$status" == 503 ]]
jq . "$DEMO_DIR/tombstone.json"

dc start "$VICTIM"
wait_for "all workers returned for deletion retry" all_healthy
~~~

**Show:** deletion made progress on reachable workers but retained its tombstone.
The object is hidden from listing, and the deletion state survived restart.
Background repair does not resurrect the object or finish its deletion.

Also prove GC preserves the pending deletion instead of mistaking it for an orphan:

~~~bash
dc stop controller
request_gc delete 2s 60s
dc start controller
wait_for "GC finished with a deletion tombstone present" api_ready
wait_for "workers healthy after maintenance" all_healthy

status=$(curl -sS -o "$DEMO_DIR/tombstone-after-gc.json" -w '%{http_code}' \
    "$API/api/v1/objects/$OBJECT_ID/metadata")
[[ "$status" == 503 ]]
jq . "$DEMO_DIR/tombstone-after-gc.json"
dc logs --no-color --tail 35 controller

status=$(curl -sS -X DELETE -o "$DEMO_DIR/delete-complete.body" -w '%{http_code}' \
    "$API/api/v1/objects/$OBJECT_ID")
[[ "$status" == 204 ]]
[[ ! -s "$DEMO_DIR/delete-complete.body" ]]
printf 'Explicit deletion retry returned %s with no body.\n' "$status"

status=$(curl -sS -o "$DEMO_DIR/deleted.json" -w '%{http_code}' \
    "$API/api/v1/objects/$OBJECT_ID")
[[ "$status" == 404 ]]
jq . "$DEMO_DIR/deleted.json"
~~~

## 12. Delete remaining objects and verify physical cleanup

~~~bash
while IFS= read -r id; do
    status=$(curl -sS -X DELETE -o "$DEMO_DIR/boundary-delete.body" -w '%{http_code}' \
        "$API/api/v1/objects/$id")
    [[ "$status" == 204 ]]
done < "$DEMO_DIR/boundary-ids.txt"

curl -fsS "$API/api/v1/objects" | jq -e '.objects | length == 0'
for w in worker1 worker2 worker3 worker4; do
    inventory "$w" | jq -e '.chunks | length == 0'
    dc exec -T "$w" ls -la /data/chunks
done
~~~

**Show:** no object metadata remains and every demo worker chunk directory is empty.
Original files and downloaded evidence remain on the host in DEMO_DIR.

## 13. Clean up this demo project

This step deliberately deletes **this demo project's volumes and stored data**.
It does not address containers or volumes belonging to another Compose project.

~~~bash
dc down -v --rmi local --remove-orphans
printf 'Host evidence retained in %s\n' "$DEMO_DIR"
~~~

If you stop early, run this same cleanup command from the original terminal.
The unique project name was printed in section 1 if you need to recover it.
No global Docker prune or broad process-kill commands are needed.

## 14. Automated verification

These commands are separate from the manual presentation. Go 1.27.4 or later is
required; the race detector also needs a working C compiler. Tests allocate their
own temporary data and ports.

~~~bash
go test ./... -v -count=1
go test -race ./... -count=1
go vet ./...
~~~

The opt-in tests exercise the real deployment, including controller crashes,
worker repair, physical hashes, persistent tombstones, GC recovery, and ordinary
restarts without GC. The Compose test uses another isolated project and removes
its own containers and volumes afterward.

~~~bash
OBJECTSTORE_PROCESS_E2E=1 go test ./tests -run '^TestMultiProcess$' -v -count=1 -timeout=5m
OBJECTSTORE_COMPOSE_KEEP=0 OBJECTSTORE_COMPOSE_E2E=1 go test ./tests -run '^TestComposeStack$' -v -count=1 -timeout=20m
~~~

Internal chunk validation, lock lifetimes, forced metadata races, failed uploads,
and cancellation are covered by these tests; the presentation does not pretend
to reproduce every possible race manually.

## Local development without Docker

This is a separate startup option, not a continuation of the Docker walkthrough.
Use free ports 8080–8084, or adjust every listen/advertise URL consistently.

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

In four other terminals, set i to 1, 2, 3, and 4 respectively:

~~~bash
i=1
WORKER_ID="worker$i" \
WORKER_ADDR="127.0.0.1:$((8080 + i))" \
WORKER_DATA_DIR="./data/worker$i" \
CONTROLLER_URL=http://127.0.0.1:8080 \
WORKER_ADVERTISE_URL="http://127.0.0.1:$((8080 + i))" \
./tmp/bin/worker
~~~

Use Ctrl+C to stop these processes. Reuse each worker's identity and directory
on restart. The client URLs then start with http://127.0.0.1:8080; Docker-specific
helpers in the main walkthrough do not apply to this local mode.

## Recovery if a GC request fails

This is an optional recovery action for a stack that is still running, not a
step after cleanup. First inspect the controller logs and restore missing workers
or database access. To cancel a pending request while keeping the data, stop the
controller, remove only its request file with a volume helper, and start again:

~~~bash
dc stop controller
dc run --rm -T --no-deps --entrypoint rm controller /data/gc.request
dc start controller
wait_for "controller ready after cancelling GC" api_ready
~~~

Only use this if a request is present and you choose to cancel it. GC cannot
automatically recognize a different but valid metadata database, so always use
the database volume that belongs to these worker volumes.
