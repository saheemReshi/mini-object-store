#!/usr/bin/env bash
# Source from the repository root: source scripts/demo_helpers.sh
# This file defines helpers only. demo_init creates an isolated session; Docker
# services are started explicitly by the commands in Demo.md.

demo_init() {
    [[ -f compose.yaml ]] || { echo "Run from the repository root." >&2; return 1; }
    local tool
    for tool in docker curl jq python3 sha256sum cmp; do
        command -v "$tool" >/dev/null || { echo "Missing tool: $tool" >&2; return 1; }
    done
    set -o pipefail
    DEMO_PROJECT="mos-demo-$(date +%s)-$RANDOM"
    DEMO_DIR=$(mktemp -d /tmp/mini-object-store-demo.XXXXXX) || return 1
    CONTROLLER_PORT=$(python3 - <<'PY'
import socket
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PY
    ) || return 1
    export CONTROLLER_PORT
    API="http://127.0.0.1:$CONTROLLER_PORT"
    printf 'Project: %s\nAPI: %s\nFiles: %s\n' "$DEMO_PROJECT" "$API" "$DEMO_DIR"
}

dc() { docker compose -p "$DEMO_PROJECT" -f compose.yaml "$@"; }
meta() { curl -fsS --max-time 15 "$API/api/v1/objects/$OBJECT_ID/metadata"; }
inventory() { dc exec -T "$1" wget -qO- "http://127.0.0.1:8081/internal/workers/$1/chunks"; }

wait_for() {
    local description="$1" deadline=$((SECONDS + 180))
    shift
    until "$@"; do
        if (( SECONDS >= deadline )); then
            printf 'Timed out: %s. Inspect the logs before continuing.\n' "$description" >&2
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

worker_unavailable() {
    curl -fsS --max-time 5 "$API/internal/workers" |
        jq -e --arg id "$VICTIM" 'any(.workers[]; .worker_id == $id and .status == "unavailable")' >/dev/null
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

make_demo_files() {
    python3 - "$DEMO_DIR" <<'PY'
import os
import pathlib
import sys
root = pathlib.Path(sys.argv[1])
(root / "original.bin").write_bytes(os.urandom(2_345_679))
(root / "empty.bin").write_bytes(b"")
(root / "small.txt").write_text("Cloud computing demo\n")
(root / "exact-1mb.bin").write_bytes(os.urandom(1_000_000))
PY
    ORIGINAL_SHA=$(sha256sum "$DEMO_DIR/original.bin") || return 1
    ORIGINAL_SHA="${ORIGINAL_SHA%% *}"
    echo "Created: empty, small text, exactly 1 MB, and 2,345,679-byte binary files."
}

check_download() {
    local label="$1" destination="$DEMO_DIR/$1.bin"
    curl -fsS --max-time 45 "$API/api/v1/objects/$OBJECT_ID" -o "$destination" || return 1
    cmp "$DEMO_DIR/original.bin" "$destination" || return 1
    sha256sum "$destination"
    printf 'OK: %s matches the original bytes.\n' "$label"
}

demo_boundaries() {
    local name type id size chunks
    : > "$DEMO_DIR/boundary-ids.txt"
    printf '%-15s %10s %8s %s\n' "File" "Bytes" "Chunks" "Download"
    for name in empty.bin small.txt exact-1mb.bin; do
        type=application/octet-stream
        [[ "$name" == small.txt ]] && type=text/plain
        curl -fsS -F "file=@$DEMO_DIR/$name;type=$type" \
            "$API/api/v1/objects" -o "$DEMO_DIR/$name.json" || return 1
        id=$(jq -er '.object_id' "$DEMO_DIR/$name.json") || return 1
        printf '%s\n' "$id" >> "$DEMO_DIR/boundary-ids.txt"
        size=$(stat -c %s "$DEMO_DIR/$name") || return 1
        chunks=$(((size + 999999) / 1000000))
        jq -e --argjson size "$size" --argjson chunks "$chunks" \
            '.size == $size and .chunk_count == $chunks' "$DEMO_DIR/$name.json" >/dev/null || return 1
        curl -fsS "$API/api/v1/objects/$id" -o "$DEMO_DIR/$name.download" || return 1
        cmp "$DEMO_DIR/$name" "$DEMO_DIR/$name.download" || return 1
        printf '%-15s %10s %8s %s\n' "$name" "$size" "$chunks" "identical"
    done
}

demo_parallel_downloads() {
    local i pid failed=0
    local pids=()
    for i in 1 2 3 4; do
        curl -fsS --max-time 45 "$API/api/v1/objects/$OBJECT_ID" -o "$DEMO_DIR/concurrent-$i.bin" &
        pids+=("$!")
    done
    # Join every child, even if one fails; do not leave requests in flight.
    for pid in "${pids[@]}"; do wait "$pid" || failed=1; done
    (( failed == 0 )) || return 1
    for i in 1 2 3 4; do
        cmp "$DEMO_DIR/original.bin" "$DEMO_DIR/concurrent-$i.bin" || return 1
    done
    echo "OK: four simultaneous downloads all matched the original."
}

# Usage: expect_http EXPECTED URL [curl options]
# Intentional 4xx/503 responses are captured without curl -f.
expect_http() {
    local expected="$1" url="$2" actual
    shift 2
    actual=$(curl -sS --max-time 45 -D "$DEMO_DIR/response.headers" \
        -o "$DEMO_DIR/response.body" -w '%{http_code}' "$@" "$url") || return 1
    printf 'HTTP %s (expected %s)\n' "$actual" "$expected"
    if [[ "$actual" == 405 ]]; then cat "$DEMO_DIR/response.headers"; fi
    if [[ -s "$DEMO_DIR/response.body" ]]; then
        jq . "$DEMO_DIR/response.body" || return 1
    fi
    [[ "$actual" == "$expected" ]] || return 1
    if [[ "$expected" == 204 ]]; then
        [[ ! -s "$DEMO_DIR/response.body" ]] || return 1
    fi
}

demo_errors() {
    echo "Missing object:"
    expect_http 404 "$API/api/v1/objects/does-not-exist" || return 1
    echo "Unsupported method (including Allow header):"
    expect_http 405 "$API/api/v1/objects/$OBJECT_ID" -X PUT || return 1
    echo "Invalid metadata value:"
    expect_http 400 "$API/api/v1/objects/$OBJECT_ID/metadata" -X PATCH \
        -H 'Content-Type: application/json' -d '{"custom_metadata":{"owner":123}}'
}

request_gc() {
    printf '{"mode":"%s","grace_period":"%s","worker_wait_timeout":"%s"}\n' "$1" "$2" "$3" |
        dc run --rm -T --no-deps --entrypoint sh controller \
            -c 'cat > /data/gc.request.tmp && mv /data/gc.request.tmp /data/gc.request'
}

maintenance_active() {
    local status
    status=$(curl -sS --max-time 5 -D "$DEMO_DIR/maintenance.headers" \
        -o "$DEMO_DIR/maintenance.json" -w '%{http_code}' "$API/api/v1/objects") || return 1
    [[ "$status" == 503 ]] &&
        jq -e '.error | contains("garbage collection")' "$DEMO_DIR/maintenance.json" >/dev/null
}

show_maintenance() {
    wait_for "GC maintenance visible" maintenance_active || return 1
    cat "$DEMO_DIR/maintenance.headers"
    jq . "$DEMO_DIR/maintenance.json" || return 1
    echo "Membership still responds while object APIs are paused:"
    curl -fsS "$API/internal/workers" | jq .
}

finish_gc() {
    wait_for "GC finished; object API ready" api_ready || return 1
    dc exec -T controller sh -c 'test ! -e /data/gc.request' || return 1
    echo "OK: completed GC request was removed."
}

gc_request_count() {
    dc logs --no-color controller |
        jq -Rs '[splits("\n") | select(contains("gc: requested"))] | length'
}

check_gc_not_repeated() {
    local actual
    actual=$(gc_request_count) || return 1
    [[ "$actual" == "$1" ]] || { echo "Unexpected additional GC run." >&2; return 1; }
    printf 'OK: GC request count stayed at %s after restart/reconnect.\n' "$actual"
}

check_metadata_persisted() {
    meta > "$DEMO_DIR/after-restart.json" || return 1
    cmp <(jq -S . "$DEMO_DIR/before-restart.json") <(jq -S . "$DEMO_DIR/after-restart.json") || return 1
    echo "OK: complete metadata snapshot survived controller restart."
}

check_upload_manifest() {
    jq -e --arg sha "$ORIGINAL_SHA" \
        '.size == 2345679 and .chunk_count == 3 and .checksum == $sha and
         [.chunks[].size] == [1000000, 1000000, 345679]' "$DEMO_DIR/upload.json" >/dev/null || return 1
    echo "OK: three ordered chunks, correct final size, and matching whole-object SHA-256."
}

check_metadata_patch() {
    meta | jq -e --arg sha "$ORIGINAL_SHA" \
        '.custom_metadata.owner == "bob" and .custom_metadata.course == "cloud" and
         .custom_metadata.experiment == "replication" and .checksum == $sha' >/dev/null || return 1
    echo "OK: metadata merged and object checksum unchanged."
}

check_object_hidden() {
    curl -fsS "$API/api/v1/objects" |
        jq -e --arg id "$OBJECT_ID" 'all(.objects[]; .object_id != $id)' >/dev/null || return 1
    echo "OK: pending deletion is hidden from the object listing."
}

delete_boundary_objects() {
    local id
    while IFS= read -r id; do
        expect_http 204 "$API/api/v1/objects/$id" -X DELETE || return 1
    done < "$DEMO_DIR/boundary-ids.txt"
}

check_empty_store() {
    local w
    curl -fsS "$API/api/v1/objects" | jq -e '.objects | length == 0' >/dev/null || return 1
    for w in worker1 worker2 worker3 worker4; do
        inventory "$w" | jq -e '.chunks | length == 0' >/dev/null || return 1
        printf '%s: no chunk files remain\n' "$w"
        dc exec -T "$w" ls -la /data/chunks || return 1
    done
    echo "OK: object catalog and all four worker inventories are empty."
}
