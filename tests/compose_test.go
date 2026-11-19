package tests

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"mini-object-store/internal/metadata"
)

// TestComposeStack drives the real Docker Compose deployment. It is opt-in
// because it needs a Docker daemon and takes several minutes:
//
//	OBJECTSTORE_COMPOSE_E2E=1 go test ./tests -run TestComposeStack -v -count=1 -timeout 20m
//
// It uses a unique Compose project name (so containers, network and volumes
// are separate from any `docker compose up` you run by hand) and a free host
// port. Cleanup runs `down -v` for THAT project only. Set
// OBJECTSTORE_COMPOSE_KEEP=1 to leave the stack running for inspection.
func TestComposeStack(t *testing.T) {
	if os.Getenv("OBJECTSTORE_COMPOSE_E2E") != "1" {
		t.Skip("set OBJECTSTORE_COMPOSE_E2E=1 to run the Docker Compose test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatal("docker not found in PATH")
	}

	project := fmt.Sprintf("mos-e2e-%d", time.Now().UnixNano())
	port := freePort(t)
	env := append(os.Environ(), "CONTROLLER_PORT="+port)

	compose := func(args ...string) (string, error) {
		full := append([]string{"compose", "-p", project, "-f", "compose.yaml"}, args...)
		cmd := exec.Command("docker", full...)
		cmd.Dir = ".."
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	mustCompose := func(args ...string) string {
		t.Helper()
		out, err := compose(args...)
		if err != nil {
			t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return out
	}

	mustCompose("config", "-q")
	t.Cleanup(func() {
		if t.Failed() {
			out, _ := compose("logs", "--no-color", "--tail", "80")
			t.Logf("compose logs:\n%s", out)
		}
		if os.Getenv("OBJECTSTORE_COMPOSE_KEEP") == "1" {
			t.Logf("leaving project %s running (CONTROLLER_PORT=%s)", project, port)
			return
		}
		if out, err := compose("down", "-v", "--remove-orphans"); err != nil {
			t.Logf("cleanup of project %s failed: %v\n%s", project, err, out)
		}
	})
	mustCompose("up", "--build", "-d", "--wait", "--wait-timeout", "180")
	t.Logf("project %s up; controller on 127.0.0.1:%s", project, port)

	api := "http://127.0.0.1:" + port
	client := &http.Client{Timeout: 15 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	request := func(method, path, contentType string, body io.Reader) (int, []byte, error) {
		req, err := http.NewRequest(method, api+path, body)
		if err != nil {
			return 0, nil, err
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		return resp.StatusCode, data, err
	}
	wait := func(label string, timeout time.Duration, ready func() bool) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for !ready() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", label)
			}
			time.Sleep(time.Second)
		}
		t.Log(label)
	}
	workerIDs := []string{"worker1", "worker2", "worker3", "worker4"}
	waitHealthy := func() {
		t.Helper()
		wait("four stable worker IDs healthy", 60*time.Second, func() bool {
			status, body, err := request("GET", "/internal/workers", "", nil)
			if err != nil || status != 200 {
				return false
			}
			var result struct {
				Workers []struct {
					ID      string `json:"worker_id"`
					Address string `json:"address"`
					Status  string `json:"status"`
				} `json:"workers"`
			}
			if json.Unmarshal(body, &result) != nil || len(result.Workers) != 4 {
				return false
			}
			seen := map[string]bool{}
			for _, w := range result.Workers {
				if w.Status != "healthy" || w.Address != "http://"+w.ID+":8081" {
					return false
				}
				seen[w.ID] = true
			}
			for _, id := range workerIDs {
				if !seen[id] {
					return false
				}
			}
			return true
		})
	}

	// workerFiles maps chunk file name -> SHA-256 for one running worker.
	workerFiles := func(worker string) (map[string]string, error) {
		out, err := compose("exec", "-T", worker, "sh", "-c",
			`cd /data/chunks && for f in *; do [ -f "$f" ] && sha256sum "$f"; done; true`)
		if err != nil {
			return nil, fmt.Errorf("%v: %s", err, out)
		}
		files := map[string]string{}
		for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 2 {
				files[fields[1]] = fields[0]
			}
		}
		return files, nil
	}

	waitHealthy()

	// --- Upload a binary object larger than 2 MB -----------------------------
	original := make([]byte, 2_500_123)
	if _, err := rand.Read(original); err != nil {
		t.Fatal(err)
	}
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, _ := form.CreateFormFile("file", "random.bin")
	_, _ = part.Write(original)
	_ = form.WriteField("metadata", `{"owner":"alice"}`)
	_ = form.Close()
	status, response, err := request("POST", "/api/v1/objects", form.FormDataContentType(), &body)
	if err != nil || status != 201 {
		t.Fatalf("upload: %d %v %s", status, err, response)
	}
	var obj metadata.Object
	if err := json.Unmarshal(response, &obj); err != nil {
		t.Fatal(err)
	}
	wholeSum := sha256.Sum256(original)
	if obj.ChunkCount != 3 || len(obj.Chunks) != 3 || obj.Size != int64(len(original)) ||
		obj.Checksum != hex.EncodeToString(wholeSum[:]) {
		t.Fatalf("object shape/checksum: %+v", obj)
	}
	for i, chunk := range obj.Chunks {
		wantSize := int64(1_000_000)
		if i == 2 {
			wantSize = 500_123 // Final partial chunk.
		}
		if chunk.Index != i || chunk.Size != wantSize || len(chunk.Replicas) != 2 || chunk.Replicas[0] == chunk.Replicas[1] {
			t.Fatalf("chunk %d: %+v", i, chunk)
		}
	}
	objectPath := "/api/v1/objects/" + obj.ID
	t.Logf("uploaded %s: 3 ordered chunks, final partial, 2 distinct replicas each", obj.ID)

	readMetadata := func() (metadata.Object, int) {
		status, body, err := request("GET", objectPath+"/metadata", "", nil)
		var current metadata.Object
		if err != nil {
			return current, 0
		}
		if status == 200 && json.Unmarshal(body, &current) != nil {
			return current, 0
		}
		return current, status
	}
	checkDownload := func(label string) {
		t.Helper()
		var lastStatus int
		var lastErr error
		// Allow a few attempts: right after a restart the registry may still be
		// learning which workers are healthy.
		for attempt := 0; attempt < 10; attempt++ {
			status, data, err := request("GET", objectPath, "", nil)
			if err == nil && status == 200 && sha256.Sum256(data) == wholeSum {
				t.Logf("%s: download SHA-256 %x", label, wholeSum)
				return
			}
			lastStatus, lastErr = status, err
			time.Sleep(2 * time.Second)
		}
		t.Fatalf("%s: download failed: status=%d err=%v", label, lastStatus, lastErr)
	}
	// exactlyTwoVerified: metadata names two distinct running workers holding
	// verified copies, and no other running worker holds the chunk.
	exactlyTwoVerified := func(running []string) bool {
		current, status := readMetadata()
		if status != 200 {
			return false
		}
		files := map[string]map[string]string{}
		for _, w := range running {
			f, err := workerFiles(w)
			if err != nil {
				return false
			}
			files[w] = f
		}
		for _, chunk := range current.Chunks {
			if len(chunk.Replicas) != 2 || chunk.Replicas[0] == chunk.Replicas[1] {
				return false
			}
			for _, w := range chunk.Replicas {
				f, ok := files[w]
				if !ok || f[chunk.ID] != chunk.Checksum {
					return false
				}
			}
			holders := 0
			for _, f := range files {
				if _, ok := f[chunk.ID]; ok {
					holders++
				}
			}
			if holders != 2 {
				return false
			}
		}
		return true
	}

	checkDownload("initial")
	if !exactlyTwoVerified(workerIDs) {
		t.Fatal("initial physical replication incorrect")
	}

	// --- Worker stop: download via replicas, then automatic repair ----------
	victim := obj.Chunks[0].Replicas[0]
	mustCompose("stop", victim)
	checkDownload("worker " + victim + " stopped")
	running := make([]string, 0, 3)
	for _, id := range workerIDs {
		if id != victim {
			running = append(running, id)
		}
	}
	wait("repair to two verified copies without "+victim, 90*time.Second, func() bool {
		current, status := readMetadata()
		if status != 200 {
			return false
		}
		for _, chunk := range current.Chunks {
			for _, id := range chunk.Replicas {
				if id == victim {
					return false
				}
			}
		}
		return exactlyTwoVerified(running)
	})

	// --- Worker restart: disk survives; stale extra copies reconciled -------
	mustCompose("start", victim)
	if files, err := workerFiles(victim); err != nil || files[obj.Chunks[0].ID] != obj.Chunks[0].Checksum {
		t.Fatalf("%s lost its disk data across stop/start: %v %v", victim, files, err)
	}
	t.Logf("%s restarted with its chunk files intact", victim)
	waitHealthy()
	wait("stale replicas on "+victim+" removed", 90*time.Second, func() bool { return exactlyTwoVerified(workerIDs) })

	// --- Corrupted physical replica -----------------------------------------
	current, _ := readMetadata()
	chunk := current.Chunks[1]
	corrupt := chunk.Replicas[0]
	mustCompose("exec", "-T", corrupt, "sh", "-c",
		`printf '\377\377\377\377' | dd of=/data/chunks/`+chunk.ID+` bs=1 count=4 conv=notrunc 2>/dev/null`)
	if files, err := workerFiles(corrupt); err != nil || files[chunk.ID] == chunk.Checksum {
		t.Fatalf("corruption not applied on %s: %v", corrupt, err)
	}
	checkDownload("replica on " + corrupt + " corrupted")
	wait("corruption repaired to two verified copies", 90*time.Second, func() bool { return exactlyTwoVerified(workerIDs) })

	// --- Metadata CRUD without re-upload -------------------------------------
	status, response, err = request("PATCH", objectPath+"/metadata", "application/json",
		strings.NewReader(`{"custom_metadata":{"owner":"bob","course":"cloud"}}`))
	if err != nil || status != 200 {
		t.Fatalf("patch: %d %v %s", status, err, response)
	}
	checkMetadata := func(label string) {
		t.Helper()
		current, status := readMetadata()
		if status != 200 || current.CustomMetadata["owner"] != "bob" || current.CustomMetadata["course"] != "cloud" ||
			current.Checksum != obj.Checksum {
			t.Fatalf("%s: metadata: %d %+v", label, status, current)
		}
		status, body, err := request("GET", "/api/v1/objects", "", nil)
		var listing struct {
			Objects []metadata.Object `json:"objects"`
		}
		if err != nil || status != 200 || json.Unmarshal(body, &listing) != nil ||
			len(listing.Objects) != 1 || listing.Objects[0].ID != obj.ID {
			t.Fatalf("%s: listing: %d %v %s", label, status, err, body)
		}
		t.Logf("%s: metadata get/list/patch intact", label)
	}
	checkMetadata("after patch")

	// --- Controller restart (same container) ---------------------------------
	mustCompose("restart", "controller")
	waitHealthy()
	checkMetadata("controller restart")
	checkDownload("controller restart")

	// --- Container recreation: volumes, not containers, hold the data -------
	mustCompose("down") // No -v: named volumes are kept.
	mustCompose("up", "-d", "--wait", "--wait-timeout", "180")
	waitHealthy()
	checkMetadata("containers recreated")
	checkDownload("containers recreated")
	if !exactlyTwoVerified(workerIDs) {
		t.Fatal("chunk files did not survive container recreation")
	}

	// --- Incomplete deletion, controller restart, DELETE retry ---------------
	down := current.Chunks[0].Replicas[1]
	mustCompose("stop", down)
	status, response, err = request("DELETE", objectPath, "", nil)
	if err != nil || status != 503 {
		t.Fatalf("delete with %s stopped: %d %v %s", down, status, err, response)
	}
	mustCompose("restart", "controller")
	mustCompose("start", down)
	waitHealthy()
	if _, status := readMetadata(); status != 503 {
		t.Fatalf("tombstone did not survive controller restart: status %d", status)
	}
	status, response, err = request("GET", "/api/v1/objects", "", nil)
	if err != nil || status != 200 || strings.Contains(string(response), obj.ID) {
		t.Fatalf("tombstone visible in listing: %s", response)
	}
	status, response, err = request("DELETE", objectPath, "", nil)
	if err != nil || status != 204 {
		t.Fatalf("delete retry: %d %v %s", status, err, response)
	}
	if _, status := readMetadata(); status != 404 {
		t.Fatalf("deleted metadata still present: %d", status)
	}
	for _, w := range workerIDs {
		files, err := workerFiles(w)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range obj.Chunks {
			if _, ok := files[c.ID]; ok {
				t.Fatalf("%s still holds %s after deletion", w, c.ID)
			}
		}
	}
	t.Log("incomplete deletion: tombstone survived restart; retry removed metadata and every physical chunk")
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	_, port, _ := net.SplitHostPort(l.Addr().String())
	return port
}
