package tests

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"mini-object-store/internal/metadata"
)

// Opt in because this builds binaries and waits for real heartbeat/repair
// intervals. All listeners, subprocesses and data belong exclusively to the test.
func TestMultiProcess(t *testing.T) {
	if os.Getenv("OBJECTSTORE_PROCESS_E2E") != "1" {
		t.Skip("set OBJECTSTORE_PROCESS_E2E=1 to run the real controller/four-worker test")
	}
	root := t.TempDir()
	binaries := make(map[string]string)
	for _, name := range []string{"controller", "worker"} {
		binary := filepath.Join(root, name)
		cmd := exec.Command("go", "build", "-o", binary, "./cmd/"+name)
		cmd.Dir = ".."
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", name, err, output)
		}
		binaries[name] = binary
	}
	// Reserve five distinct OS-selected ports together; release each only when
	// its child is about to bind. Never use or terminate an existing service.
	listeners := make([]net.Listener, 5)
	addresses := make([]string, 5)
	for i := range listeners {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = listener
		addresses[i] = listener.Addr().String()
		t.Cleanup(func() { listener.Close() })
	}
	type process struct {
		cmd *exec.Cmd
		log *os.File
	}
	processes := make([]*process, 5)
	stop := func(p *process) {
		if p != nil && p.cmd.ProcessState == nil {
			_ = p.cmd.Process.Kill()
			_ = p.cmd.Wait()
		}
	}
	start := func(index int, name string, env []string) {
		t.Helper()
		listeners[index].Close()
		log, err := os.CreateTemp(root, name+"-*.log")
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(binaries[name])
		cmd.Env = append(os.Environ(), env...)
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		p := &process{cmd: cmd, log: log}
		processes[index] = p
		t.Cleanup(func() {
			stop(p)
			log.Close()
			if t.Failed() {
				data, _ := os.ReadFile(log.Name())
				t.Logf("%s log:\n%s", name, data)
			}
		})
	}
	controllerEnv := []string{"CONTROLLER_ADDR=" + addresses[0]}
	start(0, "controller", controllerEnv)
	api := "http://" + addresses[0]
	client := &http.Client{Timeout: 8 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	dirs := make(map[string]string)
	workerEnvs := make(map[string][]string)
	indices := make(map[string]int)
	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("worker%d", i)
		dirs[id] = filepath.Join(root, id)
		indices[id] = i
		workerEnvs[id] = []string{"WORKER_ID=" + id, "WORKER_ADDR=" + addresses[i], "WORKER_DATA_DIR=" + dirs[id], "CONTROLLER_URL=" + api, "WORKER_ADVERTISE_URL=http://" + addresses[i]}
		start(i, "worker", workerEnvs[id])
	}
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
			time.Sleep(100 * time.Millisecond)
		}
		t.Log(label)
	}
	waitHealthy := func() {
		wait("four workers healthy", 15*time.Second, func() bool {
			status, body, err := request("GET", "/internal/workers", "", nil)
			if err != nil || status != 200 {
				return false
			}
			var result struct {
				Workers []struct {
					Status string `json:"status"`
				} `json:"workers"`
			}
			if json.Unmarshal(body, &result) != nil || len(result.Workers) != 4 {
				return false
			}
			for _, w := range result.Workers {
				if w.Status != "healthy" {
					return false
				}
			}
			return true
		})
	}
	waitHealthy()
	original := make([]byte, 2_345_679)
	if _, err := rand.Read(original); err != nil {
		t.Fatal(err)
	}
	upload := func(data []byte) metadata.Object {
		t.Helper()
		var body bytes.Buffer
		form := multipart.NewWriter(&body)
		part, err := form.CreateFormFile("file", "random.bin")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := part.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := form.WriteField("metadata", `{"owner":"alice"}`); err != nil {
			t.Fatal(err)
		}
		if err := form.Close(); err != nil {
			t.Fatal(err)
		}
		status, response, err := request("POST", "/api/v1/objects", form.FormDataContentType(), &body)
		if err != nil || status != 201 {
			t.Fatalf("upload: status=%d err=%v body=%s", status, err, response)
		}
		var obj metadata.Object
		if err := json.Unmarshal(response, &obj); err != nil {
			t.Fatal(err)
		}
		return obj
	}
	obj := upload(original)
	if obj.ChunkCount != 3 || obj.Size != int64(len(original)) {
		t.Fatalf("wrong object shape: %+v", obj)
	}
	objectPath := "/api/v1/objects/" + obj.ID
	readMetadata := func() (metadata.Object, bool) {
		status, body, err := request("GET", objectPath+"/metadata", "", nil)
		var current metadata.Object
		if err != nil || status != 200 || json.Unmarshal(body, &current) != nil {
			return current, false
		}
		return current, true
	}
	validCopies := func(current metadata.Object) bool {
		for _, chunk := range current.Chunks {
			if len(chunk.Replicas) != 2 || chunk.Replicas[0] == chunk.Replicas[1] {
				return false
			}
			start := int64(chunk.Index) * 1_000_000
			for _, id := range chunk.Replicas {
				data, err := os.ReadFile(filepath.Join(dirs[id], chunk.ID))
				if err != nil || !bytes.Equal(data, original[start:start+chunk.Size]) {
					return false
				}
			}
		}
		return true
	}
	physicalCopies := func(current metadata.Object) bool {
		if !validCopies(current) {
			return false
		}
		for _, chunk := range current.Chunks {
			count := 0
			for _, dir := range dirs {
				if _, err := os.Stat(filepath.Join(dir, chunk.ID)); err == nil {
					count++
				}
			}
			if count != 2 {
				return false
			}
		}
		return true
	}
	if !physicalCopies(obj) {
		t.Fatal("invalid initial chunk distribution/replication")
	}
	used := make(map[string]bool)
	for _, chunk := range obj.Chunks {
		for _, id := range chunk.Replicas {
			used[id] = true
		}
	}
	if len(used) != 4 {
		t.Fatalf("three chunks distributed across only %d workers", len(used))
	}
	t.Logf("uploaded %d bytes: 3 chunks, 2 distinct replicas each, all 4 workers used", len(original))
	checkDownload := func(label string) {
		t.Helper()
		status, downloaded, err := request("GET", objectPath, "", nil)
		if err != nil || status != 200 || sha256.Sum256(downloaded) != sha256.Sum256(original) {
			t.Fatalf("%s download: status=%d err=%v hash mismatch", label, status, err)
		}
		t.Logf("%s download SHA-256: %x", label, sha256.Sum256(downloaded))
	}
	checkDownload("initial")
	victim := obj.Chunks[0].Replicas[0]
	stop(processes[indices[victim]])
	checkDownload("one worker stopped")
	wait("automatic failure repair", 25*time.Second, func() bool {
		current, ok := readMetadata()
		if !ok {
			return false
		}
		for _, chunk := range current.Chunks {
			for _, id := range chunk.Replicas {
				if id == victim {
					return false
				}
			}
		}
		return validCopies(current)
	})
	start(indices[victim], "worker", workerEnvs[victim])
	waitHealthy()
	wait("returning worker stale copies removed", 25*time.Second, func() bool { current, ok := readMetadata(); return ok && physicalCopies(current) })
	obj, _ = readMetadata()
	chunk := obj.Chunks[0]
	if err := os.WriteFile(filepath.Join(dirs[chunk.Replicas[0]], chunk.ID), bytes.Repeat([]byte{0xff}, int(chunk.Size)), 0600); err != nil {
		t.Fatal(err)
	}
	checkDownload("one replica corrupted")
	wait("automatic corruption repair", 25*time.Second, func() bool { current, ok := readMetadata(); return ok && physicalCopies(current) })
	status, body, err := request("PATCH", objectPath+"/metadata", "application/json", bytes.NewBufferString(`{"custom_metadata":{"owner":"bob","course":"cloud"}}`))
	if err != nil || status != 200 {
		t.Fatalf("patch: %d %v %s", status, err, body)
	}
	updated, ok := readMetadata()
	if !ok || updated.CustomMetadata["owner"] != "bob" || updated.CustomMetadata["course"] != "cloud" {
		t.Fatal("metadata patch not retained")
	}
	status, body, err = request("GET", "/api/v1/objects", "", nil)
	var listing struct {
		Objects []metadata.Object `json:"objects"`
	}
	if err != nil || status != 200 || json.Unmarshal(body, &listing) != nil || len(listing.Objects) != 1 {
		t.Fatal("object listing failed")
	}
	status, body, err = request("DELETE", objectPath, "", nil)
	if err != nil || status != 204 {
		t.Fatalf("delete: %d %v %s", status, err, body)
	}
	for id, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 0 {
			t.Fatalf("%s deletion left files: %v %v", id, entries, err)
		}
	}
	status, _, err = request("GET", objectPath, "", nil)
	if err != nil || status != 404 {
		t.Fatal("deleted object still downloadable")
	}
	t.Log("metadata get/list/patch and deletion verified; every worker directory empty")
	// Explicitly verify the documented in-memory restart behavior, while
	// retaining worker disks. Never garbage-collect these now-unknown chunks.
	orphan := upload([]byte("in-memory metadata restart check"))
	stop(processes[0])
	start(0, "controller", controllerEnv)
	waitHealthy()
	status, _, err = request("GET", "/api/v1/objects/"+orphan.ID+"/metadata", "", nil)
	if err != nil || status != 404 {
		t.Fatal("unexpected metadata persistence after controller restart")
	}
	copies := 0
	for _, dir := range dirs {
		if _, err := os.Stat(filepath.Join(dir, orphan.Chunks[0].ID)); err == nil {
			copies++
		}
	}
	if copies != 2 {
		t.Fatal("controller restart unexpectedly removed worker data")
	}
	t.Log("controller restart: workers re-registered; metadata lost as designed; worker data retained")
}
