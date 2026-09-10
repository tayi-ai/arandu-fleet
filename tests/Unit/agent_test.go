package unit_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fleet "github.com/tayi-ai/arandu-fleet"
)

// What the node half of the protocol is asked to prove: that it refuses without
// the bearerToken, that it owns no vocabulary of its own, that it holds one run at a
// time, and that an artifact whose bytes disagree with its declared digest does
// not survive on disk.

const bearerToken = "a-token"

// echo is a Program whose only action runs true(1), which every unix has.
func echo(t *testing.T) fleet.Program {
	t.Helper()
	return fleet.ProgramFunc(func(j fleet.Job) (fleet.Command, error) {
		switch j.Action {
		case "sleep":
			return fleet.Command{Path: "/bin/sh", Args: []string{"-c", "sleep 30"}}, nil
		case "ok":
			return fleet.Command{Path: "/bin/sh", Args: []string{"-c", "exit 0"}}, nil
		case "bad":
			return fleet.Command{Path: "/bin/sh", Args: []string{"-c", "exit 3"}}, nil
		}
		return fleet.Command{}, errNoAction
	})
}

var errNoAction = &actionError{}

type actionError struct{}

func (*actionError) Error() string { return "no such action" }

func agent(t *testing.T) (*fleet.Agent, *httptest.Server) {
	t.Helper()
	a, err := fleet.NewAgent(bearerToken, echo(t), filepath.Join(t.TempDir(), "logs"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(a.Handler())
	t.Cleanup(server.Close)
	return a, server
}

func call(t *testing.T, server *httptest.Server, method, path, bearer, body string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	answer, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { answer.Body.Close() })
	return answer
}

func TestTheAgentRefusesEveryRouteWithoutTheToken(t *testing.T) {
	_, server := agent(t)
	for _, route := range []struct{ method, path string }{
		{"GET", "/status"},
		{"POST", "/jobs"},
		{"POST", "/cancel"},
		{"POST", "/artifacts"},
	} {
		if got := call(t, server, route.method, route.path, "", `{"id":"r1","action":"ok"}`).StatusCode; got != http.StatusUnauthorized {
			t.Errorf("%s %s without a bearerToken answered %d, want 401", route.method, route.path, got)
		}
		if got := call(t, server, route.method, route.path, "wrong", `{"id":"r1","action":"ok"}`).StatusCode; got != http.StatusUnauthorized {
			t.Errorf("%s %s with the wrong bearerToken answered %d, want 401", route.method, route.path, got)
		}
	}
}

func TestAnActionTheProgramDoesNotKnowIsRefusedAsUnprocessable(t *testing.T) {
	_, server := agent(t)
	// 422 and not 404: the route exists and the request was understood. The node
	// is refusing the work, which is a different thing from not having a route.
	if got := call(t, server, "POST", "/jobs", bearerToken, `{"id":"r1","action":"whatever"}`).StatusCode; got != http.StatusUnprocessableEntity {
		t.Fatalf("an unknown action answered %d, want 422", got)
	}
}

func TestARunIDThatCouldNameAPathIsRefused(t *testing.T) {
	_, server := agent(t)
	for _, id := range []string{"", "../escape", "a/b", "with space", strings.Repeat("x", 65)} {
		body, _ := json.Marshal(map[string]string{"id": id, "action": "ok"})
		if got := call(t, server, "POST", "/jobs", bearerToken, string(body)).StatusCode; got != http.StatusBadRequest {
			t.Errorf("run id %q answered %d, want 400", id, got)
		}
	}
}

func TestTheNodeHoldsOneRunAtATime(t *testing.T) {
	a, server := agent(t)
	if got := call(t, server, "POST", "/jobs", bearerToken, `{"id":"first","action":"sleep"}`).StatusCode; got != http.StatusAccepted {
		t.Fatalf("the first submission answered %d, want 202", got)
	}
	if got := call(t, server, "POST", "/jobs", bearerToken, `{"id":"second","action":"ok"}`).StatusCode; got != http.StatusConflict {
		t.Fatalf("a submission while running answered %d, want 409", got)
	}
	if got := call(t, server, "POST", "/cancel", bearerToken, `{"id":"second","action":"ok"}`).StatusCode; got != http.StatusConflict {
		t.Fatalf("cancelling a run that is not the current one answered %d, want 409", got)
	}
	if got := call(t, server, "POST", "/cancel", bearerToken, `{"id":"first","action":"sleep"}`).StatusCode; got != http.StatusAccepted {
		t.Fatalf("cancelling the current run answered %d, want 202", got)
	}
	// Cancelling is asynchronous: the signal is sent and the reaper closes the
	// log afterwards. Ending the test here leaves the temporary directory being
	// written to while the harness removes it, which fails the run for a reason
	// that has nothing to do with what it was proving.
	settle(t, a, "first")
}

func TestAReusedRunIDIsRefusedRatherThanOverwritingItsLog(t *testing.T) {
	a, server := agent(t)
	if got := call(t, server, "POST", "/jobs", bearerToken, `{"id":"once","action":"ok"}`).StatusCode; got != http.StatusAccepted {
		t.Fatalf("the first submission answered %d, want 202", got)
	}
	settle(t, a, "once")
	if got := call(t, server, "POST", "/jobs", bearerToken, `{"id":"once","action":"ok"}`).StatusCode; got != http.StatusConflict {
		t.Fatalf("reusing a run id answered %d, want 409", got)
	}
}

func TestHowARunEndedIsRecordedBothInMemoryAndBesideItsLog(t *testing.T) {
	a, server := agent(t)
	call(t, server, "POST", "/jobs", bearerToken, `{"id":"failing","action":"bad"}`)
	run := settle(t, a, "failing")
	if run.State != fleet.StateFailed || run.Exit != 3 {
		t.Fatalf("the run reported state %q exit %d, want failed and 3", run.State, run.Exit)
	}
	record, err := os.ReadFile(filepath.Join(a.LogDir, "failing.json"))
	if err != nil {
		t.Fatal(err)
	}
	var written fleet.NodeRun
	if err := json.Unmarshal(record, &written); err != nil {
		t.Fatal(err)
	}
	if written != run {
		t.Fatalf("the record beside the log says %+v, the agent says %+v", written, run)
	}
}

// settle waits for the reaper, which runs in its own goroutine, and returns what
// the node ended up reporting.
func settle(t *testing.T, a *fleet.Agent, id string) fleet.NodeRun {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if run := a.Current(); run.ID == id && run.State != fleet.StateRunning {
			return run
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("run %s never left the running state", id)
	return fleet.NodeRun{}
}

func TestStatusReportsTheIdentityTheHostDeclaredWithoutLettingItHideTheRun(t *testing.T) {
	a, server := agent(t)
	a.Identity = map[string]any{"node": "five", "role": "train", "job": "an attempt to hide the run"}
	var answer map[string]any
	if err := json.NewDecoder(call(t, server, "GET", "/status", bearerToken, "").Body).Decode(&answer); err != nil {
		t.Fatal(err)
	}
	if answer["node"] != "five" || answer["role"] != "train" {
		t.Fatalf("status did not report the declared identity: %+v", answer)
	}
	if _, ok := answer["job"].(map[string]any); !ok {
		t.Fatalf("the identity overwrote the run in status: %+v", answer["job"])
	}
}

func TestANodeWithNoArtifactStoreReceivesNothing(t *testing.T) {
	_, server := agent(t)
	if got := call(t, server, "POST", "/artifacts?dir=d&name=f&sha256="+strings.Repeat("a", 64), bearerToken, "x").StatusCode; got != http.StatusNotFound {
		t.Fatalf("a node with no store answered %d, want 404", got)
	}
}

func withStore(t *testing.T) (*fleet.Agent, *httptest.Server, string) {
	t.Helper()
	a, server := agent(t)
	root := t.TempDir()
	a.Artifacts = &fleet.ArtifactStore{Root: root, Accept: func(name string) bool { return name == "adapter.safetensors" }}
	return a, server, root
}

func TestAnArtifactIsWrittenOnlyWhenItsBytesMatchTheDeclaredDigest(t *testing.T) {
	_, server, root := withStore(t)
	body := "the bytes of an adapter"
	sum := sha256.Sum256([]byte(body))
	good := hex.EncodeToString(sum[:])
	target := filepath.Join(root, "step-8", "adapter.safetensors")

	wrong := strings.Repeat("b", 64)
	if got := call(t, server, "POST", "/artifacts?dir=step-8&name=adapter.safetensors&sha256="+wrong, bearerToken, body).StatusCode; got != http.StatusUnprocessableEntity {
		t.Fatalf("a mismatched digest answered %d, want 422", got)
	}
	// The file is removed rather than left in place: an artifact that looks
	// complete and is not is worse than an absent one, because the next run reads
	// it and produces a number nobody can trust.
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("the artifact survived a digest mismatch: %v", err)
	}
	if got := call(t, server, "POST", "/artifacts?dir=step-8&name=adapter.safetensors&sha256="+good, bearerToken, body).StatusCode; got != http.StatusCreated {
		t.Fatalf("a matching digest answered %d, want 201", got)
	}
	written, err := os.ReadFile(target)
	if err != nil || string(written) != body {
		t.Fatalf("the artifact on disk is %q (%v), want %q", written, err, body)
	}
}

func TestTheArtifactStoreRefusesNamesThatCouldLeaveItsRoot(t *testing.T) {
	_, server, root := withStore(t)
	digest := strings.Repeat("a", 64)
	for _, q := range []string{
		"dir=../escape&name=adapter.safetensors&sha256=" + digest,
		"dir=a/b&name=adapter.safetensors&sha256=" + digest,
		"dir=step-8&name=../adapter.safetensors&sha256=" + digest,
		"dir=step-8&name=something_else.bin&sha256=" + digest,
		"dir=step-8&name=adapter.safetensors&sha256=short",
		"dir=step-8&name=adapter.safetensors",
	} {
		if got := call(t, server, "POST", "/artifacts?"+q, bearerToken, "x").StatusCode; got != http.StatusBadRequest {
			t.Errorf("%s answered %d, want 400", q, got)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("a refused upload still created %d entries under the root", len(entries))
	}
}

func TestAnUploadLargerThanTheCeilingIsRefused(t *testing.T) {
	a, server, root := withStore(t)
	a.Artifacts.MaxBytes = 8
	if got := call(t, server, "POST", "/artifacts?dir=step-8&name=adapter.safetensors&sha256="+strings.Repeat("a", 64), bearerToken, strings.Repeat("x", 64)).StatusCode; got != http.StatusBadRequest {
		t.Fatalf("an oversized upload answered %d, want 400", got)
	}
	if _, err := os.Stat(filepath.Join(root, "step-8", "adapter.safetensors")); !os.IsNotExist(err) {
		t.Fatalf("an oversized upload left a file behind: %v", err)
	}
}

func TestAnAgentRefusesToBeBuiltWithoutWhatItsGuardsNeed(t *testing.T) {
	dir := t.TempDir()
	if _, err := fleet.NewAgent("", echo(t), dir); err == nil {
		t.Error("an agent with no bearerToken was built")
	}
	if _, err := fleet.NewAgent(bearerToken, nil, dir); err == nil {
		t.Error("an agent with no Program was built")
	}
	if _, err := fleet.NewAgent(bearerToken, echo(t), ""); err == nil {
		t.Error("an agent with no log directory was built")
	}
}

// TestBothHalvesOfTheProtocolAgreeOnTheWire is why the agent lives in this
// module: what HTTPWorker sends is what Agent reads, proven rather than assumed.
func TestBothHalvesOfTheProtocolAgreeOnTheWire(t *testing.T) {
	sent, err := json.Marshal(fleet.Job{ID: "r1", Action: "train", Request: json.RawMessage(`{"epochs":2}`)})
	if err != nil {
		t.Fatal(err)
	}
	var read fleet.Job
	if err := json.Unmarshal(sent, &read); err != nil {
		t.Fatal(err)
	}
	if read.ID != "r1" || read.Action != "train" || string(read.Request) != `{"epochs":2}` {
		t.Fatalf("the job did not survive the wire: %+v", read)
	}
}
