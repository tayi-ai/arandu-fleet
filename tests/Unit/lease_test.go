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

// What a queue that nodes fetch from has to get right.
//
// The whole reason to invert the protocol is that a host owning cards then opens
// no port. That buys nothing if the queue loses work, hands the same job to two
// nodes, or lets one node close another's run -- so these are about the queue
// being trustworthy enough to be worth the inversion.

func queueWith(t *testing.T, jobs ...string) *fleet.Queue {
	t.Helper()
	q := fleet.NewQueue(time.Minute)
	for _, id := range jobs {
		if err := q.Submit(fleet.Job{ID: id, Action: "diagnostics"}); err != nil {
			t.Fatal(err)
		}
	}
	return q
}

func TestAJobIsHandedToExactlyOneNode(t *testing.T) {
	q := queueWith(t, "run-1")
	first, err := q.Claim("five")
	if err != nil {
		t.Fatal(err)
	}
	if first.Job.ID != "run-1" || first.Node != "five" {
		t.Fatalf("the lease came out as %+v", first)
	}
	// The second node asks while the first still holds it. Handing the job out
	// twice would run the same training step on two hosts, and the second would
	// fail on a log file the first already created -- if it were lucky.
	if _, err := q.Claim("three"); err == nil {
		t.Fatal("a second node was given a job that was already leased")
	}
}

func TestAnAbandonedLeaseComesBack(t *testing.T) {
	q := queueWith(t, "run-1")
	now := time.Now()
	q.Now = func() time.Time { return now }
	if _, err := q.Claim("five"); err != nil {
		t.Fatal(err)
	}
	// A node that died holding a lease and a node that is slow look identical
	// from here, so the deadline decides. Without it the job is lost, and losing
	// work silently is worse than running it twice.
	now = now.Add(2 * time.Minute)
	lease, err := q.Claim("three")
	if err != nil {
		t.Fatalf("the abandoned job did not come back: %v", err)
	}
	if lease.Job.ID != "run-1" || lease.Node != "three" {
		t.Fatalf("the reclaimed lease is %+v", lease)
	}
}

func TestAReclaimedJobGoesToTheFrontOfTheQueue(t *testing.T) {
	q := queueWith(t, "run-1")
	now := time.Now()
	q.Now = func() time.Time { return now }
	if _, err := q.Claim("five"); err != nil {
		t.Fatal(err)
	}
	if err := q.Submit(fleet.Job{ID: "run-2", Action: "diagnostics"}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	// A job that has already waited once should not queue behind everything
	// submitted since; otherwise a node that keeps dying starves its own work.
	lease, err := q.Claim("three")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Job.ID != "run-1" {
		t.Fatalf("the queue handed out %s; the reclaimed job should come first", lease.Job.ID)
	}
}

func TestOnlyTheNodeHoldingTheLeaseMayCloseIt(t *testing.T) {
	q := queueWith(t, "run-1")
	if _, err := q.Claim("five"); err != nil {
		t.Fatal(err)
	}
	// Without this check any node could close another's run, and an operator
	// would read a completion that never happened.
	if err := q.Report(fleet.Outcome{JobID: "run-1", Node: "three", State: "succeeded"}); err == nil {
		t.Fatal("a node closed a run it was not holding")
	}
	if err := q.Report(fleet.Outcome{JobID: "run-1", Node: "five", State: "succeeded"}); err != nil {
		t.Fatalf("the holder could not close its own run: %v", err)
	}
	if _, done := q.Outcome("run-1"); !done {
		t.Fatal("the outcome was accepted and then not recorded")
	}
}

func TestReportingAJobNobodyHoldsIsRefused(t *testing.T) {
	q := fleet.NewQueue(time.Minute)
	if err := q.Report(fleet.Outcome{JobID: "never-existed", Node: "five", State: "succeeded"}); err == nil {
		t.Fatal("an outcome for a job nobody leased was accepted")
	}
}

func TestTheQueueRefusesAJobItIsAlreadyHolding(t *testing.T) {
	q := queueWith(t, "run-1")
	if err := q.Submit(fleet.Job{ID: "run-1", Action: "diagnostics"}); err == nil {
		t.Fatal("the same run id was queued twice; two nodes would write the same log")
	}
	if _, err := q.Claim("five"); err != nil {
		t.Fatal(err)
	}
	if err := q.Submit(fleet.Job{ID: "run-1", Action: "diagnostics"}); err == nil {
		t.Fatal("a run id that is out on lease was queued again")
	}
}

func TestARunIDThatCouldNameAPathIsRefusedByTheQueueToo(t *testing.T) {
	q := fleet.NewQueue(time.Minute)
	// The id names a log file on whichever node claims it, so the queue refuses
	// what the agent would refuse. Catching it here means the job never travels.
	for _, id := range []string{"", "../escape", "a/b", strings.Repeat("x", 65)} {
		if err := q.Submit(fleet.Job{ID: id, Action: "diagnostics"}); err == nil {
			t.Errorf("the queue accepted %q as a run id", id)
		}
	}
}

func TestAnEmptyQueueIsNotAnError(t *testing.T) {
	handler, err := fleet.LeaseHandler(fleet.NewQueue(time.Minute), "a-token")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	// 204 and not 404. A node polling an idle fleet would otherwise log an error
	// a minute, and an error that is normal is an error nobody reads.
	answer := postAs(t, server, "/lease", "a-token", `{"node":"five"}`)
	if answer.StatusCode != http.StatusNoContent {
		t.Fatalf("an empty queue answered %d, want 204", answer.StatusCode)
	}
}

func TestTheControlPlaneRefusesWithoutTheToken(t *testing.T) {
	handler, err := fleet.LeaseHandler(queueWith(t, "run-1"), "a-token")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	for _, path := range []string{"/lease", "/report"} {
		for _, token := range []string{"", "wrong"} {
			if got := postAs(t, server, path, token, `{"node":"five"}`).StatusCode; got != http.StatusUnauthorized {
				t.Errorf("%s with token %q answered %d, want 401", path, token, got)
			}
		}
	}
}

func TestAControlPlaneWithNoTokenIsRefused(t *testing.T) {
	if _, err := fleet.LeaseHandler(fleet.NewQueue(time.Minute), ""); err == nil {
		t.Fatal("a lease handler with no token was built; it would hand work to anyone who asked")
	}
	if _, err := fleet.LeaseHandler(nil, "a-token"); err == nil {
		t.Fatal("a lease handler with no queue was built")
	}
}

func TestALeaserRefusesToBeBuiltWithoutWhatItNeeds(t *testing.T) {
	for name, build := range map[string]func() (*fleet.Leaser, error){
		"no address": func() (*fleet.Leaser, error) { return fleet.NewLeaser("", "five", "t", nil, 0) },
		"no node":    func() (*fleet.Leaser, error) { return fleet.NewLeaser("http://x", "", "t", nil, 0) },
		"no token":   func() (*fleet.Leaser, error) { return fleet.NewLeaser("http://x", "five", "", nil, 0) },
		"bad pin": func() (*fleet.Leaser, error) {
			return fleet.NewLeaser("https://x", "five", "t", []byte("not a certificate"), 0)
		},
	} {
		if _, err := build(); err == nil {
			t.Errorf("%s: a leaser was built anyway", name)
		}
	}
}

func TestANodeClaimsAndReportsOverTheWire(t *testing.T) {
	queue := queueWith(t, "run-1")
	handler, err := fleet.LeaseHandler(queue, "a-token")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	leaser, err := fleet.NewLeaser(server.URL, "five", "a-token", nil, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := leaser.Claim(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if lease.Job.ID != "run-1" {
		t.Fatalf("the node claimed %+v", lease)
	}
	if err := leaser.Report(t.Context(), fleet.Outcome{JobID: "run-1", State: "succeeded"}); err != nil {
		t.Fatal(err)
	}
	// The leaser fills in its own node name, so a node cannot report as another
	// even by mistake.
	outcome, done := queue.Outcome("run-1")
	if !done || outcome.Node != "five" {
		t.Fatalf("the queue recorded %+v", outcome)
	}
	if _, err := leaser.Claim(t.Context()); err == nil {
		t.Fatal("the queue handed out the same job after it was reported")
	}
}

func TestDepthSaysWhatIsWaitingAndWhatIsOut(t *testing.T) {
	q := queueWith(t, "run-1", "run-2", "run-3")
	if pending, leased := q.Depth(); pending != 3 || leased != 0 {
		t.Fatalf("a fresh queue reports %d pending and %d leased", pending, leased)
	}
	if _, err := q.Claim("five"); err != nil {
		t.Fatal(err)
	}
	// This is what an operator asks when a fleet looks idle: is there no work, or
	// is it all out on leases nobody is finishing.
	if pending, leased := q.Depth(); pending != 2 || leased != 1 {
		t.Fatalf("after one claim the queue reports %d pending and %d leased", pending, leased)
	}
}

func postAs(t *testing.T, server *httptest.Server, path, token, body string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	answer, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { answer.Body.Close() })
	return answer
}

func TestALeaseSurvivesTheWire(t *testing.T) {
	// Both halves of this protocol live in one module for the same reason the
	// agent does: a field added on one end cannot go missing on the other.
	sent, err := json.Marshal(fleet.Lease{
		Job:      fleet.Job{ID: "run-1", Action: "train", Request: json.RawMessage(`{"epochs":2}`)},
		Node:     "five",
		Deadline: time.Now().Add(time.Minute).UTC().Truncate(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	var read fleet.Lease
	if err := json.Unmarshal(sent, &read); err != nil {
		t.Fatal(err)
	}
	if read.Job.ID != "run-1" || read.Node != "five" || string(read.Job.Request) != `{"epochs":2}` {
		t.Fatalf("the lease did not survive the wire: %+v", read)
	}
	if read.Deadline.IsZero() {
		t.Fatal("the deadline was lost, and a lease without one never expires")
	}
}

// TestMaterialIsAddressedByWhatItContains covers the gap a dataset fell into:
// it reached one node by hand and the program that read it checked the digest.
// The check catches a wrong copy; nothing put the right one there.
func TestMaterialIsAddressedByWhatItContains(t *testing.T) {
	dir := t.TempDir()
	corpus := filepath.Join(dir, "tokenized.jsonl")
	body := []byte(`{"id":"repair-skeleton-validation"}` + "\n")
	if err := os.WriteFile(corpus, body, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])

	published, err := fleet.Publish(corpus)
	if err != nil {
		t.Fatal(err)
	}
	if published[digest] != corpus {
		t.Fatalf("Publish indexed %v; the digest of the file is %s", published, digest)
	}

	handler, err := fleet.MaterialHandler(published, "a-token")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	leaser, err := fleet.NewLeaser(server.URL, "five", "a-token", nil, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	into := t.TempDir()
	path, err := leaser.Fetch(t.Context(), into, fleet.Material{
		Name: "tokenized", SHA256: digest, Bytes: int64(len(body)),
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(body) {
		t.Fatalf("the fetched material is %q (%v)", got, err)
	}
}

func TestMaterialThatDoesNotHashAsPromisedIsNotKept(t *testing.T) {
	dir := t.TempDir()
	corpus := filepath.Join(dir, "corpus")
	if err := os.WriteFile(corpus, []byte("the real thing"), 0o644); err != nil {
		t.Fatal(err)
	}
	published, err := fleet.Publish(corpus)
	if err != nil {
		t.Fatal(err)
	}
	// The control plane is asked for a digest it has, but the caller declares a
	// different one. Nothing may land: a partial or wrong dataset is worse than
	// an absent one, because the trainer reads it and the loss comes out
	// plausible.
	var served string
	for digest := range published {
		served = digest
	}
	handler, err := fleet.MaterialHandler(published, "a-token")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	leaser, err := fleet.NewLeaser(server.URL, "five", "a-token", nil, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	into := t.TempDir()
	if _, err := leaser.Fetch(t.Context(), into, fleet.Material{Name: "corpus", SHA256: served, Bytes: 999}); err == nil {
		t.Fatal("material of the wrong length was accepted")
	}
	entries, _ := os.ReadDir(into)
	if len(entries) != 0 {
		t.Fatalf("a refused fetch left %d files behind", len(entries))
	}
}

func TestMaterialWithoutADigestIsRefusedBeforeAnythingIsFetched(t *testing.T) {
	leaser, err := fleet.NewLeaser("http://127.0.0.1:1", "five", "a-token", nil, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	into := t.TempDir()
	for name, material := range map[string]fleet.Material{
		"no digest":     {Name: "corpus"},
		"short digest":  {Name: "corpus", SHA256: "abc"},
		"name climbs":   {Name: "../escape", SHA256: strings.Repeat("a", 64)},
		"name is empty": {SHA256: strings.Repeat("a", 64)},
	} {
		if _, err := leaser.Fetch(t.Context(), into, material); err == nil {
			t.Errorf("%s: the fetch was attempted", name)
		}
	}
}

func TestANodeThatAlreadyHasTheRightBytesFetchesNothing(t *testing.T) {
	body := []byte("already here")
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	into := t.TempDir()
	if err := os.WriteFile(filepath.Join(into, "corpus"), body, 0o644); err != nil {
		t.Fatal(err)
	}
	// The control plane is deliberately unreachable. On a fleet this is the
	// common case -- most nodes already hold the release -- and the digest is
	// what makes skipping the download safe.
	leaser, err := fleet.NewLeaser("http://127.0.0.1:1", "five", "a-token", nil, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leaser.Fetch(t.Context(), into, fleet.Material{Name: "corpus", SHA256: digest, Bytes: int64(len(body))}); err != nil {
		t.Fatalf("a node holding the right bytes tried to fetch anyway: %v", err)
	}
}
