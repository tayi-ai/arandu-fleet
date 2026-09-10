package fleet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// The third shape of the protocol: the node dials, and nothing listens on it.
//
// HTTPWorker and Agent are the two halves of a fleet the control plane reaches
// by address. That design puts one listening socket on every host that owns
// cards, and on a fleet of twenty-one it puts twenty-one. Each of them accepts a
// connection from anything that can route to it, and a bearer token is the whole
// lock: the node cannot tell who is calling, and plain HTTP means neither end
// can verify the other.
//
// Leasing inverts it. The node asks the control plane for work and reports what
// happened, so it opens no port at all. Three things follow, and only the first
// is about ports:
//
//   - The fleet's listening sockets go from one per node to one in total. What
//     is left is the control plane, which was already reachable.
//   - The node dials, so the node can verify who it is talking to. A pinned
//     certificate makes an impostor control plane unusable, which nothing in the
//     push design could do.
//   - A compromised host cannot be commanded. It has no inbound surface to
//     command through; it fetches, and it fetches from one place.
//
// What it costs: a job is picked up on the next poll rather than the moment it
// is dispatched, and the control plane has to hold the queue. Both halves are
// kept here so the shapes cannot drift apart, the same reason Agent lives beside
// HTTPWorker.

// Lease is one node's claim on one job.
//
// The deadline is the point. A node that takes a job and dies leaves it claimed,
// and a queue that cannot tell "in progress" from "abandoned" either loses the
// work or runs it twice. The claim expires, and expiry is what makes the second
// choice unnecessary.
type Lease struct {
	Job      Job       `json:"job"`
	Node     string    `json:"node"`
	Deadline time.Time `json:"deadline"`
}

// Outcome is what a node reports when the job is over.
type Outcome struct {
	JobID string `json:"job_id"`
	Node  string `json:"node"`
	State string `json:"state"`
	Exit  int    `json:"exit"`
	// Detail carries whatever the node wants the operator to read. It is not
	// parsed here: this module owns the protocol, not the vocabulary.
	Detail string `json:"detail,omitempty"`
}

// Queue is the work the control plane is holding.
//
// It is in memory on purpose, and that is a limitation worth stating rather than
// hiding: a control plane that restarts forgets what was leased, and every node
// holding a lease finishes work nobody is waiting for. The alternative is a
// table, and a table is the application's decision -- this module would have to
// own a schema to make it, and owning a schema is how a module stops being
// composable.
type Queue struct {
	mu        sync.Mutex
	pending   []Job
	leased    map[string]*Lease
	completed map[string]Outcome
	// reported is the order outcomes arrived in, so the oldest can be dropped.
	// A control plane that runs for a month holds every outcome of that month
	// otherwise, and the queue is in memory: nothing evicts it and nothing
	// reports that it grew. The bound is what makes "in memory" a decision
	// rather than a leak.
	reported []string
	// Lease is how long a claim survives without a report.
	LeaseFor time.Duration
	// Now is the clock, so a test can move it.
	Now func() time.Time
}

// NewQueue returns an empty queue with the timings a fleet of this shape needs.
func NewQueue(leaseFor time.Duration) *Queue {
	if leaseFor <= 0 {
		// Long enough for a training step measured at 11.86 s and for a model
		// load measured at 5.3 s, with room for a node that is swapping.
		leaseFor = 10 * time.Minute
	}
	return &Queue{
		leased:    map[string]*Lease{},
		completed: map[string]Outcome{},
		LeaseFor:  leaseFor,
		Now:       time.Now,
	}
}

// keptOutcomes bounds what Outcome can still answer for.
//
// A fleet of twenty-one nodes reporting a job each is twenty-one entries per
// round, so this is roughly the last five hundred rounds -- far longer than
// anybody waits before reading a result, and small enough that the queue's
// memory does not depend on how long the control plane has been up.
//
// Asking for an outcome older than this reads as not reported, which is the
// same answer a restarted control plane gives. A caller that needs a result to
// survive either has to write it down, and writing it down means a table,
// which is the application's decision and not this module's.
const keptOutcomes = 10000

// ErrNoWork is returned when the queue has nothing for a node.
//
// It is an error rather than a nil job so a caller cannot forget to check. A
// zero Job has an empty action, and an agent handed one would ask its Program
// what to run for "", which is a question with no good answer.
var ErrNoWork = errors.New("fleet: no work is waiting")

// Submit puts a job on the queue.
func (q *Queue) Submit(job Job) error {
	if !validRunID.MatchString(job.ID) {
		return fmt.Errorf("fleet: %q is not a usable run id", job.ID)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, waiting := range q.pending {
		if waiting.ID == job.ID {
			return fmt.Errorf("fleet: run %s is already queued", job.ID)
		}
	}
	if _, held := q.leased[job.ID]; held {
		return fmt.Errorf("fleet: run %s is already leased", job.ID)
	}
	q.pending = append(q.pending, job)
	return nil
}

// Claim hands one job to a node, or reports that there is none.
//
// Expired leases are returned to the queue first. A node that died holding one
// is indistinguishable from a node that is slow, so the deadline decides rather
// than a heartbeat: a heartbeat is another thing to get wrong, and a lease that
// expires wrongly costs one repeated job.
func (q *Queue) Claim(node string) (Lease, error) {
	if node == "" {
		return Lease{}, errors.New("fleet: a claim has to say which node is making it")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.reclaimExpired()
	if len(q.pending) == 0 {
		return Lease{}, ErrNoWork
	}
	job := q.pending[0]
	q.pending = q.pending[1:]
	lease := &Lease{Job: job, Node: node, Deadline: q.Now().Add(q.LeaseFor)}
	q.leased[job.ID] = lease
	return *lease, nil
}

// reclaimExpired moves abandoned leases back to the front of the queue.
//
// The front and not the back: a job that has already waited once should not
// queue behind everything submitted since.
func (q *Queue) reclaimExpired() {
	now := q.Now()
	for id, lease := range q.leased {
		if now.Before(lease.Deadline) {
			continue
		}
		delete(q.leased, id)
		q.pending = append([]Job{lease.Job}, q.pending...)
	}
}

// Report records how a job ended and releases its lease.
//
// A report from a node that does not hold the lease is refused. Without that
// check any node could close another node's job, and the operator would read a
// completion that never happened.
func (q *Queue) Report(outcome Outcome) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	lease, held := q.leased[outcome.JobID]
	if !held {
		return fmt.Errorf("fleet: run %s is not leased by anybody", outcome.JobID)
	}
	if lease.Node != outcome.Node {
		return fmt.Errorf("fleet: run %s is leased by %s, not by %s", outcome.JobID, lease.Node, outcome.Node)
	}
	delete(q.leased, outcome.JobID)
	if _, seen := q.completed[outcome.JobID]; !seen {
		q.reported = append(q.reported, outcome.JobID)
	}
	q.completed[outcome.JobID] = outcome
	for len(q.reported) > keptOutcomes {
		delete(q.completed, q.reported[0])
		q.reported = q.reported[1:]
	}
	return nil
}

// Outcome reads what a job reported, if it has.
func (q *Queue) Outcome(id string) (Outcome, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	outcome, done := q.completed[id]
	return outcome, done
}

// Depth reports what is waiting and what is out, which is what an operator asks
// when a fleet looks idle.
func (q *Queue) Depth() (pending, leased int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.reclaimExpired()
	return len(q.pending), len(q.leased)
}

// LeaseHandler is the control plane's side: the one place a node dials.
//
// It is the only listening socket the fleet needs. The token is compared in
// constant time, as it is on the agent, and the node names itself in the claim
// so a report can be matched against the lease that authorised it.
func LeaseHandler(queue *Queue, token string) (stdhttp.Handler, error) {
	if queue == nil {
		return nil, errors.New("fleet: the lease handler needs a queue")
	}
	if token == "" {
		return nil, errors.New("fleet: the lease handler needs a token; refusing to hand out work without one")
	}
	mux := stdhttp.NewServeMux()
	mux.HandleFunc("POST /lease", func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		var request struct {
			Node string `json:"node"`
		}
		if json.NewDecoder(stdhttp.MaxBytesReader(w, r.Body, maxJob)).Decode(&request) != nil {
			stdhttp.Error(w, "invalid claim", stdhttp.StatusBadRequest)
			return
		}
		lease, err := queue.Claim(request.Node)
		if errors.Is(err, ErrNoWork) {
			// 204 rather than 404: there is nothing wrong with an empty queue, and
			// a node that polls an idle fleet should not log an error a minute.
			w.WriteHeader(stdhttp.StatusNoContent)
			return
		}
		if err != nil {
			stdhttp.Error(w, err.Error(), stdhttp.StatusBadRequest)
			return
		}
		answerJSON(w, stdhttp.StatusOK, lease)
	})
	mux.HandleFunc("POST /report", func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		var outcome Outcome
		if json.NewDecoder(stdhttp.MaxBytesReader(w, r.Body, maxJob)).Decode(&outcome) != nil {
			stdhttp.Error(w, "invalid outcome", stdhttp.StatusBadRequest)
			return
		}
		if err := queue.Report(outcome); err != nil {
			stdhttp.Error(w, err.Error(), stdhttp.StatusConflict)
			return
		}
		w.WriteHeader(stdhttp.StatusAccepted)
	})

	want := []byte("Bearer " + token)
	return stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			stdhttp.Error(w, "unauthorized", stdhttp.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	}), nil
}

// Leaser is a node fetching its own work.
//
// It opens no port. Everything it does is an outbound request to one address,
// which is what lets a host that owns cards have no inbound surface at all.
type Leaser struct {
	// BaseURL is the control plane. It is the only address this node speaks to.
	BaseURL string
	// Node is who this is, and it has to match what the inventory calls it, or a
	// report will not match the lease that authorised it.
	Node   string
	Token  string
	Client *stdhttp.Client
	// Poll is how long to wait between empty answers. A job is picked up within
	// this, which is the price of having no listening socket.
	Poll time.Duration
}

// NewLeaser returns a node-side client, optionally pinned to a certificate.
//
// Pinning is the capability this design buys and the push design could not have:
// the node initiates, so the node can refuse to speak to anything that is not
// the control plane it was given. An empty pin means system roots, which is a
// weaker claim honestly stated rather than a stronger one implied.
func NewLeaser(baseURL, node, token string, pin []byte, poll time.Duration) (*Leaser, error) {
	if baseURL == "" || node == "" || token == "" {
		return nil, errors.New("fleet: a leaser needs a control plane address, a node name and a token")
	}
	if poll <= 0 {
		poll = 5 * time.Second
	}
	client := &stdhttp.Client{Timeout: 30 * time.Second}
	if len(pin) > 0 {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pin) {
			return nil, errors.New("fleet: the pinned certificate is not readable as PEM")
		}
		client.Transport = &stdhttp.Transport{TLSClientConfig: &tls.Config{
			RootCAs:    roots,
			MinVersion: tls.VersionTLS12,
		}}
	}
	return &Leaser{BaseURL: baseURL, Node: node, Token: token, Client: client, Poll: poll}, nil
}

// Claim asks for one job. It returns ErrNoWork when the queue is empty.
func (l *Leaser) Claim(ctx context.Context) (Lease, error) {
	body, err := json.Marshal(map[string]string{"node": l.Node})
	if err != nil {
		return Lease{}, err
	}
	answer, status, err := l.post(ctx, "/lease", body)
	if err != nil {
		return Lease{}, err
	}
	if status == stdhttp.StatusNoContent {
		return Lease{}, ErrNoWork
	}
	var lease Lease
	if err := json.Unmarshal(answer, &lease); err != nil {
		return Lease{}, fmt.Errorf("fleet: reading the lease: %w", err)
	}
	return lease, nil
}

// Report says how it went.
func (l *Leaser) Report(ctx context.Context, outcome Outcome) error {
	outcome.Node = l.Node
	body, err := json.Marshal(outcome)
	if err != nil {
		return err
	}
	_, _, err = l.post(ctx, "/report", body)
	return err
}

func (l *Leaser) post(ctx context.Context, path string, body []byte) ([]byte, int, error) {
	request, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodPost, l.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	request.Header.Set("Authorization", "Bearer "+l.Token)
	request.Header.Set("Content-Type", "application/json")
	answer, err := l.Client.Do(request)
	if err != nil {
		return nil, 0, fmt.Errorf("fleet: reaching the control plane at %s: %w", path, err)
	}
	defer answer.Body.Close()
	read, err := io.ReadAll(io.LimitReader(answer.Body, maxAnswer))
	if err != nil {
		return nil, answer.StatusCode, err
	}
	if answer.StatusCode >= 300 && answer.StatusCode != stdhttp.StatusNoContent {
		return read, answer.StatusCode, fmt.Errorf("fleet: the control plane answered HTTP %d on %s: %s", answer.StatusCode, path, read)
	}
	return read, answer.StatusCode, nil
}

// Work runs the loop: claim, run, report, repeat, until the context ends.
//
// The outcome is reported even when the job failed, and that is the whole point
// of reporting separately from running. A node that only reported successes
// would leave every failure looking like a node that went quiet.
func (l *Leaser) Work(ctx context.Context, agent *Agent, each func(Lease, Outcome)) error {
	if agent == nil {
		return errors.New("fleet: the worker loop needs an agent to run the jobs it claims")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		lease, err := l.Claim(ctx)
		if errors.Is(err, ErrNoWork) {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(l.Poll):
			}
			continue
		}
		if err != nil {
			// A control plane that cannot be reached is a reason to wait, not a
			// reason to stop: the node has nothing else to do, and stopping means
			// somebody has to notice and start it again.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(l.Poll):
			}
			continue
		}
		outcome := agent.RunToCompletion(ctx, lease.Job)
		outcome.JobID = lease.Job.ID
		reported := l.Report(ctx, outcome)
		if reported != nil {
			// A report that did not arrive is the worst of the three outcomes and
			// used to be the quietest: the job ran, the control plane never heard,
			// the lease expired, and the fleet did the work twice. It was swallowed
			// whenever a callback was passed, which made the error handling depend
			// on whether the caller wanted to watch -- an arbitrary coupling.
			//
			// It is said out loud now, through the same callback that carries every
			// other outcome, and the loop continues for the reason an unreachable
			// control plane is a reason to wait rather than to stop.
			outcome.State = "unreported"
			outcome.Detail = strings.TrimSpace(outcome.Detail + " | report failed: " + reported.Error())
		}
		if each != nil {
			each(lease, outcome)
		} else if reported != nil {
			return reported
		}
	}
}

// Material is a file a job needs before it can run, named by what it is and
// pinned by what it contains.
//
// A dataset reached these nodes by hand, once, and the digest was checked by the
// program that consumed it. That works on one host and stops working on
// twenty-one: the check catches a node whose copy is wrong, but nothing puts the
// right copy there, so the answer to a failed check is somebody with scp at two
// in the morning.
//
// The digest is the name. Two jobs asking for the same digest ask for the same
// bytes, a node that already has them fetches nothing, and a node that has the
// wrong ones cannot be talked into using them.
type Material struct {
	// Name is where the node puts it, relative to the root the host declares. A
	// name and never a path: the node joins it and checks the result stayed
	// inside.
	Name string `json:"name"`
	// SHA256 is what the contents have to hash to. A job that names material
	// without one is refused rather than trusted.
	SHA256 string `json:"sha256"`
	// Bytes is the expected length, checked before the digest so a truncated
	// download fails on the cheap test.
	Bytes int64 `json:"bytes"`
}

// Fetch downloads one piece of material and verifies it before it is usable.
//
// It is written to a temporary file and renamed only after the digest matches,
// so a reader never opens a partial dataset. A partial dataset is worse than an
// absent one: the trainer reads it, the loss comes out plausible, and nothing
// says which examples were missing.
func (l *Leaser) Fetch(ctx context.Context, root string, want Material) (string, error) {
	if !validArtifactDir.MatchString(want.Name) {
		return "", fmt.Errorf("fleet: %q is not a simple name", want.Name)
	}
	if !validDigest.MatchString(want.SHA256) {
		return "", fmt.Errorf("fleet: material %s declares no usable digest; unverifiable bytes are not material", want.Name)
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	target := filepath.Join(absoluteRoot, want.Name)
	if !strings.HasPrefix(target, absoluteRoot+string(filepath.Separator)) {
		return "", fmt.Errorf("fleet: %s would leave the material root", want.Name)
	}

	// A node that already holds the right bytes fetches nothing. On a fleet this
	// is most nodes most of the time, and the digest is what makes the check
	// trustworthy enough to skip the download.
	if existing, err := digestOf(target); err == nil && existing == want.SHA256 {
		return target, nil
	}

	request, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, l.BaseURL+"/material/"+want.SHA256, nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Bearer "+l.Token)
	answer, err := l.Client.Do(request)
	if err != nil {
		return "", fmt.Errorf("fleet: fetching material %s: %w", want.Name, err)
	}
	defer answer.Body.Close()
	if answer.StatusCode != stdhttp.StatusOK {
		return "", fmt.Errorf("fleet: the control plane answered HTTP %d for material %s", answer.StatusCode, want.Name)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return "", err
	}
	temporary, err := os.CreateTemp(filepath.Dir(target), ".material-")
	if err != nil {
		return "", err
	}
	sum := sha256.New()
	limit := want.Bytes
	if limit <= 0 {
		limit = maxMaterialBytes
	}
	written, err := io.Copy(io.MultiWriter(temporary, sum), io.LimitReader(answer.Body, limit+1))
	temporary.Close()
	if err != nil {
		os.Remove(temporary.Name())
		return "", err
	}
	if want.Bytes > 0 && written != want.Bytes {
		os.Remove(temporary.Name())
		return "", fmt.Errorf("fleet: material %s arrived as %d bytes, expected %d", want.Name, written, want.Bytes)
	}
	if got := hex.EncodeToString(sum.Sum(nil)); got != want.SHA256 {
		os.Remove(temporary.Name())
		return "", fmt.Errorf("fleet: material %s hashes to %s, expected %s", want.Name, got, want.SHA256)
	}
	if err := os.Rename(temporary.Name(), target); err != nil {
		os.Remove(temporary.Name())
		return "", err
	}
	return target, nil
}

// maxMaterialBytes caps a download that declares no length. The release this
// fleet trains on is 1.47 MB and the largest adapter is 69 MB; a gigabyte is
// room for something much larger without being room for a filled disk.
const maxMaterialBytes = 1 << 30

func digestOf(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(sum.Sum(nil)), nil
}

// MaterialHandler serves files by digest, from a directory the control plane
// declares.
//
// Addressing by digest rather than by path is what makes this safe to expose:
// there is no name to traverse, a request either matches something the control
// plane published or it does not, and what comes back is what was asked for by
// definition.
func MaterialHandler(published map[string]string, token string) (stdhttp.Handler, error) {
	if token == "" {
		return nil, errors.New("fleet: the material handler needs a token")
	}
	want := []byte("Bearer " + token)
	return stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			stdhttp.Error(w, "unauthorized", stdhttp.StatusUnauthorized)
			return
		}
		digest := strings.TrimPrefix(r.URL.Path, "/material/")
		if !validDigest.MatchString(digest) {
			stdhttp.Error(w, "a digest is the only way to name material", stdhttp.StatusBadRequest)
			return
		}
		path, ok := published[digest]
		if !ok {
			stdhttp.Error(w, "no material with that digest is published", stdhttp.StatusNotFound)
			return
		}
		stdhttp.ServeFile(w, r, path)
	}), nil
}

// Publish reads a directory and indexes what is in it by digest, which is what
// MaterialHandler serves.
//
// Reading the files rather than trusting a manifest means the index cannot drift
// from what is on disk: a control plane that published a digest it does not have
// would hand every node a 404 at the moment they all needed the dataset.
func Publish(paths ...string) (map[string]string, error) {
	index := map[string]string{}
	for _, path := range paths {
		digest, err := digestOf(path)
		if err != nil {
			return nil, fmt.Errorf("fleet: publishing %s: %w", path, err)
		}
		index[digest] = path
	}
	return index, nil
}
