package fleet

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

// Agent is the node half of the worker protocol: what runs on a machine that
// owns cards, answering the same /status, /jobs and /cancel that HTTPWorker
// calls from the control plane.
//
// Both halves live in this module deliberately. A protocol whose ends are
// maintained in two repositories drifts, and the drift is found in the middle
// of a distributed run, when six processes are already holding cards.
//
// The agent knows the shape of a job and nothing about the work. Which actions
// exist, what they execute and under which guards is decided by the Program the
// host installs -- the same inversion Convention makes for identity, for the
// same reason: an installation's vocabulary is not this module's business.
type Agent struct {
	// Identity is what /status reports about the node beside its run. The host
	// fills it with whatever its operators read: the id, the declared role, the
	// cards it measured. The module adds nothing to it and never a credential.
	Identity map[string]any
	// Program decides what an action executes.
	Program Program
	// Token authorizes every request, compared in constant time.
	Token string
	// LogDir is where a run's output and its final record are written, one file
	// per run id.
	LogDir string
	// Artifacts, when set, serves POST /artifacts. A nil store answers 404:
	// receiving files is a capability a node opts into, not a default.
	Artifacts *ArtifactStore

	mu      sync.Mutex
	current NodeRun
	proc    *exec.Cmd
}

// The states a run passes through. A node holds one run at a time, so these are
// also the states of the node.
const (
	StateIdle      = "idle"
	StateRunning   = "running"
	StateSucceeded = "succeeded"
	StateFailed    = "failed"
)

// Command is what a Program decided a job executes.
//
// It is a program and its arguments, never a shell line: a request that could
// carry a command line is a request that could run an arbitrary program on a
// host holding client cards.
type Command struct {
	Path string
	Args []string
	Env  []string
	Dir  string
}

// Program decides what an action runs on this node.
//
// An action it does not implement is an error, and that error is answered 422
// rather than 404: the route exists and the request was understood; the node
// refuses to do what it describes. Every guard that depends on what the work is
// -- the role the node declares, the ceiling on memory, the range a schedule may
// ask for -- belongs here, because only the host knows them.
type Program interface {
	Command(a Job) (Command, error)
}

// ProgramFunc adapts an ordinary function to Program.
type ProgramFunc func(Job) (Command, error)

func (f ProgramFunc) Command(a Job) (Command, error) { return f(a) }

// NodeRun is what one node reports about the job it is holding, as against Run,
// which is a dispatch across the whole fleet. Exit is -1 while it runs, so a
// reader never mistakes a running job for one that ended cleanly.
type NodeRun struct {
	ID     string `json:"id"`
	Action string `json:"action"`
	State  string `json:"state"`
	Exit   int    `json:"exit"`
}

// validRunID is what a run id may look like. It admits no separator and no dot,
// because the id names a log file the agent creates.
var validRunID = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// maxJob caps a submission. A job is a few hundred bytes, and a body larger
// than this is not one.
const maxJob = 8192

// NewAgent returns an agent ready to serve, or refuses to build one that could
// not honor its guards.
func NewAgent(token string, program Program, logDir string) (*Agent, error) {
	if token == "" {
		return nil, errors.New("fleet: the agent token is empty; refusing to serve a node API without one")
	}
	if program == nil {
		return nil, errors.New("fleet: the agent needs a Program; without one it has no vocabulary and would accept any action")
	}
	if logDir == "" {
		return nil, errors.New("fleet: the agent needs a log directory; a run whose output goes nowhere cannot be audited")
	}
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return nil, err
	}
	return &Agent{Program: program, Token: token, LogDir: logDir, current: NodeRun{State: StateIdle, Exit: -1}}, nil
}

// Handler is the node API. The host binds it to the private address it measured
// and decides its timeouts; this module does not open sockets.
func (a *Agent) Handler() stdhttp.Handler {
	mux := stdhttp.NewServeMux()
	mux.HandleFunc("GET /status", a.status)
	mux.HandleFunc("POST /jobs", a.submit)
	mux.HandleFunc("POST /cancel", a.cancel)
	mux.HandleFunc("POST /artifacts", a.receive)
	return a.authorized(mux)
}

// Current reports the run the node is holding, for a host that wants to answer
// its own health route without going through HTTP.
func (a *Agent) Current() NodeRun {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.current
}

func (a *Agent) authorized(next stdhttp.Handler) stdhttp.Handler {
	want := []byte("Bearer " + a.Token)
	return stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			stdhttp.Error(w, "unauthorized", stdhttp.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *Agent) status(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	a.mu.Lock()
	answer := map[string]any{"job": a.current}
	a.mu.Unlock()
	for k, v := range a.Identity {
		if k != "job" {
			answer[k] = v
		}
	}
	answerJSON(w, stdhttp.StatusOK, answer)
}

func (a *Agent) submit(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	job, ok := readJob(w, r)
	if !ok {
		return
	}
	command, err := a.Program.Command(job)
	if err != nil {
		stdhttp.Error(w, err.Error(), stdhttp.StatusUnprocessableEntity)
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current.State == StateRunning {
		stdhttp.Error(w, "busy", stdhttp.StatusConflict)
		return
	}
	// O_EXCL refuses a reused run id rather than overwriting the log of the run
	// that already used it. Two runs sharing one log is two runs nobody can tell
	// apart afterwards.
	log, err := os.OpenFile(filepath.Join(a.LogDir, job.ID+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		stdhttp.Error(w, "cannot create the log for this run id; use a new one", stdhttp.StatusConflict)
		return
	}
	process := exec.Command(command.Path, command.Args...)
	process.Stdout, process.Stderr = log, log
	process.Env, process.Dir = command.Env, command.Dir
	detach(process)
	if err := process.Start(); err != nil {
		log.Close()
		stdhttp.Error(w, err.Error(), stdhttp.StatusInternalServerError)
		return
	}
	a.current = NodeRun{ID: job.ID, Action: job.Action, State: StateRunning, Exit: -1}
	a.proc = process
	go a.reap(process, log, job.ID)
	answerJSON(w, stdhttp.StatusAccepted, a.current)
}

// reap records how a run ended, both in memory and beside its log, so a node
// restarted after a run still says what happened: the in-memory state comes back
// idle, and the file is what remains.
func (a *Agent) reap(process *exec.Cmd, log *os.File, id string) {
	err := process.Wait()
	log.Close()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.current.State, a.current.Exit = StateSucceeded, 0
	if err != nil {
		a.current.State, a.current.Exit = StateFailed, process.ProcessState.ExitCode()
	}
	record, _ := json.Marshal(a.current)
	os.WriteFile(filepath.Join(a.LogDir, id+".json"), record, 0o600)
}

func (a *Agent) cancel(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	job, ok := readJob(w, r)
	if !ok {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current.ID != job.ID || a.current.State != StateRunning {
		stdhttp.Error(w, "no matching active job", stdhttp.StatusConflict)
		return
	}
	// The whole group, not the process: a launcher that spawned one worker per
	// card leaves those workers holding the cards when only its own pid is
	// signalled, and the next run finds the memory already taken.
	if err := killGroup(a.proc); err != nil {
		stdhttp.Error(w, err.Error(), stdhttp.StatusInternalServerError)
		return
	}
	answerJSON(w, stdhttp.StatusAccepted, map[string]string{"state": "cancelling"})
}

func readJob(w stdhttp.ResponseWriter, r *stdhttp.Request) (Job, bool) {
	var a Job
	if json.NewDecoder(stdhttp.MaxBytesReader(w, r.Body, maxJob)).Decode(&a) != nil || !validRunID.MatchString(a.ID) {
		stdhttp.Error(w, "invalid job", stdhttp.StatusBadRequest)
		return Job{}, false
	}
	return a, true
}

func answerJSON(w stdhttp.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// ArtifactStore receives the files a run needs and keeps them under one root.
//
// It exists because work that ends on one node produces bytes every other node
// needs before the next round starts, and moving them by hand between machines
// is how launches are lost: the file is on one host and absent on two.
type ArtifactStore struct {
	// Root is the only directory this store writes under. Every name a request
	// carries is joined to it and the result checked to still be under it.
	Root string
	// Accept decides which file names are admitted. A store without one admits
	// nothing, on purpose: a node is not a file server, and the host is the only
	// party that knows which files its work produces.
	Accept func(name string) bool
	// MaxBytes caps one upload. Zero means the default below.
	MaxBytes int64
}

// defaultMaxArtifactBytes caps an upload when the store declares no ceiling. It
// is large enough for an adapter and small enough that a mistake does not fill
// a disk.
const defaultMaxArtifactBytes = 256 << 20

// validArtifactDir is what a directory name in a request may look like: no
// separator and no dot, so nothing that names a path can be smuggled through a
// field the store joins.
var validArtifactDir = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// validDigest is a hex sha256.
var validDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)

func (a *Agent) receive(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	store := a.Artifacts
	if store == nil {
		stdhttp.Error(w, "this node receives no artifacts", stdhttp.StatusNotFound)
		return
	}
	dir, name := r.URL.Query().Get("dir"), r.URL.Query().Get("name")
	want := r.URL.Query().Get("sha256")
	if !validArtifactDir.MatchString(dir) || store.Accept == nil || !store.Accept(name) {
		stdhttp.Error(w, "dir and name have to be simple names this node admits", stdhttp.StatusBadRequest)
		return
	}
	if !validDigest.MatchString(want) {
		stdhttp.Error(w, "the sha256 is absent or malformed; an artifact with no declared digest is not verifiable", stdhttp.StatusBadRequest)
		return
	}
	root, err := filepath.Abs(store.Root)
	if err != nil {
		stdhttp.Error(w, err.Error(), stdhttp.StatusInternalServerError)
		return
	}
	target := filepath.Join(root, dir, name)
	if !strings.HasPrefix(target, root+string(filepath.Separator)) {
		stdhttp.Error(w, "the path leaves the artifact root", stdhttp.StatusBadRequest)
		return
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		stdhttp.Error(w, err.Error(), stdhttp.StatusInternalServerError)
		return
	}
	written, got, err := store.write(w, r, target)
	if err != nil {
		stdhttp.Error(w, err.Error(), stdhttp.StatusBadRequest)
		return
	}
	// The digest is verified after the write and a mismatch removes the file. A
	// partial artifact that looks complete is worse than an absent one: the next
	// run reads it and produces a number nobody can trust.
	if got != want {
		os.Remove(target)
		stdhttp.Error(w, fmt.Sprintf("digest mismatch: declared %s, received %s", want, got), stdhttp.StatusUnprocessableEntity)
		return
	}
	answerJSON(w, stdhttp.StatusCreated, map[string]any{"dir": dir, "name": name, "bytes": written, "sha256": got})
}

// write streams the body to a temporary file beside the target and renames it
// into place, so a reader never observes a half-written artifact.
func (s *ArtifactStore) write(w stdhttp.ResponseWriter, r *stdhttp.Request, target string) (int64, string, error) {
	limit := s.MaxBytes
	if limit <= 0 {
		limit = defaultMaxArtifactBytes
	}
	temporary, err := os.CreateTemp(filepath.Dir(target), ".upload-")
	if err != nil {
		return 0, "", err
	}
	sum := sha256.New()
	written, err := io.Copy(io.MultiWriter(temporary, sum), stdhttp.MaxBytesReader(w, r.Body, limit))
	temporary.Close()
	if err == nil {
		err = os.Rename(temporary.Name(), target)
	}
	if err != nil {
		os.Remove(temporary.Name())
		return 0, "", err
	}
	return written, hex.EncodeToString(sum.Sum(nil)), nil
}
