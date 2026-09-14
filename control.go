package fleet

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/arandu-io/framework/security"
)

// The actions of the control plane.
const (
	// FleetView is reading the inventory and the status of the nodes.
	FleetView security.Action = "fleet.view"
	// FleetDispatch is starting or cancelling a run on the fleet.
	FleetDispatch security.Action = "fleet.dispatch"
)

// ControlActions are the actions a group may carry, sorted.
func ControlActions() []security.Action {
	return []security.Action{FleetDispatch, FleetView}
}

// Run is one dispatch to the fleet: a run id, an action, the nodes it went to
// and what each answered.
//
// It is the resource the policy decides about. A submission is not a
// completion: the run closes when every node reports the same id as finished,
// which is what Status reads.
type Run struct {
	ID         string
	Action     string
	Role       string
	Generation uint64
	Started    time.Time
	Nodes      []string
	Answers    map[string]string
	Errors     map[string]string
}

// ControlPolicy decides who may look at the fleet and who may occupy it.
//
// It decides from what the subject carries: the effective actions filled in
// before any policy runs. A subject that carries nothing is refused everything.
type ControlPolicy struct{}

var _ security.Policy[Run] = ControlPolicy{}

// Can decides whether the subject may perform the action on the run.
func (ControlPolicy) Can(_ context.Context, s security.Subject, a security.Action, _ Run) error {
	switch a {
	case FleetView, FleetDispatch:
		if s.Can(a) {
			return nil
		}
		return fmt.Errorf("fleet: %s is not among the subject's effective actions", a)
	}
	return fmt.Errorf("fleet: no rule allows %s on a run", a)
}

// ErrBusy is returned when a distributed run is already in flight.
//
// The cluster is a queue of one: two simultaneous launches contend for the
// same cards, shuffle run ids and produce measurements that count for nothing.
var ErrBusy = errors.New("fleet: a run is already in flight; the cluster is a queue of one")

// ControlPlane coordinates the fleet from the inventory.
type ControlPlane struct {
	convention Convention
	nodes      []Node
	worker     Worker
	policy     ControlPolicy
	now        func() time.Time

	mu     sync.Mutex
	active *Run
}

// NewControlPlane refuses an inventory the fleet must not be addressed with,
// and a nil worker.
func NewControlPlane(convention Convention, nodes []Node, worker Worker) (*ControlPlane, error) {
	convention, err := convention.Compile()
	if err != nil {
		return nil, err
	}
	if err := ValidateInventory(convention, nodes); err != nil {
		return nil, err
	}
	if worker == nil {
		return nil, errors.New("fleet: NewControlPlane needs a worker client")
	}
	return &ControlPlane{convention: convention, nodes: InDeclaredOrder(convention, nodes), worker: worker, now: time.Now}, nil
}

// Convention is what this control plane validates its inventory against.
func (c *ControlPlane) Convention() Convention { return c.convention }

// Nodes returns the inventory in the order the convention declares.
func (c *ControlPlane) Nodes() []Node { return append([]Node(nil), c.nodes...) }

// Plan returns the training group the inventory yields.
func (c *ControlPlane) Plan() (TrainingPlan, error) { return Plan(c.convention, c.nodes) }

// Status reads every node of the role, in parallel, and returns what each
// answered. Reading never occupies a card and never needs the queue.
func (c *ControlPlane) Status(ctx context.Context, actor security.Subject, role string) (Run, error) {
	if _, err := security.Authorize(ctx, c.policy, actor, FleetView, Run{}); err != nil {
		return Run{}, err
	}
	selected, err := c.selectNodes(role)
	if err != nil {
		return Run{}, err
	}
	run := newRun("", ActionStatus, role, c.now(), selected)
	c.fanOut(ctx, selected, &run, func(n Node) ([]byte, error) { return c.worker.Status(ctx, n) })
	return run, nil
}

// StatusNodes reads the exact declared nodes, preserving convention order.
func (c *ControlPlane) StatusNodes(ctx context.Context, actor security.Subject, nodeIDs []string) (Run, error) {
	if _, err := security.Authorize(ctx, c.policy, actor, FleetView, Run{}); err != nil {
		return Run{}, err
	}
	selected, err := c.selectNodeIDs(nodeIDs)
	if err != nil {
		return Run{}, err
	}
	run := newRun("", ActionStatus, "selected", c.now(), selected)
	c.fanOut(ctx, selected, &run, func(n Node) ([]byte, error) { return c.worker.Status(ctx, n) })
	return run, nil
}

// ResultNode reads the persisted result of one exact fenced job.
func (c *ControlPlane) ResultNode(ctx context.Context, actor security.Subject, job Job, nodeID string) ([]byte, error) {
	if _, err := security.Authorize(ctx, c.policy, actor, FleetView, Run{ID: job.ID, Action: job.Action}); err != nil {
		return nil, err
	}
	selected, err := c.selectNodeIDs([]string{nodeID})
	if err != nil {
		return nil, err
	}
	reader, ok := c.worker.(ResultWorker)
	if !ok {
		return nil, errors.New("fleet: the configured worker transport does not expose persisted results")
	}
	return reader.Result(ctx, selected[0], job)
}

// PutArtifactNode delivers one verified artifact to an exact declared node.
// It does not reserve the fleet because delivery does not start a process or
// occupy a GPU; the job that consumes the artifact is fenced separately.
func (c *ControlPlane) PutArtifactNode(ctx context.Context, actor security.Subject, nodeID string, artifact Artifact) ([]byte, error) {
	if _, err := security.Authorize(ctx, c.policy, actor, FleetDispatch, Run{Action: "artifact.put"}); err != nil {
		return nil, err
	}
	selected, err := c.selectNodeIDs([]string{nodeID})
	if err != nil {
		return nil, err
	}
	uploader, ok := c.worker.(ArtifactWorker)
	if !ok {
		return nil, errors.New("fleet: the configured worker transport does not deliver artifacts")
	}
	return uploader.PutArtifact(ctx, selected[0], artifact)
}

// Dispatch starts one action on every node of the role.
//
// Every submission is sent in parallel. If any node refuses, the run id is
// cancelled on every selected node, so a partial launch never leaves cards
// occupied under an id nobody will close. One run at a time: a second Dispatch
// while one is in flight is refused with ErrBusy, and Release is what the
// caller invokes once Status shows every node finished.
func (c *ControlPlane) Dispatch(ctx context.Context, actor security.Subject, action, role string) (Run, error) {
	if action != ActionDiagnostics && action != ActionCollective {
		return Run{}, fmt.Errorf("fleet: %q is not an action the fleet runs; the actions are %s and %s", action, ActionDiagnostics, ActionCollective)
	}
	if action == ActionCollective {
		role = c.convention.TrainingRole
	}
	job := Job{ID: fmt.Sprintf("%s-%d", action, c.now().UnixNano()), Action: action}
	return c.DispatchJob(ctx, actor, job, role)
}

// DispatchJob starts an application-defined, typed job on the selected nodes.
//
// The module validates identity, authorization and selection. The installed
// Program remains the vocabulary boundary and refuses actions or request
// shapes the application did not implement.
func (c *ControlPlane) DispatchJob(ctx context.Context, actor security.Subject, job Job, role string) (Run, error) {
	if !validRunID.MatchString(job.ID) || strings.TrimSpace(job.Action) == "" {
		return Run{}, errors.New("fleet: a dispatched job needs a usable id and action")
	}
	if _, err := security.Authorize(ctx, c.policy, actor, FleetDispatch, Run{ID: job.ID, Action: job.Action, Role: role}); err != nil {
		return Run{}, err
	}
	selected, err := c.selectNodes(role)
	if err != nil {
		return Run{}, err
	}
	return c.dispatchSelected(ctx, job, role, selected)
}

// DispatchNodes starts a typed job on an exact subset of declared nodes.
//
// A second phase may extend the same fenced run onto disjoint nodes. This is
// how remote RPC servers and their coordinator belong to one execution without
// opening the fleet to another run in between.
func (c *ControlPlane) DispatchNodes(ctx context.Context, actor security.Subject, job Job, nodeIDs []string) (Run, error) {
	if !validRunID.MatchString(job.ID) || strings.TrimSpace(job.Action) == "" {
		return Run{}, errors.New("fleet: a dispatched job needs a usable id and action")
	}
	if _, err := security.Authorize(ctx, c.policy, actor, FleetDispatch, Run{ID: job.ID, Action: job.Action, Role: "selected"}); err != nil {
		return Run{}, err
	}
	selected, err := c.selectNodeIDs(nodeIDs)
	if err != nil {
		return Run{}, err
	}
	return c.dispatchSelected(ctx, job, "selected", selected)
}

func (c *ControlPlane) dispatchSelected(ctx context.Context, job Job, role string, selected []Node) (Run, error) {
	run := newRun(job.ID, job.Action, role, c.now(), selected)
	run.Generation = job.Generation
	if err := c.reserveNodes(run, selected); err != nil {
		return Run{}, err
	}
	c.fanOut(ctx, selected, &run, func(n Node) ([]byte, error) { return c.worker.Submit(ctx, n, job) })
	if len(run.Errors) > 0 {
		cancel := newRun(run.ID, "cancel", role, c.now(), selected)
		cancel.Generation = job.Generation
		c.fanOut(ctx, selected, &cancel, func(n Node) ([]byte, error) { return c.worker.Cancel(ctx, n, job) })
		failedSubmission := make([]string, 0, len(run.Errors))
		for id := range run.Errors {
			failedSubmission = append(failedSubmission, id)
		}
		c.releaseNodes(run.ID, run.Generation, failedSubmission)
		return run, fmt.Errorf("fleet: partial launch of %s; cancelled on every selected node: %s", run.ID, joinErrors(run.Errors))
	}
	return run, nil
}

// CancelJob sends a fenced cancellation for one job to every selected node.
func (c *ControlPlane) CancelJob(ctx context.Context, actor security.Subject, job Job, role string) (Run, error) {
	if !validRunID.MatchString(job.ID) {
		return Run{}, errors.New("fleet: cancellation needs a usable run id")
	}
	if _, err := security.Authorize(ctx, c.policy, actor, FleetDispatch, Run{ID: job.ID, Action: "cancel", Role: role}); err != nil {
		return Run{}, err
	}
	selected, err := c.selectNodes(role)
	if err != nil {
		return Run{}, err
	}
	run := newRun(job.ID, "cancel", role, c.now(), selected)
	run.Generation = job.Generation
	c.fanOut(ctx, selected, &run, func(n Node) ([]byte, error) { return c.worker.Cancel(ctx, n, job) })
	return run, nil
}

// CancelNodes sends a fenced cancellation to an exact subset of nodes.
func (c *ControlPlane) CancelNodes(ctx context.Context, actor security.Subject, job Job, nodeIDs []string) (Run, error) {
	if !validRunID.MatchString(job.ID) {
		return Run{}, errors.New("fleet: cancellation needs a usable run id")
	}
	if _, err := security.Authorize(ctx, c.policy, actor, FleetDispatch, Run{ID: job.ID, Action: "cancel", Role: "selected"}); err != nil {
		return Run{}, err
	}
	selected, err := c.selectNodeIDs(nodeIDs)
	if err != nil {
		return Run{}, err
	}
	run := newRun(job.ID, "cancel", "selected", c.now(), selected)
	run.Generation = job.Generation
	c.fanOut(ctx, selected, &run, func(n Node) ([]byte, error) { return c.worker.Cancel(ctx, n, job) })
	return run, nil
}

// ReleaseNodes frees nodes only after the caller has observed their fenced
// process as terminal or absent. Accepting SIGTERM is not that observation.
func (c *ControlPlane) ReleaseNodes(job Job, nodeIDs []string) error {
	selected, err := c.selectNodeIDs(nodeIDs)
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(selected))
	for _, node := range selected {
		ids = append(ids, node.ID)
	}
	return c.releaseNodes(job.ID, job.Generation, ids)
}

// Release closes the queue for the run with that id. It refuses another id, so
// a stale caller cannot free a run it does not own.
func (c *ControlPlane) Release(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == nil {
		return errors.New("fleet: no run is in flight")
	}
	if c.active.ID != id {
		return fmt.Errorf("fleet: %s is in flight, not %s", c.active.ID, id)
	}
	c.active = nil
	return nil
}

// InFlight returns the run occupying the queue, if any.
func (c *ControlPlane) InFlight() (Run, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == nil {
		return Run{}, false
	}
	copy := *c.active
	copy.Nodes = append([]string(nil), c.active.Nodes...)
	return copy, true
}

func (c *ControlPlane) reserveNodes(run Run, nodes []Node) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == nil {
		reserved := run
		reserved.Answers = nil
		reserved.Errors = nil
		c.active = &reserved
		return nil
	}
	if c.active.ID != run.ID {
		return fmt.Errorf("%w: %s", ErrBusy, c.active.ID)
	}
	if c.active.Generation != run.Generation {
		return fmt.Errorf("%w: %s generation %d", ErrBusy, c.active.ID, c.active.Generation)
	}
	held := make(map[string]bool, len(c.active.Nodes))
	for _, id := range c.active.Nodes {
		held[id] = true
	}
	for _, node := range nodes {
		if held[node.ID] {
			return fmt.Errorf("%w: node %s is already held by %s", ErrBusy, node.ID, run.ID)
		}
	}
	for _, node := range nodes {
		c.active.Nodes = append(c.active.Nodes, node.ID)
	}
	return nil
}

func (c *ControlPlane) releaseNodes(runID string, generation uint64, nodeIDs []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == nil {
		return nil
	}
	if c.active.ID != runID || c.active.Generation != generation {
		return fmt.Errorf("fleet: %s generation %d is in flight, not %s generation %d", c.active.ID, c.active.Generation, runID, generation)
	}
	release := make(map[string]bool, len(nodeIDs))
	for _, id := range nodeIDs {
		release[id] = true
	}
	kept := c.active.Nodes[:0]
	for _, id := range c.active.Nodes {
		if !release[id] {
			kept = append(kept, id)
		}
	}
	c.active.Nodes = kept
	if len(c.active.Nodes) == 0 {
		c.active = nil
	}
	return nil
}

func (c *ControlPlane) selectNodes(role string) ([]Node, error) {
	if role != "all" && !c.convention.IsRole(role) {
		return nil, fmt.Errorf("fleet: role must be all or one the convention declares (%s), not %q", strings.Join(c.convention.Roles, ", "), role)
	}
	selected := WithRole(c.nodes, role)
	if len(selected) == 0 {
		return nil, fmt.Errorf("fleet: no node declares role %q", role)
	}
	return selected, nil
}

func (c *ControlPlane) selectNodeIDs(ids []string) ([]Node, error) {
	if len(ids) == 0 {
		return nil, errors.New("fleet: at least one node id is required")
	}
	wanted := make(map[string]bool, len(ids))
	for _, id := range ids {
		if wanted[id] {
			return nil, fmt.Errorf("fleet: node %q is selected more than once", id)
		}
		wanted[id] = true
	}
	selected := make([]Node, 0, len(ids))
	for _, node := range c.nodes {
		if wanted[node.ID] {
			selected = append(selected, node)
			delete(wanted, node.ID)
		}
	}
	if len(wanted) > 0 {
		unknown := make([]string, 0, len(wanted))
		for id := range wanted {
			unknown = append(unknown, id)
		}
		sort.Strings(unknown)
		return nil, fmt.Errorf("fleet: unknown node ids: %s", strings.Join(unknown, ", "))
	}
	return selected, nil
}

func newRun(id, action, role string, started time.Time, nodes []Node) Run {
	run := Run{ID: id, Action: action, Role: role, Started: started, Answers: map[string]string{}, Errors: map[string]string{}}
	for _, node := range nodes {
		run.Nodes = append(run.Nodes, node.ID)
	}
	return run
}

func (c *ControlPlane) fanOut(ctx context.Context, nodes []Node, run *Run, call func(Node) ([]byte, error)) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, n := range nodes {
		wg.Add(1)
		go func(n Node) {
			defer wg.Done()
			answer, err := call(n)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				run.Errors[n.ID] = err.Error()
				return
			}
			run.Answers[n.ID] = string(answer)
		}(n)
	}
	wg.Wait()
}

func joinErrors(errs map[string]string) string {
	ids := make([]string, 0, len(errs))
	for id := range errs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += "; "
		}
		out += id + ": " + errs[id]
	}
	return out
}
