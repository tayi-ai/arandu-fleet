package cluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/arandu-io/framework/security"
)

// The actions of the control plane.
const (
	// ClusterView is reading the inventory and the status of the nodes.
	ClusterView security.Action = "cluster.view"
	// ClusterDispatch is starting or cancelling a run on the fleet.
	ClusterDispatch security.Action = "cluster.dispatch"
)

// ControlActions are the actions a group may carry, sorted.
func ControlActions() []security.Action {
	return []security.Action{ClusterDispatch, ClusterView}
}

// Run is one dispatch to the fleet: a run id, an action, the nodes it went to
// and what each answered.
//
// It is the resource the policy decides about. A submission is not a
// completion: the run closes when every node reports the same id as finished,
// which is what Status reads.
type Run struct {
	ID      string
	Action  string
	Role    string
	Started time.Time
	Nodes   []string
	Answers map[string]string
	Errors  map[string]string
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
	case ClusterView, ClusterDispatch:
		if s.Can(a) {
			return nil
		}
		return fmt.Errorf("cluster: %s is not among the subject's effective actions", a)
	}
	return fmt.Errorf("cluster: no rule allows %s on a run", a)
}

// ErrBusy is returned when a distributed run is already in flight.
//
// The cluster is a queue of one: two simultaneous launches contend for the
// same cards, shuffle run ids and produce measurements that count for nothing.
var ErrBusy = errors.New("cluster: a run is already in flight; the cluster is a queue of one")

// ControlPlane coordinates the fleet from the inventory.
type ControlPlane struct {
	nodes  []Node
	worker Worker
	policy ControlPolicy
	now    func() time.Time

	mu     sync.Mutex
	active *Run
}

// NewControlPlane refuses an inventory the fleet must not be addressed with,
// and a nil worker.
func NewControlPlane(nodes []Node, worker Worker) (*ControlPlane, error) {
	if err := ValidateInventory(nodes); err != nil {
		return nil, err
	}
	if worker == nil {
		return nil, errors.New("cluster: NewControlPlane needs a worker client")
	}
	return &ControlPlane{nodes: ByNumeral(nodes), worker: worker, now: time.Now}, nil
}

// Nodes returns the inventory in numeral order.
func (c *ControlPlane) Nodes() []Node { return append([]Node(nil), c.nodes...) }

// Plan returns the training group the inventory yields.
func (c *ControlPlane) Plan() (TrainingPlan, error) { return Plan(c.nodes) }

// Status reads every node of the role, in parallel, and returns what each
// answered. Reading never occupies a card and never needs the queue.
func (c *ControlPlane) Status(ctx context.Context, actor security.Subject, role string) (Run, error) {
	if _, err := security.Authorize(ctx, c.policy, actor, ClusterView, Run{}); err != nil {
		return Run{}, err
	}
	selected, err := c.select(role)
	if err != nil {
		return Run{}, err
	}
	run := Run{Action: ActionStatus, Role: role, Started: c.now(), Answers: map[string]string{}, Errors: map[string]string{}}
	c.fanOut(ctx, selected, &run, func(n Node) ([]byte, error) { return c.worker.Status(ctx, n) })
	return run, nil
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
		return Run{}, fmt.Errorf("cluster: %q is not an action the fleet runs; the actions are %s and %s", action, ActionDiagnostics, ActionCollective)
	}
	if action == ActionCollective {
		role = RoleTrain
	}
	if _, err := security.Authorize(ctx, c.policy, actor, ClusterDispatch, Run{Action: action, Role: role}); err != nil {
		return Run{}, err
	}
	selected, err := c.select(role)
	if err != nil {
		return Run{}, err
	}
	run := Run{ID: fmt.Sprintf("%s-%d", action, c.now().UnixNano()), Action: action, Role: role, Started: c.now(), Answers: map[string]string{}, Errors: map[string]string{}}

	c.mu.Lock()
	if c.active != nil {
		c.mu.Unlock()
		return Run{}, fmt.Errorf("%w: %s", ErrBusy, c.active.ID)
	}
	c.active = &run
	c.mu.Unlock()

	job := Job{ID: run.ID, Action: action}
	c.fanOut(ctx, selected, &run, func(n Node) ([]byte, error) { return c.worker.Submit(ctx, n, job) })
	if len(run.Errors) > 0 {
		cancel := Run{ID: run.ID, Action: "cancel", Answers: map[string]string{}, Errors: map[string]string{}}
		c.fanOut(ctx, selected, &cancel, func(n Node) ([]byte, error) { return c.worker.Cancel(ctx, n, job) })
		c.Release(run.ID)
		return run, fmt.Errorf("cluster: partial launch of %s; cancelled on every selected node: %s", run.ID, joinErrors(run.Errors))
	}
	return run, nil
}

// Release closes the queue for the run with that id. It refuses another id, so
// a stale caller cannot free a run it does not own.
func (c *ControlPlane) Release(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == nil {
		return errors.New("cluster: no run is in flight")
	}
	if c.active.ID != id {
		return fmt.Errorf("cluster: %s is in flight, not %s", c.active.ID, id)
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
	return *c.active, true
}

func (c *ControlPlane) select(role string) ([]Node, error) {
	if role != "all" && !roles[role] {
		return nil, fmt.Errorf("cluster: role must be all, train, rollout or eval, not %q", role)
	}
	selected := WithRole(c.nodes, role)
	if len(selected) == 0 {
		return nil, fmt.Errorf("cluster: no node declares role %q", role)
	}
	return selected, nil
}

func (c *ControlPlane) fanOut(ctx context.Context, nodes []Node, run *Run, call func(Node) ([]byte, error)) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, n := range nodes {
		run.Nodes = append(run.Nodes, n.ID)
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
