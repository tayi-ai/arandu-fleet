package unit_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/arandu-io/framework/security"
	cluster "github.com/tayi-ai/arandu-cluster"
)

func inventory() []cluster.Node {
	return []cluster.Node{
		{ID: "five", IP: "100.64.0.5", Interface: "tailscale0", Role: cluster.RoleTrain, AvailableGPUIDs: []int{0, 1}},
		{ID: "one", IP: "100.64.0.1", Interface: "tailscale0", Role: cluster.RoleTrain, AvailableGPUIDs: []int{0, 1}},
		{ID: "three", IP: "10.0.0.3", Interface: "tailscale0", Role: cluster.RoleTrain, AvailableGPUIDs: []int{1}},
		{ID: "seven", IP: "100.64.0.7", Interface: "tailscale0", Role: cluster.RoleRollout, AvailableGPUIDs: []int{0, 1}},
		{ID: "twentyone", IP: "100.64.0.21", Interface: "tailscale0", Role: cluster.RoleEval, AvailableGPUIDs: []int{0}},
	}
}

func TestInventoryValidationRefusesWhatMustNotBeAddressed(t *testing.T) {
	if err := cluster.ValidateInventory(inventory()); err != nil {
		t.Fatalf("valid inventory refused: %v", err)
	}
	mutate := func(f func(n *cluster.Node)) []cluster.Node {
		nodes := inventory()
		f(&nodes[0])
		return nodes
	}
	cases := map[string][]cluster.Node{
		"integer id":        mutate(func(n *cluster.Node) { n.ID = "5" }),
		"public ip":         mutate(func(n *cluster.Node) { n.IP = "8.8.8.8" }),
		"ipv6":              mutate(func(n *cluster.Node) { n.IP = "fd00::1" }),
		"undeclared role":   mutate(func(n *cluster.Node) { n.Role = "" }),
		"unknown role":      mutate(func(n *cluster.Node) { n.Role = "serve" }),
		"unmeasured gpus":   mutate(func(n *cluster.Node) { n.AvailableGPUIDs = nil }),
		"negative gpu":      mutate(func(n *cluster.Node) { n.AvailableGPUIDs = []int{-1} }),
		"repeated gpu":      mutate(func(n *cluster.Node) { n.AvailableGPUIDs = []int{0, 0} }),
		"malformed iface":   mutate(func(n *cluster.Node) { n.Interface = "tail scale0" }),
		"duplicate id":      append(inventory(), cluster.Node{ID: "five", IP: "100.64.0.99", Interface: "tailscale0", Role: "eval", AvailableGPUIDs: []int{0}}),
		"duplicate address": append(inventory(), cluster.Node{ID: "nine", IP: "100.64.0.5", Interface: "tailscale0", Role: "eval", AvailableGPUIDs: []int{0}}),
		"empty":             nil,
	}
	for name, nodes := range cases {
		if err := cluster.ValidateInventory(nodes); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestFirstNodesAreEligibleByMeasurementNotByNumber(t *testing.T) {
	nodes := []cluster.Node{{ID: "one", IP: "10.0.0.1", Interface: "eth0", Role: cluster.RoleTrain, AvailableGPUIDs: []int{0, 1}}}
	if err := cluster.ValidateInventory(nodes); err != nil {
		t.Fatalf("node one refused by its number: %v", err)
	}
}

func TestPlanRanksTheTrainGroupByNumeralAndSumsMeasuredCards(t *testing.T) {
	plan, err := cluster.Plan(inventory())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Master.ID != "one" {
		t.Fatalf("master should be the first train node in numeral order, got %s", plan.Master.ID)
	}
	if len(plan.Ranks) != 3 || plan.Ranks[0].Node.ID != "one" || plan.Ranks[1].Node.ID != "three" || plan.Ranks[2].Node.ID != "five" {
		t.Fatalf("ranks out of order: %+v", plan.Ranks)
	}
	if plan.World != 5 || plan.ExpectedCollectiveSum() != 10 {
		t.Fatalf("world should follow measurement: world=%d sum=%d", plan.World, plan.ExpectedCollectiveSum())
	}
	if cluster.GPUList([]int{0, 1}) != "0,1" {
		t.Fatal("gpu list rendering changed")
	}
}

// fakeWorker records what the control plane asked of each node and answers as
// told. It is the node API with the network removed.
type fakeWorker struct {
	mu        sync.Mutex
	refuse    map[string]bool
	submitted map[string]cluster.Job
	cancelled map[string]cluster.Job
	statused  []string
}

func newFake() *fakeWorker {
	return &fakeWorker{refuse: map[string]bool{}, submitted: map[string]cluster.Job{}, cancelled: map[string]cluster.Job{}}
}

func (f *fakeWorker) Status(_ context.Context, n cluster.Node) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statused = append(f.statused, n.ID)
	return []byte(`{"state":"idle"}`), nil
}

func (f *fakeWorker) Submit(_ context.Context, n cluster.Node, job cluster.Job) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refuse[n.ID] {
		return nil, errors.New("node " + n.ID + " answered HTTP 409: busy")
	}
	f.submitted[n.ID] = job
	return []byte(`{"accepted":true}`), nil
}

func (f *fakeWorker) Cancel(_ context.Context, n cluster.Node, job cluster.Job) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled[n.ID] = job
	return []byte(`{"cancelled":true}`), nil
}

func operator() security.Subject {
	return security.Subject{ID: "paulo", Tenant: "tayi", Actions: []security.Action{cluster.ClusterView, cluster.ClusterDispatch}}
}

func TestStatusReadsEveryNodeOfTheRoleWithoutTakingTheQueue(t *testing.T) {
	fake := newFake()
	cp, err := cluster.NewControlPlane(inventory(), fake)
	if err != nil {
		t.Fatal(err)
	}
	run, err := cp.Status(context.Background(), operator(), "all")
	if err != nil {
		t.Fatal(err)
	}
	if len(run.Answers) != 5 || len(run.Errors) != 0 {
		t.Fatalf("status did not reach every node: %+v", run)
	}
	if _, busy := cp.InFlight(); busy {
		t.Fatal("a status read occupied the queue")
	}
	if _, err := cp.Status(context.Background(), operator(), "serve"); err == nil {
		t.Fatal("an unknown role was accepted")
	}
}

func TestDispatchIsRefusedWithoutTheAction(t *testing.T) {
	cp, err := cluster.NewControlPlane(inventory(), newFake())
	if err != nil {
		t.Fatal(err)
	}
	viewer := security.Subject{ID: "viewer", Tenant: "tayi", Actions: []security.Action{cluster.ClusterView}}
	if _, err := cp.Dispatch(context.Background(), viewer, cluster.ActionDiagnostics, "all"); err == nil {
		t.Fatal("a subject without cluster.dispatch occupied the fleet")
	}
	if _, err := cp.Status(context.Background(), security.Subject{ID: "nobody", Tenant: "tayi"}, "all"); err == nil {
		t.Fatal("a subject without cluster.view read the fleet")
	}
}

func TestCollectiveGoesOnlyToTrainNodesAndTheQueueIsOne(t *testing.T) {
	fake := newFake()
	cp, err := cluster.NewControlPlane(inventory(), fake)
	if err != nil {
		t.Fatal(err)
	}
	run, err := cp.Dispatch(context.Background(), operator(), cluster.ActionCollective, "all")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(run.ID, "collective-") || len(run.Nodes) != 3 {
		t.Fatalf("collective did not go to exactly the train nodes: %+v", run)
	}
	for _, id := range []string{"one", "three", "five"} {
		if fake.submitted[id].ID != run.ID {
			t.Errorf("node %s did not receive run %s", id, run.ID)
		}
	}
	if _, err := cp.Dispatch(context.Background(), operator(), cluster.ActionDiagnostics, "all"); !errors.Is(err, cluster.ErrBusy) {
		t.Fatalf("a second dispatch was not refused while one is in flight: %v", err)
	}
	if err := cp.Release("someone-else"); err == nil {
		t.Fatal("a stale id released the queue")
	}
	if err := cp.Release(run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := cp.Dispatch(context.Background(), operator(), cluster.ActionDiagnostics, "eval"); err != nil {
		t.Fatalf("dispatch after release refused: %v", err)
	}
}

func TestPartialLaunchIsCancelledEverywhereAndFreesTheQueue(t *testing.T) {
	fake := newFake()
	fake.refuse["three"] = true
	cp, err := cluster.NewControlPlane(inventory(), fake)
	if err != nil {
		t.Fatal(err)
	}
	run, err := cp.Dispatch(context.Background(), operator(), cluster.ActionDiagnostics, cluster.RoleTrain)
	if err == nil || !strings.Contains(err.Error(), "partial launch") {
		t.Fatalf("a partial launch was reported as success: %v", err)
	}
	for _, id := range []string{"one", "three", "five"} {
		if fake.cancelled[id].ID != run.ID {
			t.Errorf("node %s was not sent the cancel for %s", id, run.ID)
		}
	}
	if _, busy := cp.InFlight(); busy {
		t.Fatal("a failed launch left the queue occupied")
	}
}

func TestControlPlaneRefusesBadInventoryAndNilWorker(t *testing.T) {
	if _, err := cluster.NewControlPlane(nil, newFake()); err == nil {
		t.Fatal("empty inventory accepted")
	}
	if _, err := cluster.NewControlPlane(inventory(), nil); err == nil {
		t.Fatal("nil worker accepted")
	}
	if _, err := cluster.NewHTTPWorker(""); err == nil {
		t.Fatal("empty token accepted")
	}
}
