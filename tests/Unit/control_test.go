package unit_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/arandu-io/framework/security"
	fleet "github.com/tayi-ai/arandu-fleet"
)

// tayiDeclared is this installation's convention as somebody writes it:
// English cardinal numerals for ids, three roles, the Tailscale range beside
// RFC 1918, and the port the node API listens on. Every one of these is a
// choice, and another installation writes another one.
func tayiDeclared() fleet.Convention {
	return fleet.Convention{
		IDs:          []string{"one", "two", "three", "four", "five", "six", "seven", "twentyone"},
		Roles:        []string{"train", "rollout", "eval"},
		TrainingRole: "train",
		Networks:     []string{"100.64.0.0/10", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"},
		DefaultPort:  8787,
	}
}

// tayi is the same convention compiled, which is what the free functions take.
func tayi(t *testing.T) fleet.Convention {
	t.Helper()
	c, err := tayiDeclared().Compile()
	if err != nil {
		t.Fatalf("the convention was refused: %v", err)
	}
	return c
}

func inventory() []fleet.Node {
	return []fleet.Node{
		{ID: "five", IP: "100.64.0.5", Interface: "tailscale0", Role: "train", AvailableGPUIDs: []int{0, 1}},
		{ID: "one", IP: "100.64.0.1", Interface: "tailscale0", Role: "train", AvailableGPUIDs: []int{0, 1}},
		{ID: "three", IP: "10.0.0.3", Interface: "tailscale0", Role: "train", AvailableGPUIDs: []int{1}},
		{ID: "seven", IP: "100.64.0.7", Interface: "tailscale0", Role: "rollout", AvailableGPUIDs: []int{0, 1}},
		{ID: "twentyone", IP: "100.64.0.21", Interface: "tailscale0", Role: "eval", AvailableGPUIDs: []int{0}},
	}
}

func TestInventoryValidationRefusesWhatMustNotBeAddressed(t *testing.T) {
	if err := fleet.ValidateInventory(tayi(t), inventory()); err != nil {
		t.Fatalf("valid inventory refused: %v", err)
	}
	mutate := func(f func(n *fleet.Node)) []fleet.Node {
		nodes := inventory()
		f(&nodes[0])
		return nodes
	}
	cases := map[string][]fleet.Node{
		"integer id":        mutate(func(n *fleet.Node) { n.ID = "5" }),
		"public ip":         mutate(func(n *fleet.Node) { n.IP = "8.8.8.8" }),
		"ipv6":              mutate(func(n *fleet.Node) { n.IP = "fd00::1" }),
		"undeclared role":   mutate(func(n *fleet.Node) { n.Role = "" }),
		"unknown role":      mutate(func(n *fleet.Node) { n.Role = "serve" }),
		"unmeasured gpus":   mutate(func(n *fleet.Node) { n.AvailableGPUIDs = nil }),
		"negative gpu":      mutate(func(n *fleet.Node) { n.AvailableGPUIDs = []int{-1} }),
		"repeated gpu":      mutate(func(n *fleet.Node) { n.AvailableGPUIDs = []int{0, 0} }),
		"malformed iface":   mutate(func(n *fleet.Node) { n.Interface = "tail scale0" }),
		"duplicate id":      append(inventory(), fleet.Node{ID: "five", IP: "100.64.0.99", Interface: "tailscale0", Role: "eval", AvailableGPUIDs: []int{0}}),
		"duplicate address": append(inventory(), fleet.Node{ID: "nine", IP: "100.64.0.5", Interface: "tailscale0", Role: "eval", AvailableGPUIDs: []int{0}}),
		"empty":             nil,
	}
	for name, nodes := range cases {
		if err := fleet.ValidateInventory(tayi(t), nodes); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestANodeIsEligibleByMeasurementNotByItsPositionInTheConvention(t *testing.T) {
	nodes := []fleet.Node{{ID: "one", IP: "10.0.0.1", Interface: "eth0", Role: "train", AvailableGPUIDs: []int{0, 1}}}
	if err := fleet.ValidateInventory(tayi(t), nodes); err != nil {
		t.Fatalf("the first declared node was refused for being first: %v", err)
	}
}

func TestPlanRanksTheTrainGroupInDeclaredOrderAndSumsMeasuredCards(t *testing.T) {
	plan, err := fleet.Plan(tayi(t), inventory())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Master.ID != "one" {
		t.Fatalf("master should be the first train node in the convention's order, got %s", plan.Master.ID)
	}
	if len(plan.Ranks) != 3 || plan.Ranks[0].Node.ID != "one" || plan.Ranks[1].Node.ID != "three" || plan.Ranks[2].Node.ID != "five" {
		t.Fatalf("ranks out of order: %+v", plan.Ranks)
	}
	if plan.World != 5 || plan.ExpectedCollectiveSum() != 10 {
		t.Fatalf("world should follow measurement: world=%d sum=%d", plan.World, plan.ExpectedCollectiveSum())
	}
	if fleet.GPUList([]int{0, 1}) != "0,1" {
		t.Fatal("gpu list rendering changed")
	}
}

// fakeWorker records what the control plane asked of each node and answers as
// told. It is the node API with the network removed.
type fakeWorker struct {
	mu        sync.Mutex
	refuse    map[string]bool
	submitted map[string]fleet.Job
	cancelled map[string]fleet.Job
	statused  []string
}

func newFake() *fakeWorker {
	return &fakeWorker{refuse: map[string]bool{}, submitted: map[string]fleet.Job{}, cancelled: map[string]fleet.Job{}}
}

func (f *fakeWorker) Status(_ context.Context, n fleet.Node) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statused = append(f.statused, n.ID)
	return []byte(`{"state":"idle"}`), nil
}

func (f *fakeWorker) Submit(_ context.Context, n fleet.Node, job fleet.Job) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refuse[n.ID] {
		return nil, errors.New("node " + n.ID + " answered HTTP 409: busy")
	}
	f.submitted[n.ID] = job
	return []byte(`{"accepted":true}`), nil
}

func (f *fakeWorker) Cancel(_ context.Context, n fleet.Node, job fleet.Job) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled[n.ID] = job
	return []byte(`{"cancelled":true}`), nil
}

func operator() security.Subject {
	return security.Subject{ID: "paulo", Tenant: "tayi", Actions: []security.Action{fleet.FleetView, fleet.FleetDispatch}}
}

func TestStatusReadsEveryNodeOfTheRoleWithoutTakingTheQueue(t *testing.T) {
	fake := newFake()
	cp, err := fleet.NewControlPlane(tayiDeclared(), inventory(), fake)
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
	cp, err := fleet.NewControlPlane(tayiDeclared(), inventory(), newFake())
	if err != nil {
		t.Fatal(err)
	}
	viewer := security.Subject{ID: "viewer", Tenant: "tayi", Actions: []security.Action{fleet.FleetView}}
	if _, err := cp.Dispatch(context.Background(), viewer, fleet.ActionDiagnostics, "all"); err == nil {
		t.Fatal("a subject without fleet.dispatch occupied the fleet")
	}
	if _, err := cp.Status(context.Background(), security.Subject{ID: "nobody", Tenant: "tayi"}, "all"); err == nil {
		t.Fatal("a subject without fleet.view read the fleet")
	}
}

func TestCollectiveGoesOnlyToTrainNodesAndTheQueueIsOne(t *testing.T) {
	fake := newFake()
	cp, err := fleet.NewControlPlane(tayiDeclared(), inventory(), fake)
	if err != nil {
		t.Fatal(err)
	}
	run, err := cp.Dispatch(context.Background(), operator(), fleet.ActionCollective, "all")
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
	if _, err := cp.Dispatch(context.Background(), operator(), fleet.ActionDiagnostics, "all"); !errors.Is(err, fleet.ErrBusy) {
		t.Fatalf("a second dispatch was not refused while one is in flight: %v", err)
	}
	if err := cp.Release("someone-else"); err == nil {
		t.Fatal("a stale id released the queue")
	}
	if err := cp.Release(run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := cp.Dispatch(context.Background(), operator(), fleet.ActionDiagnostics, "eval"); err != nil {
		t.Fatalf("dispatch after release refused: %v", err)
	}
}

func TestPartialLaunchIsCancelledEverywhereAndFreesTheQueue(t *testing.T) {
	fake := newFake()
	fake.refuse["three"] = true
	cp, err := fleet.NewControlPlane(tayiDeclared(), inventory(), fake)
	if err != nil {
		t.Fatal(err)
	}
	run, err := cp.Dispatch(context.Background(), operator(), fleet.ActionDiagnostics, "train")
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
	if _, err := fleet.NewControlPlane(tayiDeclared(), nil, newFake()); err == nil {
		t.Fatal("empty inventory accepted")
	}
	if _, err := fleet.NewControlPlane(tayiDeclared(), inventory(), nil); err == nil {
		t.Fatal("nil worker accepted")
	}
	if _, err := fleet.NewHTTPWorker("", tayi(t).PortOf); err == nil {
		t.Fatal("empty token accepted")
	}
	if _, err := fleet.NewHTTPWorker("a-token", nil); err == nil {
		t.Fatal("a worker with no way to resolve a port was accepted")
	}
}

func TestTheConventionIsWhatRefuses(t *testing.T) {
	// Nothing in this package knows what a node is called, what a role means or
	// which network is private. An installation that says so is served; one
	// that leaves it open is refused rather than guessed for.
	declared := tayiDeclared()
	if _, err := declared.Compile(); err != nil {
		t.Fatalf("a complete convention was refused: %v", err)
	}
	without := func(f func(c *fleet.Convention)) fleet.Convention {
		c := tayiDeclared()
		f(&c)
		return c
	}
	cases := map[string]fleet.Convention{
		"no id":                without(func(c *fleet.Convention) { c.IDs = nil }),
		"empty id":             without(func(c *fleet.Convention) { c.IDs = []string{"one", " "} }),
		"repeated id":          without(func(c *fleet.Convention) { c.IDs = []string{"one", "one"} }),
		"no role":              without(func(c *fleet.Convention) { c.Roles = nil }),
		"training role absent": without(func(c *fleet.Convention) { c.TrainingRole = "gradient" }),
		"no network":           without(func(c *fleet.Convention) { c.Networks = nil }),
		"network is not cidr":  without(func(c *fleet.Convention) { c.Networks = []string{"100.64.0.1"} }),
		"port zero":            without(func(c *fleet.Convention) { c.DefaultPort = 0 }),
		"port too high":        without(func(c *fleet.Convention) { c.DefaultPort = 70000 }),
		"pattern not regexp":   without(func(c *fleet.Convention) { c.InterfacePattern = "([" }),
	}
	for name, c := range cases {
		if _, err := c.Compile(); err == nil {
			t.Errorf("%s: the convention was accepted", name)
		}
	}
	if err := fleet.ValidateInventory(fleet.Convention{}, inventory()); err == nil {
		t.Error("an uncompiled convention validated an inventory")
	}
}

func TestAnotherInstallationDeclaresAnotherFleet(t *testing.T) {
	// The point of the convention: none of this installation's words appear
	// here, and the module serves it exactly the same.
	other := fleet.Convention{
		IDs:          []string{"gpu-a", "gpu-b", "gpu-c"},
		Roles:        []string{"gradient", "sampler"},
		TrainingRole: "gradient",
		Networks:     []string{"192.168.10.0/24"},
		DefaultPort:  9000,
	}
	nodes := []fleet.Node{
		{ID: "gpu-c", IP: "192.168.10.3", Interface: "eno1", Role: "sampler", AvailableGPUIDs: []int{0}},
		{ID: "gpu-a", IP: "192.168.10.1", Interface: "eno1", Role: "gradient", AvailableGPUIDs: []int{0, 1, 2, 3}},
		{ID: "gpu-b", IP: "192.168.10.2", Interface: "eno1", Role: "gradient", AvailableGPUIDs: []int{0, 1, 2, 3}, Port: 9100},
	}
	compiled, err := other.Compile()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := fleet.Plan(compiled, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Master.ID != "gpu-a" || len(plan.Ranks) != 2 || plan.World != 8 || plan.ExpectedCollectiveSum() != 28 {
		t.Fatalf("another installation's plan is wrong: master=%s ranks=%d world=%d", plan.Master.ID, len(plan.Ranks), plan.World)
	}
	if got := compiled.PortOf(nodes[2]); got != 9100 {
		t.Errorf("a node that names its own port was given %d", got)
	}
	if got := compiled.PortOf(nodes[0]); got != 9000 {
		t.Errorf("a node that names no port was not given the default: %d", got)
	}
	// This installation's own inventory is refused here, which is the whole
	// point: the rules travel with the declaration, not with the package.
	if err := fleet.ValidateInventory(compiled, inventory()); err == nil {
		t.Error("one installation's inventory validated against another's convention")
	}
}

func TestADeclarationIsOneDocument(t *testing.T) {
	document := []byte(`{
	  "convention": {
	    "ids": ["alpha", "beta"],
	    "roles": ["train", "serve"],
	    "training_role": "train",
	    "networks": ["10.9.0.0/16"],
	    "default_port": 7000
	  },
	  "nodes": [
	    {"id": "beta", "private_ip": "10.9.0.2", "interface": "eth0", "role": "serve", "available_gpu_ids": [0]},
	    {"id": "alpha", "private_ip": "10.9.0.1", "interface": "eth0", "role": "train", "available_gpu_ids": [0, 1]}
	  ]
	}`)
	d, err := fleet.ParseDeclaration(document)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Nodes) != 2 || d.Nodes[0].ID != "alpha" {
		t.Fatalf("the declaration did not come back in declared order: %+v", d.Nodes)
	}
	plan, err := fleet.Plan(d.Convention, d.Nodes)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Master.ID != "alpha" || plan.World != 2 {
		t.Fatalf("plan from declaration is wrong: %+v", plan)
	}
	for name, bad := range map[string]string{
		"node outside the declared ids": `{"convention":{"ids":["alpha"],"roles":["train"],"training_role":"train","networks":["10.9.0.0/16"],"default_port":7000},"nodes":[{"id":"gamma","private_ip":"10.9.0.3","interface":"eth0","role":"train","available_gpu_ids":[0]}]}`,
		"address outside the networks":  `{"convention":{"ids":["alpha"],"roles":["train"],"training_role":"train","networks":["10.9.0.0/16"],"default_port":7000},"nodes":[{"id":"alpha","private_ip":"8.8.8.8","interface":"eth0","role":"train","available_gpu_ids":[0]}]}`,
		"not json":                      `{`,
	} {
		if _, err := fleet.ParseDeclaration([]byte(bad)); err == nil {
			t.Errorf("%s: the declaration was accepted", name)
		}
	}
}
