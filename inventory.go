package cluster

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strings"
)

// Node is one machine of the fleet as the inventory declares it.
//
// Eligibility comes from measurement and never from the node's number: a node
// is eligible when it declares a role and carries the GPU indexes measured free
// on the host. An empty list is the same thing as an occupied card.
type Node struct {
	// ID is an English cardinal numeral: one, two, ... twentyone. Never an
	// integer, so a node cannot be selected by arithmetic on its name.
	ID string `json:"id"`
	// IP is the private address on the cluster network: RFC 1918 or the
	// Tailscale CGNAT range. A public address is refused.
	IP string `json:"private_ip"`
	// Interface is the network interface the collectives bind to.
	Interface string `json:"interface"`
	// Role is train, rollout or eval, and must be declared.
	Role string `json:"role"`
	// AvailableGPUIDs are the card indexes measured free on the host.
	AvailableGPUIDs []int `json:"available_gpu_ids"`
}

// The roles a node may declare.
const (
	RoleTrain   = "train"
	RoleRollout = "rollout"
	RoleEval    = "eval"
)

var roles = map[string]bool{RoleTrain: true, RoleRollout: true, RoleEval: true}

var interfaceName = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,32}$`)

var numerals = []string{
	"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
	"eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen",
	"eighteen", "nineteen", "twenty", "twentyone", "twentytwo", "twentythree",
	"twentyfour", "twentyfive",
}

var ordinal = func() map[string]int {
	m := make(map[string]int, len(numerals))
	for i, n := range numerals {
		m[n] = i + 1
	}
	return m
}()

var cgnat = func() *net.IPNet { _, n, _ := net.ParseCIDR("100.64.0.0/10"); return n }()

// IsRole reports whether r is a declared role.
func IsRole(r string) bool { return roles[r] }

// ValidateInventory refuses every inventory the control plane must not act on.
//
// It is the last line of defense, not the first: the first is never writing
// the address of an ineligible node anywhere.
func ValidateInventory(nodes []Node) error {
	if len(nodes) == 0 {
		return errors.New("cluster: inventory is empty")
	}
	ids := map[string]bool{}
	ips := map[string]bool{}
	for _, n := range nodes {
		if _, ok := ordinal[n.ID]; !ok {
			return fmt.Errorf("cluster: node %q: id must be an english cardinal numeral (one, two, ... twentyone)", n.ID)
		}
		if ids[n.ID] {
			return fmt.Errorf("cluster: node %q: duplicate id", n.ID)
		}
		ip := net.ParseIP(n.IP)
		if ip == nil || ip.To4() == nil || !(ip.IsPrivate() || cgnat.Contains(ip)) {
			return fmt.Errorf("cluster: node %q: private_ip must be a private IPv4 address (RFC 1918) or a Tailscale CGNAT address (100.64.0.0/10)", n.ID)
		}
		if ips[n.IP] {
			return fmt.Errorf("cluster: node %q: duplicate private_ip", n.ID)
		}
		if !interfaceName.MatchString(n.Interface) {
			return fmt.Errorf("cluster: node %q: interface is missing or malformed", n.ID)
		}
		if !roles[n.Role] {
			return fmt.Errorf("cluster: node %q: role must be declared as train, rollout or eval", n.ID)
		}
		if len(n.AvailableGPUIDs) == 0 {
			return fmt.Errorf("cluster: node %q: available_gpu_ids is empty; a node without measured free GPUs is not eligible", n.ID)
		}
		seen := map[int]bool{}
		for _, g := range n.AvailableGPUIDs {
			if g < 0 {
				return fmt.Errorf("cluster: node %q: available_gpu_ids has a negative index", n.ID)
			}
			if seen[g] {
				return fmt.Errorf("cluster: node %q: available_gpu_ids repeats index %d", n.ID, g)
			}
			seen[g] = true
		}
		ids[n.ID] = true
		ips[n.IP] = true
	}
	return nil
}

// ByNumeral returns the nodes sorted by their numeral, which is the only order
// the inventory has.
func ByNumeral(nodes []Node) []Node {
	out := append([]Node(nil), nodes...)
	sort.Slice(out, func(i, j int) bool { return ordinal[out[i].ID] < ordinal[out[j].ID] })
	return out
}

// WithRole keeps the nodes declaring the role; "all" keeps every node.
func WithRole(nodes []Node, role string) []Node {
	out := []Node{}
	for _, n := range nodes {
		if role == "all" || n.Role == role {
			out = append(out, n)
		}
	}
	return out
}

// GPUList renders the measured indexes the way the node environment carries
// them: "0,1".
func GPUList(ids []int) string {
	s := make([]string, len(ids))
	for i, g := range ids {
		s[i] = fmt.Sprint(g)
	}
	return strings.Join(s, ",")
}

// Rank is one node's place in the training group.
type Rank struct {
	Node     Node
	NodeRank int
}

// TrainingPlan is the distributed group the inventory yields.
//
// Master, rank and world come from the position inside the group and from the
// measured GPUs, never from the node's number: the first train node in numeral
// order is the master, and the world is the sum of measured cards.
type TrainingPlan struct {
	Master Node
	Ranks  []Rank
	World  int
}

// Plan computes the training group from the inventory.
func Plan(nodes []Node) (TrainingPlan, error) {
	if err := ValidateInventory(nodes); err != nil {
		return TrainingPlan{}, err
	}
	train := WithRole(ByNumeral(nodes), RoleTrain)
	if len(train) == 0 {
		return TrainingPlan{}, errors.New("cluster: no node declares role train; nothing to rank")
	}
	plan := TrainingPlan{Master: train[0]}
	for i, n := range train {
		plan.Ranks = append(plan.Ranks, Rank{Node: n, NodeRank: i})
		plan.World += len(n.AvailableGPUIDs)
	}
	return plan, nil
}

// ExpectedCollectiveSum is what a successful all-reduce of the ranks reports:
// the sum of 0..world-1.
func (p TrainingPlan) ExpectedCollectiveSum() int { return p.World * (p.World - 1) / 2 }
