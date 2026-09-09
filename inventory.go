package fleet

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
// Eligibility comes from measurement and never from the node's position: a node
// is eligible when it declares a role the installation knows and carries the
// GPU indexes measured free on the host. An empty list is the same thing as an
// occupied card.
type Node struct {
	// ID identifies the node. What counts as a valid id is the installation's
	// decision, declared in Convention.IDs; nothing here derives meaning from
	// the characters in it.
	ID string `json:"id"`
	// IP is the address on the cluster network. Which ranges are acceptable is
	// the installation's decision, declared in Convention.Networks.
	IP string `json:"private_ip"`
	// Interface is the network interface the collectives bind to.
	Interface string `json:"interface"`
	// Role is what the node is for, and must be one the installation declared.
	Role string `json:"role"`
	// AvailableGPUIDs are the card indexes measured free on the host.
	AvailableGPUIDs []int `json:"available_gpu_ids"`
	// Port is where this node's worker listens. Zero means the convention's
	// default, so an inventory only names it for the node that differs.
	Port int `json:"port,omitempty"`
}

// Convention is what an installation declares about its own fleet, so that no
// rule of this package is a constant somebody else has to live with.
//
// It is a struct and not a set of globals because a process may address more
// than one fleet: a control plane serving two customers holds two of these,
// and a rule that lived in a package variable would be one customer's rule
// applied to the other's inventory.
//
// Every field is a refusal waiting to happen, and that is the point: the
// permissive choice has to be written down by whoever wants it.
type Convention struct {
	// IDs are the identifiers a node may carry, in the order that orders the
	// fleet. The order is the declaration order, so the training group's ranks
	// follow the list rather than the alphabet or a number parsed out of a name.
	//
	// Required: an inventory whose convention names no id refuses every node,
	// which is the honest answer to "what counts as a node here?" going
	// unanswered.
	IDs []string
	// Roles are what a node may declare it is for. Required, for the same
	// reason.
	//
	// TrainingRole names the one the distributed group is built from; it has to
	// be one of these.
	Roles        []string
	TrainingRole string
	// Networks are the CIDR ranges an address may fall in. Required: a fleet
	// whose convention accepts every range accepts the public internet, and
	// this package will not assume that is what somebody meant.
	Networks []string
	// DefaultPort is where a worker listens when the node does not say.
	// Required, and refused outside 1..65535.
	DefaultPort int
	// InterfacePattern is what an interface name has to match. Empty means the
	// conservative default below, which is every name a Linux interface can
	// actually have.
	InterfacePattern string

	ids        map[string]int
	roles      map[string]bool
	networks   []*net.IPNet
	interface_ *regexp.Regexp
}

// DefaultInterfacePattern is what InterfacePattern falls back to.
const DefaultInterfacePattern = `^[a-zA-Z0-9_.-]{1,32}$`

// Compile checks the convention itself and prepares it for use.
//
// A convention is checked once, here, rather than on every node: a malformed
// CIDR in the declaration is a mistake in the installation, and finding it
// while validating the fourteenth node would report it as if the node were
// wrong.
func (c Convention) Compile() (Convention, error) {
	if len(c.IDs) == 0 {
		return c, errors.New("fleet: the convention declares no id; nothing would be a valid node")
	}
	c.ids = make(map[string]int, len(c.IDs))
	for i, id := range c.IDs {
		if strings.TrimSpace(id) == "" {
			return c, errors.New("fleet: the convention declares an empty id")
		}
		if _, repeated := c.ids[id]; repeated {
			return c, fmt.Errorf("fleet: the convention declares id %q twice, so two nodes would rank the same", id)
		}
		c.ids[id] = i
	}
	if len(c.Roles) == 0 {
		return c, errors.New("fleet: the convention declares no role; every node would be refused")
	}
	c.roles = make(map[string]bool, len(c.Roles))
	for _, r := range c.Roles {
		if strings.TrimSpace(r) == "" {
			return c, errors.New("fleet: the convention declares an empty role")
		}
		c.roles[r] = true
	}
	if !c.roles[c.TrainingRole] {
		return c, fmt.Errorf("fleet: the training role %q is not one of the declared roles; the distributed group would have no members", c.TrainingRole)
	}
	if len(c.Networks) == 0 {
		return c, errors.New("fleet: the convention declares no network; refusing to accept an address from anywhere, including the public internet")
	}
	c.networks = nil
	for _, cidr := range c.Networks {
		_, n, err := net.ParseCIDR(cidr)
		if err != nil {
			return c, fmt.Errorf("fleet: the convention declares %q as a network, which is not a CIDR: %w", cidr, err)
		}
		c.networks = append(c.networks, n)
	}
	if c.DefaultPort < 1 || c.DefaultPort > 65535 {
		return c, fmt.Errorf("fleet: the convention declares port %d, which is not a port", c.DefaultPort)
	}
	pattern := c.InterfacePattern
	if pattern == "" {
		pattern = DefaultInterfacePattern
	}
	expression, err := regexp.Compile(pattern)
	if err != nil {
		return c, fmt.Errorf("fleet: the convention's interface pattern does not compile: %w", err)
	}
	c.interface_ = expression
	c.InterfacePattern = pattern
	return c, nil
}

// Rank is where an id sits in the convention's order, and whether it is known.
func (c Convention) Rank(id string) (int, bool) {
	i, ok := c.ids[id]
	return i, ok
}

// IsRole reports whether the convention declares the role.
func (c Convention) IsRole(r string) bool { return c.roles[r] }

// PortOf is the port a node's worker listens on.
func (c Convention) PortOf(n Node) int {
	if n.Port != 0 {
		return n.Port
	}
	return c.DefaultPort
}

func (c Convention) accepts(ip net.IP) bool {
	for _, n := range c.networks {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

func (c Convention) compiled() bool { return c.ids != nil && c.roles != nil && c.interface_ != nil }

// ValidateInventory refuses every inventory the control plane must not act on,
// by the installation's own convention.
//
// It is the last line of defense, not the first: the first is never writing the
// address of an ineligible node anywhere.
func ValidateInventory(convention Convention, nodes []Node) error {
	if !convention.compiled() {
		return errors.New("fleet: the convention was not compiled; call Convention.Compile before validating an inventory")
	}
	if len(nodes) == 0 {
		return errors.New("fleet: inventory is empty")
	}
	ids := map[string]bool{}
	ips := map[string]bool{}
	for _, n := range nodes {
		if _, ok := convention.Rank(n.ID); !ok {
			return fmt.Errorf("fleet: node %q: the convention does not declare that id", n.ID)
		}
		if ids[n.ID] {
			return fmt.Errorf("fleet: node %q: duplicate id", n.ID)
		}
		ip := net.ParseIP(n.IP)
		if ip == nil || !convention.accepts(ip) {
			return fmt.Errorf("fleet: node %q: the address is not in a network the convention declares (%s)", n.ID, strings.Join(convention.Networks, ", "))
		}
		if ips[n.IP] {
			return fmt.Errorf("fleet: node %q: duplicate address", n.ID)
		}
		if !convention.interface_.MatchString(n.Interface) {
			return fmt.Errorf("fleet: node %q: interface is missing or does not match %s", n.ID, convention.InterfacePattern)
		}
		if !convention.IsRole(n.Role) {
			return fmt.Errorf("fleet: node %q: role must be one the convention declares (%s)", n.ID, strings.Join(convention.Roles, ", "))
		}
		if n.Port != 0 && (n.Port < 1 || n.Port > 65535) {
			return fmt.Errorf("fleet: node %q: port %d is not a port", n.ID, n.Port)
		}
		if len(n.AvailableGPUIDs) == 0 {
			return fmt.Errorf("fleet: node %q: available_gpu_ids is empty; a node without measured free GPUs is not eligible", n.ID)
		}
		seen := map[int]bool{}
		for _, g := range n.AvailableGPUIDs {
			if g < 0 {
				return fmt.Errorf("fleet: node %q: available_gpu_ids has a negative index", n.ID)
			}
			if seen[g] {
				return fmt.Errorf("fleet: node %q: available_gpu_ids repeats index %d", n.ID, g)
			}
			seen[g] = true
		}
		ids[n.ID] = true
		ips[n.IP] = true
	}
	return nil
}

// InDeclaredOrder returns the nodes sorted by where their id sits in the
// convention, which is the only order an inventory has.
func InDeclaredOrder(convention Convention, nodes []Node) []Node {
	out := append([]Node(nil), nodes...)
	sort.Slice(out, func(i, j int) bool {
		a, _ := convention.Rank(out[i].ID)
		b, _ := convention.Rank(out[j].ID)
		return a < b
	})
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

// GPUList renders the measured indexes the way a node environment carries
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
// measured GPUs: the first node of the training role in declared order is the
// master, and the world is the sum of measured cards.
type TrainingPlan struct {
	Master Node
	Ranks  []Rank
	World  int
}

// Plan computes the training group from the inventory.
func Plan(convention Convention, nodes []Node) (TrainingPlan, error) {
	if err := ValidateInventory(convention, nodes); err != nil {
		return TrainingPlan{}, err
	}
	train := WithRole(InDeclaredOrder(convention, nodes), convention.TrainingRole)
	if len(train) == 0 {
		return TrainingPlan{}, fmt.Errorf("fleet: no node declares the training role %q; nothing to rank", convention.TrainingRole)
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
