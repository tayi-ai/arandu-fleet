package fleet

import (
	"encoding/json"
	"fmt"
)

// Declaration is a whole fleet in one document: the convention it obeys and
// the nodes that obey it.
//
// It exists because the two halves are one decision. An inventory read without
// the convention it was written for is a list of names nothing can check, and a
// convention without an inventory checks nothing. Reading both from the same
// file is what makes "these are the rules, and here is what follows them" a
// single artifact somebody can hand over, diff and version.
//
// The file carries no address of this installation's making beyond the nodes
// themselves, and never a token: the credential that authorizes occupying a
// card is configuration of the process, not of the fleet.
type Declaration struct {
	Convention Convention `json:"convention"`
	Nodes      []Node     `json:"nodes"`
}

// ParseDeclaration reads a declaration, compiles its convention and validates
// its inventory against it, so that a caller holding the result holds something
// already known to be addressable.
func ParseDeclaration(document []byte) (Declaration, error) {
	var d Declaration
	if err := json.Unmarshal(document, &d); err != nil {
		return Declaration{}, fmt.Errorf("fleet: reading the declaration: %w", err)
	}
	convention, err := d.Convention.Compile()
	if err != nil {
		return Declaration{}, err
	}
	d.Convention = convention
	if err := ValidateInventory(d.Convention, d.Nodes); err != nil {
		return Declaration{}, err
	}
	d.Nodes = InDeclaredOrder(d.Convention, d.Nodes)
	return d, nil
}

// MarshalJSON writes the convention back out, including the pattern it settled
// on, so a declaration that was read and written again says the same thing.
func (c Convention) MarshalJSON() ([]byte, error) {
	type plain struct {
		IDs              []string `json:"ids"`
		Roles            []string `json:"roles"`
		TrainingRole     string   `json:"training_role"`
		Networks         []string `json:"networks"`
		DefaultPort      int      `json:"default_port"`
		InterfacePattern string   `json:"interface_pattern,omitempty"`
	}
	return json.Marshal(plain{c.IDs, c.Roles, c.TrainingRole, c.Networks, c.DefaultPort, c.InterfacePattern})
}

// UnmarshalJSON reads the declared half; the compiled half is built by Compile.
func (c *Convention) UnmarshalJSON(data []byte) error {
	type plain struct {
		IDs              []string `json:"ids"`
		Roles            []string `json:"roles"`
		TrainingRole     string   `json:"training_role"`
		Networks         []string `json:"networks"`
		DefaultPort      int      `json:"default_port"`
		InterfacePattern string   `json:"interface_pattern"`
	}
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	c.IDs, c.Roles, c.TrainingRole = p.IDs, p.Roles, p.TrainingRole
	c.Networks, c.DefaultPort, c.InterfacePattern = p.Networks, p.DefaultPort, p.InterfacePattern
	return nil
}
