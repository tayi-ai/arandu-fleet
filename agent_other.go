//go:build !unix

package fleet

import (
	"errors"
	"os/exec"
)

// detach is a no-op where process groups are not what this module relies on.
func detach(c *exec.Cmd) {}

// killGroup refuses rather than killing only the child. A cancel that leaves
// the spawned workers holding the cards is worse than a cancel that says it did
// nothing, because the operator then believes the node is free.
func killGroup(c *exec.Cmd) error {
	return errors.New("fleet: cancelling a run needs process groups, which this platform does not provide; run the agent on a unix host")
}

func terminateGroup(c *exec.Cmd) error {
	return errors.New("fleet: cancelling a run needs process groups, which this platform does not provide; run the agent on a unix host")
}

func terminateProcessGroup(int) error {
	return errors.New("fleet: cancelling a restored run needs process groups, which this platform does not provide; run the agent on a unix host")
}

func killProcessGroup(int) error {
	return errors.New("fleet: forcing a restored run to stop needs process groups, which this platform does not provide; run the agent on a unix host")
}
