//go:build unix

package fleet

import (
	"errors"
	"os/exec"
	"syscall"
)

// detach puts the child in its own process group, which is what makes killGroup
// able to reach everything it spawned.
func detach(c *exec.Cmd) {
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// killGroup signals the whole group the child leads.
//
// The negative pid is the point: a launcher spawns one worker process per card
// and exits before them, so signalling the launcher alone leaves the workers
// holding the memory, and the next run finds the cards already taken.
func killGroup(c *exec.Cmd) error {
	if c == nil || c.Process == nil {
		return errors.New("fleet: there is no process to signal")
	}
	return syscall.Kill(-c.Process.Pid, syscall.SIGKILL)
}
