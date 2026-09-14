//go:build !unix

package fleet

type processIdentity struct {
	Start      string
	Executable string
}

// Process identity cannot be proven with this module on non-Unix hosts. A
// restored job is therefore reported unknown instead of attaching to a PID
// that may already belong to another process.
func runningProcessIdentity(_ int, _ string) (processIdentity, bool) {
	return processIdentity{}, false
}
