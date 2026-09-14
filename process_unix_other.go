//go:build unix && !linux

package fleet

type processIdentity struct {
	Start      string
	Executable string
}

func runningProcessIdentity(_ int, _ string) (processIdentity, bool) {
	return processIdentity{}, false
}
