//go:build linux

package fleet

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type processIdentity struct {
	Start      string
	Executable string
}

func runningProcessIdentity(pid int, fallback string) (processIdentity, bool) {
	if pid <= 0 {
		return processIdentity{}, false
	}
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return processIdentity{}, false
	}
	closeName := strings.LastIndexByte(string(stat), ')')
	if closeName < 0 {
		return processIdentity{}, false
	}
	fields := strings.Fields(string(stat)[closeName+1:])
	if len(fields) < 20 {
		return processIdentity{}, false
	}
	executable, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(pid), "exe"))
	if err != nil {
		executable = fallback
	}
	return processIdentity{Start: fields[19], Executable: executable}, true
}
