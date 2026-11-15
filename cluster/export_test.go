package cluster

import "github.com/warrenguy/luas/aeron"

// MemberAeronConfigForTest returns the aeron config OpenConsensusModule
// passes to Connect for an owned client.
func MemberAeronConfigForTest(cfg aeron.Config) aeron.Config {
	return memberAeronConfig(cfg)
}

// LockFileFd exposes the cluster.lock descriptor for black-box lock tests.
//
// Returns -1 when recovery is nil or when the lock file is already closed.
func LockFileFd(r *Recovery) int {
	if r == nil || r.lockFile == nil {
		return -1
	}
	return int(r.lockFile.Fd())
}
