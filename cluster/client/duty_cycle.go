package client

import "github.com/warrenguy/luas/idlestrategy"

// SpinOnce runs one duty-cycle iteration and returns the PollEgress work count.
//
// SpinOnce calls DoWork when the Aeron client has a conductor invoker. It then
// calls PollEgress and passes the work count to the config IdleStrategy.
// SpinOnce does not call Reset.
//
// Nil receiver returns 0. PollEgress returns 0 when the session is closed.
// Injected-transport sessions may keep a non-nil Aeron after Close, so DoWork
// can still run.
//
// Not safe for concurrent use on the same session.
func (ac *AeronCluster) SpinOnce() int {
	if ac == nil {
		return 0
	}
	return ac.spinOnce(ac.cfg.IdleStrategy)
}

func (ac *AeronCluster) spinOnce(idle idlestrategy.IdleStrategy) int {
	if client := ac.aeron; client.HasConductorInvoker() {
		client.DoWork()
		recordSpinOnceOp("DoWork")
	}

	recordSpinOnceOp("PollEgress")
	work := ac.PollEgress()
	if idle != nil {
		idle.Idle(work)
	}
	return work
}
