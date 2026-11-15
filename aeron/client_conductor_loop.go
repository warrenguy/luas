package aeron

import "github.com/warrenguy/luas/idlestrategy"

func resolveConductorIdleStrategy(cfg Config) idlestrategy.IdleStrategy {
	if cfg.ConductorIdleStrategy != nil {
		return cfg.ConductorIdleStrategy
	}
	return idlestrategy.NewSleepingMillis(cfg.IdleSleepDuration)
}

// startConductorLoop starts the conductor goroutine unless
// UseConductorInvoker is set.
func (client *Client) startConductorLoop(cfg Config) {
	if cfg.UseConductorInvoker {
		return
	}

	client.conductorLoopDone = make(chan struct{})

	go client.runConductorLoop(resolveConductorIdleStrategy(cfg))
}

// runConductorLoop calls DoWork and then Idle until Close or termination.
//
// The loop checks before each step, so a Close or termination inside DoWork
// still idles once. The loop does not Close the client.
func (client *Client) runConductorLoop(strategy idlestrategy.IdleStrategy) {
	for !client.closed.Load() && !client.IsTerminating() {
		client.conductorLoopStep(strategy)
	}

	close(client.conductorLoopDone)
}

func (client *Client) conductorLoopStep(strategy idlestrategy.IdleStrategy) {
	work := client.DoWork()
	strategy.Idle(work)
}
