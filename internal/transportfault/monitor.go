// Package transportfault reports when a connect-owned transport client has
// terminated.
package transportfault

// TerminationReporter reports whether a transport client is terminating and
// why. *aeron.Client implements it.
type TerminationReporter interface {
	IsTerminating() bool
	TerminationCause() error
}

// Monitor reports the termination cause of one connect-owned transport
// client.
//
// Concurrency: safe from any goroutine.
type Monitor struct{}

// TerminationCause returns the cause from client when client is terminating.
// Otherwise it returns nil. A nil Monitor returns nil.
func (monitor *Monitor) TerminationCause(client TerminationReporter) error {
	if monitor == nil || client == nil {
		return nil
	}

	if !client.IsTerminating() {
		return nil
	}
	return client.TerminationCause()
}
