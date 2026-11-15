package transportfault

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeReporter reports a fixed terminating flag and cause.
type fakeReporter struct {
	terminating bool
	cause       error
	calls       int64
}

func (reporter *fakeReporter) IsTerminating() bool {
	reporter.calls++
	return reporter.terminating
}

func (reporter *fakeReporter) TerminationCause() error {
	return reporter.cause
}

func TestMonitor_TerminationCause(t *testing.T) {
	t.Parallel()

	terminationErr := errors.New("termination fault")

	tests := []struct {
		name        string
		nilMonitor  bool
		terminating bool
		want        error
		wantCalls   int64
	}{
		{name: "nil_monitor", nilMonitor: true, terminating: true},
		{name: "running", wantCalls: 1},
		{name: "terminating", terminating: true, want: terminationErr, wantCalls: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var monitor *Monitor
			if !tc.nilMonitor {
				monitor = &Monitor{}
			}

			reporter := &fakeReporter{terminating: tc.terminating, cause: terminationErr}

			// Phase: a repeat check returns the same result.
			for check := range int64(2) {
				got := monitor.TerminationCause(reporter)
				if tc.want == nil {
					require.NoError(t, got)
				} else {
					require.ErrorIs(t, got, tc.want)
				}

				require.Equal(t, (check+1)*tc.wantCalls, reporter.calls)
			}
		})
	}
}
