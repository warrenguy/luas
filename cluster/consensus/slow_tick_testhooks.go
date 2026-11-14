//go:build testhooks

package consensus

// SlowTickIntervalNsForTest returns the slow tick interval in nanoseconds.
func SlowTickIntervalNsForTest() int64 {
	return slowTickIntervalNs
}
