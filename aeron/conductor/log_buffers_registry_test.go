package conductor_test

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron/conductor"
)

func writeMinimalLogFileForRegistryTest(tb testing.TB, path string) {
	tb.Helper()
	conductor.WriteMinimalLogFileForTest(tb, path)
}

const (
	releaseUnderflowPanic = "conductor: log buffer release underflow"
	negativeLingerPanic   = "conductor: negative linger duration"
)

func newRegistryWithLogFile(
	tb testing.TB,
	logFileName string,
) (*conductor.LogBuffersRegistry, string) {
	tb.Helper()

	dir := tb.TempDir()
	logPath := filepath.Join(dir, logFileName)
	writeMinimalLogFileForRegistryTest(tb, logPath)
	registry := conductor.NewLogBuffersRegistry(dir)
	tb.Cleanup(func() { conductor.CloseLogBuffersRegistryForTest(registry) })
	return registry, logPath
}

func newRegistryWithAcquiredLog(
	t *testing.T,
	logFileName string,
) (*conductor.LogBuffersRegistry, string) {
	t.Helper()

	registry, logPath := newRegistryWithLogFile(t, logFileName)
	_, err := registry.Acquire(logPath)
	require.NoError(t, err)
	return registry, logPath
}

func scheduleLingerForRegistryLog(
	t *testing.T,
	registry *conductor.LogBuffersRegistry,
	logPath string,
	startNs int64,
	lingerNs int64,
) {
	t.Helper()

	key, err := conductor.ResolveLogFileKeyForTest(registry, logPath)
	require.NoError(t, err)
	conductor.ReleaseResolvedKeyAtForTest(registry, key, startNs, lingerNs)
}

func registrySweepForTest(registry *conductor.LogBuffersRegistry) func(int64) int {
	return func(nowNs int64) int {
		return conductor.SweepLingeringLogBuffersForTest(registry, nowNs)
	}
}

func assertRegistryLingerScheduled(
	t *testing.T,
	registry *conductor.LogBuffersRegistry,
	logPath string,
	startNs, lingerNs int64,
) {
	t.Helper()

	require.Zero(t, conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
	require.Equal(t, startNs+lingerNs, conductor.LingerDeadlineNsForTest(registry, logPath))
	assertRegistryLingerScheduledMatchesScan(t, registry)
}

func assertRegistryLingerScheduledMatchesScan(
	t *testing.T,
	registry *conductor.LogBuffersRegistry,
) {
	t.Helper()

	require.Equal(
		t,
		conductor.LogBuffersRegistryCountScheduledLingersForTest(registry),
		conductor.LogBuffersRegistryLingerScheduledForTest(registry),
	)
	require.Equal(
		t,
		conductor.LogBuffersRegistryLingerScheduledForTest(registry) > 0,
		registry.HasScheduledLinger(),
	)
}

func assertLingerRetainedAt(
	t *testing.T,
	registry *conductor.LogBuffersRegistry,
	logPath string,
	sweep func(nowNs int64) int,
	nowNs int64,
) {
	t.Helper()

	require.Zero(t, sweep(nowNs))
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func assertLingerSweptAt(
	t *testing.T,
	registry *conductor.LogBuffersRegistry,
	logPath string,
	sweep func(nowNs int64) int,
	nowNs int64,
) {
	t.Helper()

	require.Equal(t, 1, sweep(nowNs))
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func newSeededRegistryWithRefCount(
	t *testing.T,
	logFileName string,
	refCount int32,
) (*conductor.LogBuffersRegistry, string) {
	t.Helper()

	logPath := filepath.Join(t.TempDir(), logFileName)
	registry := conductor.NewLogBuffersRegistry("")
	require.NoError(t, conductor.SeedLogBuffersRegistryForTest(
		registry,
		logPath,
		conductor.NewSyntheticLogBuffersForTest(t),
	))
	require.NoError(t, conductor.SetLogBuffersRegistryRefCountForTest(registry, logPath, refCount))
	return registry, logPath
}

func TestLogBuffersRegistry_Acquire_errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		logPath func(t *testing.T) string
		wantErr error
	}{
		{
			name:    "emptyPath",
			logPath: func(t *testing.T) string { return "" },
			wantErr: conductor.ErrLogFileNameEmpty,
		},
		{
			name: "missingFile",
			logPath: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "missing.log")
			},
			wantErr: conductor.ErrLogBufferMapFailed,
		},
		{
			name: "finalSymlink",
			logPath: func(t *testing.T) string {
				dir := t.TempDir()
				target := filepath.Join(dir, "target.log")
				writeMinimalLogFileForRegistryTest(t, target)
				link := filepath.Join(dir, "link.log")
				require.NoError(t, os.Symlink(target, link))
				return link
			},
			wantErr: conductor.ErrLogBufferMapFailed,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			registry := conductor.NewLogBuffersRegistry("")
			_, err := registry.Acquire(testCase.logPath(t))
			require.ErrorIs(t, err, testCase.wantErr)
		})
	}
}

func TestLogBuffersRegistry_Acquire_dedupPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(t *testing.T, dir string) (pathA, pathB string)
	}{
		{
			name: "symlink",
			setup: func(t *testing.T, dir string) (string, string) {
				targetPath := filepath.Join(dir, "target.log")
				writeMinimalLogFileForRegistryTest(t, targetPath)

				aliasPath := filepath.Join(dir, "alias.log")
				require.NoError(t, os.Symlink(targetPath, aliasPath))
				return aliasPath, targetPath
			},
		},
		{
			name: "canonical",
			setup: func(t *testing.T, dir string) (string, string) {
				logPath := filepath.Join(dir, "shared.log")
				writeMinimalLogFileForRegistryTest(t, logPath)
				return logPath, filepath.Join(dir, ".", "shared.log")
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			pathA, pathB := testCase.setup(t, dir)
			registry := conductor.NewLogBuffersRegistry(dir)

			first, err := registry.Acquire(pathA)
			require.NoError(t, err)
			second, err := registry.Acquire(pathB)
			require.NoError(t, err)
			require.Same(t, first, second)
			require.Equal(t, int32(2), conductor.LogBuffersRegistryRefCountForTest(registry, pathA))
			require.Equal(t, int32(2), conductor.LogBuffersRegistryRefCountForTest(registry, pathB))
		})
	}
}

func TestLogBuffersRegistry_Acquire_corruptLogFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "corrupt.log")
	require.NoError(t, os.WriteFile(logPath, []byte{0, 0, 0}, 0o644))

	registry := conductor.NewLogBuffersRegistry(dir)
	_, err := registry.Acquire(logPath)
	require.ErrorIs(t, err, conductor.ErrLogBufferMapFailed)
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestLogBuffersRegistry_Release_panics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(t *testing.T) (*conductor.LogBuffersRegistry, string)
		lingerNs  int64
		wantPanic string
	}{
		{
			name: "overRelease",
			setup: func(t *testing.T) (*conductor.LogBuffersRegistry, string) {
				return newSeededRegistryWithRefCount(t, "over-release.log", 0)
			},
			wantPanic: releaseUnderflowPanic,
		},
		{
			name: "negativeLinger",
			setup: func(t *testing.T) (*conductor.LogBuffersRegistry, string) {
				return newRegistryWithAcquiredLog(t, "negative-linger.log")
			},
			lingerNs:  -1,
			wantPanic: negativeLingerPanic,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			registry, logPath := testCase.setup(t)
			require.PanicsWithValue(
				t,
				testCase.wantPanic,
				func() { registry.Release(logPath, testCase.lingerNs) },
			)
		})
	}
}

func TestSetLogBuffersRegistryRefCountForTest_errors(t *testing.T) {
	t.Parallel()

	t.Run("resolveFailure", func(t *testing.T) {
		t.Parallel()
		registry := conductor.NewLogBuffersRegistry("")
		err := conductor.SetLogBuffersRegistryRefCountForTest(registry, "", 0)
		require.ErrorIs(t, err, conductor.ErrLogFileNameEmpty)
	})

	t.Run("missingEntry", func(t *testing.T) {
		t.Parallel()
		logPath := filepath.Join(t.TempDir(), "not-seeded.log")
		registry := conductor.NewLogBuffersRegistry("")
		err := conductor.SetLogBuffersRegistryRefCountForTest(registry, logPath, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not in registry")
	})
}

func TestLogBuffersRegistry_releaseResolvedKey_skipsReResolve(t *testing.T) {
	t.Parallel()

	registry, logPath := newRegistryWithAcquiredLog(t, "shared.log")
	resolvedKey, err := conductor.ResolveLogFileKeyForTest(registry, logPath)
	require.NoError(t, err)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))

	registry.Release("/tmp/\xff.log", 0)
	require.Equal(
		t,
		int32(1),
		conductor.LogBuffersRegistryRefCountByResolvedKeyForTest(registry, resolvedKey),
	)

	conductor.ReleaseResolvedKeyForTest(registry, resolvedKey, 0)
	require.Zero(
		t,
		conductor.LogBuffersRegistryRefCountByResolvedKeyForTest(registry, resolvedKey),
	)
}

func TestLogBuffersRegistry_Release_unknownPath(t *testing.T) {
	t.Parallel()

	registry, logPath := newRegistryWithAcquiredLog(t, "shared.log")

	// Footgun: unknown paths are an intentional silent no-op, unlike over-release
	// which panics. Callers must pair Release with a prior Acquire.
	require.NotPanics(t, func() {
		registry.Release(filepath.Join(filepath.Dir(logPath), "missing.log"), 0)
	})
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestLogBuffersRegistry_aliasPath_resolvesToKey(t *testing.T) {
	t.Parallel()

	registry, logPath := newRegistryWithAcquiredLog(t, "shared.log")
	aliasPath := filepath.Join(filepath.Dir(logPath), ".", "shared.log")
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, aliasPath))
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, aliasPath))

	registry.Release(aliasPath, 0)
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestLogBuffersRegistry_SeedForTest_refcount(t *testing.T) {
	t.Parallel()

	logBuffers := conductor.NewSyntheticLogBuffersForTest(t)

	logPath := filepath.Join(t.TempDir(), "seeded.log")
	registry := conductor.NewLogBuffersRegistry("")
	require.NoError(t, conductor.SeedLogBuffersRegistryForTest(registry, logPath, logBuffers))

	acquired, err := registry.Acquire(logPath)
	require.NoError(t, err)
	require.Same(t, logBuffers, acquired)
	require.Equal(t, int32(2), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))

	registry.Release(logPath, 0)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestLogBuffersRegistry_SeedForTest_errors(t *testing.T) {
	t.Parallel()

	registry := conductor.NewLogBuffersRegistry("")
	logBuffers := conductor.NewSyntheticLogBuffersForTest(t)

	require.EqualError(
		t,
		conductor.SeedLogBuffersRegistryForTest(nil, "path", logBuffers),
		"conductor: log buffers registry is nil",
	)
	require.EqualError(
		t,
		conductor.SeedLogBuffersRegistryForTest(registry, "", logBuffers),
		"conductor: log file name is empty",
	)
	require.EqualError(
		t,
		conductor.SeedLogBuffersRegistryForTest(registry, "path", nil),
		"conductor: log buffers is nil",
	)
	require.NoError(t, conductor.SeedLogBuffersRegistryForTest(registry, "path", logBuffers))
	require.EqualError(
		t,
		conductor.SeedLogBuffersRegistryForTest(registry, "path", logBuffers),
		`conductor: log file "path" already registered`,
	)
}

func TestLogBuffersRegistry_AcquireRelease_refcount(t *testing.T) {
	t.Parallel()

	registry, logPath := newRegistryWithLogFile(t, "shared.log")

	first, err := registry.Acquire(logPath)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))

	second, err := registry.Acquire(logPath)
	require.NoError(t, err)
	require.Same(t, first.MappedLogBuffer(), second.MappedLogBuffer())
	require.Equal(t, int32(2), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))

	registry.Release(logPath, 0)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))

	registry.Release(logPath, 0)
	require.Zero(t, conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.False(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
}

func TestLogBuffersRegistry_Release_emptyPathNoOp(t *testing.T) {
	t.Parallel()

	registry, logPath := newRegistryWithAcquiredLog(t, "shared.log")
	registry.Release(logPath, 0)
	require.NotPanics(t, func() { registry.Release("", 0) })
	require.Zero(t, conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
}

func TestLogBuffersRegistry_Release_lingerDefersUnmap(t *testing.T) {
	t.Parallel()

	const startNs int64 = 4_000_000_000
	const lingerNs int64 = int64(3 * time.Second)

	registry, logPath := newRegistryWithAcquiredLog(t, "registry-linger.log")
	scheduleLingerForRegistryLog(t, registry, logPath, startNs, lingerNs)
	assertRegistryLingerScheduled(t, registry, logPath, startNs, lingerNs)

	sweep := registrySweepForTest(registry)
	assertLingerRetainedAt(t, registry, logPath, sweep, startNs+lingerNs-1)
	assertLingerSweptAt(t, registry, logPath, sweep, startNs+lingerNs)
	require.Zero(t, sweep(startNs+lingerNs))
}

func TestLogBuffersRegistry_Release_positiveLingerIgnoredUntilFinalRelease(t *testing.T) {
	t.Parallel()

	const lingerNs int64 = int64(time.Hour)

	registry, logPath := newRegistryWithLogFile(t, "positive-linger-refcount.log")

	_, err := registry.Acquire(logPath)
	require.NoError(t, err)
	_, err = registry.Acquire(logPath)
	require.NoError(t, err)
	require.Equal(t, int32(2), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))

	registry.Release(logPath, lingerNs)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
	require.Zero(t, conductor.LingerDeadlineNsForTest(registry, logPath))
	assertRegistryLingerScheduledMatchesScan(t, registry)
	require.Zero(t, conductor.LogBuffersRegistryLingerScheduledForTest(registry))

	registry.Release(logPath, lingerNs)
	require.Zero(t, conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))
	require.NotZero(t, conductor.LingerDeadlineNsForTest(registry, logPath))
	assertRegistryLingerScheduledMatchesScan(t, registry)
	require.Equal(t, 1, conductor.LogBuffersRegistryLingerScheduledForTest(registry))
}

func TestLogBuffersRegistry_Release_lingerSchedulesUnmap(t *testing.T) {
	t.Parallel()

	const lingerNs int64 = int64(time.Hour)

	registry, logPath := newRegistryWithAcquiredLog(t, "public-release-linger.log")
	registry.Release(logPath, lingerNs)
	require.Zero(t, conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.True(t, conductor.LogBuffersRegistryContainsForTest(registry, logPath))

	deadline := conductor.LingerDeadlineNsForTest(registry, logPath)
	require.NotZero(t, deadline)
	sweep := registrySweepForTest(registry)
	assertLingerRetainedAt(t, registry, logPath, sweep, 0)
	assertLingerSweptAt(t, registry, logPath, sweep, deadline)
}

func TestLogBuffersRegistry_acquireAfterLingerScheduled_keepsLinger(t *testing.T) {
	t.Parallel()

	const startNs int64 = 5_000_000_000
	const lingerNs int64 = int64(time.Second)

	registry, logPath := newRegistryWithAcquiredLog(t, "reacquire.log")
	scheduleLingerForRegistryLog(t, registry, logPath, startNs, lingerNs)
	deadline := conductor.LingerDeadlineNsForTest(registry, logPath)
	require.NotZero(t, deadline)

	// Release after the linger starts does nothing.
	registry.Release(logPath, 0)
	require.Equal(t, deadline, conductor.LingerDeadlineNsForTest(registry, logPath))

	// Acquire maps the file again. The lingering mapping keeps its deadline.
	_, err := registry.Acquire(logPath)
	require.NoError(t, err)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
	require.Equal(t, 2, conductor.LogBuffersRegistryEntryCountForTest(registry))
	require.Equal(t, deadline, conductor.LingerDeadlineNsForTest(registry, logPath))

	// Zero-linger release unmaps only the new mapping.
	registry.Release(logPath, 0)
	require.Equal(t, 1, conductor.LogBuffersRegistryEntryCountForTest(registry))
	require.Equal(t, deadline, conductor.LingerDeadlineNsForTest(registry, logPath))

	sweep := registrySweepForTest(registry)
	assertLingerRetainedAt(t, registry, logPath, sweep, deadline-1)
	assertLingerSweptAt(t, registry, logPath, sweep, deadline)
}

func TestLogBuffersRegistry_lingerScheduled_nilRegistry(t *testing.T) {
	t.Parallel()

	var nilRegistry *conductor.LogBuffersRegistry
	require.False(t, nilRegistry.HasScheduledLinger())
	require.Zero(t, conductor.LogBuffersRegistryLingerScheduledForTest(nil))
	require.Zero(t, conductor.LogBuffersRegistryCountScheduledLingersForTest(nil))
}

func TestLogBuffersRegistry_SweepLingering_maxInt64(t *testing.T) {
	t.Parallel()

	const startNs int64 = 6_000_000_000

	tests := []struct {
		name           string
		acquires       int
		releases       int
		lingerNs       int64
		sweepNs        int64
		wantSwept      int
		wantContains   bool
		wantRefCount   int32
		wantDeadlineNs int64
	}{
		{
			name:      "lingeringBeforeDeadline",
			acquires:  1,
			releases:  1,
			lingerNs:  int64(time.Hour),
			sweepNs:   math.MaxInt64,
			wantSwept: 1,
		},
		{
			name:           "saturatedDeadlineRetainedBelowMax",
			acquires:       1,
			releases:       1,
			lingerNs:       math.MaxInt64,
			sweepNs:        math.MaxInt64 - 1,
			wantContains:   true,
			wantDeadlineNs: math.MaxInt64,
		},
		{
			name:      "saturatedDeadlineSweptAtMax",
			acquires:  1,
			releases:  1,
			lingerNs:  math.MaxInt64,
			sweepNs:   math.MaxInt64,
			wantSwept: 1,
		},
		{
			name:         "positiveRefCountStaysMapped",
			acquires:     2,
			releases:     1,
			lingerNs:     int64(time.Hour),
			sweepNs:      math.MaxInt64,
			wantContains: true,
			wantRefCount: 1,
		},
		{
			name:    "emptyRegistry",
			sweepNs: math.MaxInt64,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			registry, logPath := newRegistryWithLogFile(t, "sweep-max.log")
			key, err := conductor.ResolveLogFileKeyForTest(registry, logPath)
			require.NoError(t, err)
			for range test.acquires {
				_, err := registry.Acquire(logPath)
				require.NoError(t, err)
			}
			for range test.releases {
				conductor.ReleaseResolvedKeyAtForTest(registry, key, startNs, test.lingerNs)
			}

			swept := conductor.SweepLingeringLogBuffersForTest(registry, test.sweepNs)
			require.Equal(t, test.wantSwept, swept)
			require.Equal(
				t, test.wantContains, conductor.LogBuffersRegistryContainsForTest(registry, logPath),
			)
			require.Equal(
				t, test.wantRefCount, conductor.LogBuffersRegistryRefCountForTest(registry, logPath),
			)
			require.Equal(
				t, test.wantDeadlineNs, conductor.LingerDeadlineNsForTest(registry, logPath),
			)
			require.Equal(t, test.wantDeadlineNs != 0, registry.HasScheduledLinger())
			assertRegistryLingerScheduledMatchesScan(t, registry)
		})
	}
}

func TestSweepLingeringLogBuffers_nilRegistry(t *testing.T) {
	t.Parallel()

	require.Zero(t, conductor.SweepLingeringLogBuffersForTest(nil, 0))
}

func TestLogBuffersRegistry_lingerScheduledCounter(t *testing.T) {
	t.Parallel()

	const startNs int64 = 6_000_000_000
	const lingerNs int64 = int64(time.Second)

	registry, logPath := newRegistryWithAcquiredLog(t, "linger-counter.log")
	assertRegistryLingerScheduledMatchesScan(t, registry)
	require.Zero(t, conductor.LogBuffersRegistryLingerScheduledForTest(registry))

	_, err := registry.Acquire(logPath)
	require.NoError(t, err)
	registry.Release(logPath, int64(time.Hour))
	assertRegistryLingerScheduledMatchesScan(t, registry)
	require.Zero(t, conductor.LogBuffersRegistryLingerScheduledForTest(registry))

	scheduleLingerForRegistryLog(t, registry, logPath, startNs, lingerNs)
	assertRegistryLingerScheduledMatchesScan(t, registry)
	require.Equal(t, 1, conductor.LogBuffersRegistryLingerScheduledForTest(registry))

	_, err = registry.Acquire(logPath)
	require.NoError(t, err)
	assertRegistryLingerScheduledMatchesScan(t, registry)
	require.Equal(t, 1, conductor.LogBuffersRegistryLingerScheduledForTest(registry))

	registry.Release(logPath, 0)
	assertRegistryLingerScheduledMatchesScan(t, registry)
	require.Equal(t, 1, conductor.LogBuffersRegistryLingerScheduledForTest(registry))

	registry, logPath = newRegistryWithAcquiredLog(t, "linger-counter-sweep.log")
	scheduleLingerForRegistryLog(t, registry, logPath, startNs, lingerNs)
	require.Equal(t, 1, conductor.LogBuffersRegistryLingerScheduledForTest(registry))

	sweep := registrySweepForTest(registry)
	require.Equal(t, 1, sweep(startNs+lingerNs))
	assertRegistryLingerScheduledMatchesScan(t, registry)
	require.Zero(t, conductor.LogBuffersRegistryLingerScheduledForTest(registry))
}
