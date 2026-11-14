//go:build rapid

package conductor_test

import (
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/luas/aeron/conductor"
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/internal/testutil"
)

const registryRapidBaseNowNs int64 = 1_000_000_000

type registryRapidOpKind int

const (
	registryRapidOpAcquire registryRapidOpKind = iota
	registryRapidOpRelease
	registryRapidOpSweep
	registryRapidOpClosePublication
	registryRapidOpUnmapLingering
	registryRapidOpStopUnmapper
)

const registryRapidClosePublicationRegID int64 = 42

var (
	registryRapidOpsNoRelease = []registryRapidOpKind{
		registryRapidOpAcquire,
		registryRapidOpSweep,
		registryRapidOpUnmapLingering,
	}
	registryRapidOpsWithRelease = []registryRapidOpKind{
		registryRapidOpAcquire,
		registryRapidOpRelease,
		registryRapidOpSweep,
		registryRapidOpUnmapLingering,
	}
)

// registryRapidMapping is one mapping that left the live map with a linger.
type registryRapidMapping struct {
	mapped     *logbuffer.MappedLogBuffer
	deadlineNs int64
}

// registryRapidModel tracks every mapping of one log file: the shared live
// mapping, the lingering mappings, and the unmapped ones.
type registryRapidModel struct {
	refCount  int32
	live      *logbuffer.MappedLogBuffer
	lingering []registryRapidMapping
	unmapped  []*logbuffer.MappedLogBuffer
}

func (model *registryRapidModel) contains() bool {
	return model.refCount > 0 || len(model.lingering) > 0
}

func (model *registryRapidModel) latestDeadlineNs() int64 {
	var deadlineNs int64
	for _, mapping := range model.lingering {
		deadlineNs = max(deadlineNs, mapping.deadlineNs)
	}
	return deadlineNs
}

type registryRapidKeyState struct {
	logPath string
	key     string
	model   registryRapidModel
}

func drawRegistryRapidNowNs(rt *rapid.T, label string) int64 {
	rt.Helper()

	offset := rapid.Int64Range(-500_000_000, 2_000_000_000).Draw(rt, label)
	return registryRapidBaseNowNs + offset
}

// drawRegistryRapidLingerNs also draws math.MaxInt64, the linger that
// Client.Close uses to hold a log buffer until the unmap sweep.
func drawRegistryRapidLingerNs(rt *rapid.T, label string) int64 {
	rt.Helper()

	return rapid.OneOf(
		rapid.Int64Range(0, 1_000_000_000),
		rapid.Just(int64(math.MaxInt64)),
	).Draw(rt, label)
}

func drawRegistryRapidPositiveLingerNs(rt *rapid.T, label string) int64 {
	rt.Helper()

	return rapid.Int64Range(1, 1_000_000_000).Draw(rt, label)
}

func legalRegistryRapidOps(model registryRapidModel) []registryRapidOpKind {
	if model.refCount > 0 {
		return registryRapidOpsWithRelease
	}
	return registryRapidOpsNoRelease
}

func legalRegistryRapidOpsWithClose(
	model registryRapidModel,
	publicationInMap bool,
) []registryRapidOpKind {
	base := legalRegistryRapidOps(model)
	ops := make([]registryRapidOpKind, len(base), len(base)+1)
	copy(ops, base)
	if model.refCount == 1 && publicationInMap {
		ops = append(ops, registryRapidOpClosePublication)
	}
	return ops
}

func applyRegistryRapidRelease(
	model *registryRapidModel,
	nowNs int64,
	lingerNs int64,
) {
	if model.refCount > 1 {
		model.refCount--
		return
	}

	model.refCount = 0
	released := model.live
	model.live = nil
	if lingerNs == 0 {
		model.unmapped = append(model.unmapped, released)
		return
	}

	deadlineNs := int64(math.MaxInt64)
	if nowNs <= math.MaxInt64-lingerNs {
		deadlineNs = nowNs + lingerNs
	}
	model.lingering = append(model.lingering, registryRapidMapping{
		mapped:     released,
		deadlineNs: deadlineNs,
	})
}

// applyRegistryRapidAcquire checks that Acquire shares the live mapping and
// never revives a lingering one.
func applyRegistryRapidAcquire(
	rt *rapid.T,
	model *registryRapidModel,
	acquired *logbuffer.MappedLogBuffer,
) {
	rt.Helper()

	if model.refCount > 0 {
		require.Same(rt, model.live, acquired)
		model.refCount++
		return
	}

	for _, mapping := range model.lingering {
		require.NotSame(rt, mapping.mapped, acquired)
	}
	model.live = acquired
	model.refCount = 1
}

func applyRegistryRapidSweep(model *registryRapidModel, nowNs int64) {
	kept := model.lingering[:0]
	for _, mapping := range model.lingering {
		if nowNs < mapping.deadlineNs {
			kept = append(kept, mapping)
			continue
		}
		model.unmapped = append(model.unmapped, mapping.mapped)
	}
	model.lingering = kept
}

// assertRegistryRapidInvariants checks the registry against model. The live
// mapping and each lingering mapping must still be mapped. With syncUnmap,
// each removed mapping must be unmapped. An unmapping registry closes removed
// mappings later, so its callers pass false.
func assertRegistryRapidInvariants(
	rt *rapid.T,
	registry *conductor.LogBuffersRegistry,
	logPath string,
	model registryRapidModel,
	syncUnmap bool,
) {
	rt.Helper()

	require.Equal(
		rt,
		model.refCount,
		conductor.LogBuffersRegistryRefCountForTest(registry, logPath),
	)
	require.Equal(
		rt,
		model.contains(),
		conductor.LogBuffersRegistryContainsForTest(registry, logPath),
	)
	require.Equal(
		rt,
		model.latestDeadlineNs(),
		conductor.LingerDeadlineNsForTest(registry, logPath),
	)
	require.Equal(
		rt,
		conductor.LogBuffersRegistryCountScheduledLingersForTest(registry),
		conductor.LogBuffersRegistryLingerScheduledForTest(registry),
	)
	require.Equal(
		rt,
		conductor.LogBuffersRegistryLingerScheduledForTest(registry) > 0,
		registry.HasScheduledLinger(),
	)

	if model.live != nil {
		require.NotNil(rt, model.live.Bytes())
	}

	for _, mapping := range model.lingering {
		require.NotNil(rt, mapping.mapped.Bytes())
	}

	if syncUnmap {
		assertRegistryRapidRemovedUnmapped(rt, model)
	}
}

func assertRegistryRapidRemovedUnmapped(rt *rapid.T, model registryRapidModel) {
	rt.Helper()

	for _, mapped := range model.unmapped {
		require.Nil(rt, mapped.Bytes())
	}
}

type registryRapidStepTarget struct {
	registry         *conductor.LogBuffersRegistry
	logPath          string
	key              string
	model            *registryRapidModel
	conductor        *conductor.Conductor
	nanoNs           *int64
	resourceLingerNs int64
	publicationInMap *bool
}

func runRegistryRapidStep(
	rt *rapid.T,
	target registryRapidStepTarget,
	op registryRapidOpKind,
	step int,
) {
	rt.Helper()

	switch op {
	case registryRapidOpAcquire:
		acquired, err := target.registry.Acquire(target.logPath)
		require.NoError(rt, err)
		require.NotNil(rt, acquired)
		applyRegistryRapidAcquire(rt, target.model, acquired.MappedLogBuffer())

	case registryRapidOpRelease:
		require.Positive(rt, target.model.refCount)

		nowNs := drawRegistryRapidNowNs(rt, fmt.Sprintf("releaseNowOffset%d", step))
		lingerNs := drawRegistryRapidLingerNs(rt, fmt.Sprintf("lingerNs%d", step))
		conductor.ReleaseResolvedKeyAtForTest(target.registry, target.key, nowNs, lingerNs)
		applyRegistryRapidRelease(target.model, nowNs, lingerNs)

	case registryRapidOpSweep:
		nowNs := drawRegistryRapidNowNs(rt, fmt.Sprintf("sweepNowOffset%d", step))
		conductor.SweepLingeringLogBuffersForTest(target.registry, nowNs)
		applyRegistryRapidSweep(target.model, nowNs)

	case registryRapidOpUnmapLingering:
		conductor.SweepLingeringLogBuffersForTest(target.registry, math.MaxInt64)
		applyRegistryRapidSweep(target.model, math.MaxInt64)
		require.False(rt, target.registry.HasScheduledLinger())

	case registryRapidOpClosePublication:
		require.NotNil(rt, target.conductor)
		require.NotNil(rt, target.nanoNs)
		require.NotNil(rt, target.publicationInMap)
		require.True(rt, *target.publicationInMap)
		require.Equal(rt, int32(1), target.model.refCount)

		nowNs := drawRegistryRapidNowNs(rt, fmt.Sprintf("closeNowOffset%d", step))
		*target.nanoNs = nowNs

		require.NoError(rt, target.conductor.ClosePublication(
			stubPublication{registrationID: registryRapidClosePublicationRegID},
		))
		*target.publicationInMap = false
		require.Equal(rt, 0, conductor.PublicationCountForTest(target.conductor))

		applyRegistryRapidRelease(target.model, nowNs, target.resourceLingerNs)
	}
}

func sweepAllRegistryRapidKeys(
	rt *rapid.T,
	registry *conductor.LogBuffersRegistry,
	states []registryRapidKeyState,
	nowNs int64,
) {
	rt.Helper()

	conductor.SweepLingeringLogBuffersForTest(registry, nowNs)
	for i := range states {
		applyRegistryRapidSweep(&states[i].model, nowNs)
		assertRegistryRapidInvariants(rt, registry, states[i].logPath, states[i].model, true)
	}
}

func registryRapidKeyStateForLog(
	rt *rapid.T,
	registry *conductor.LogBuffersRegistry,
	logPath string,
) registryRapidKeyState {
	rt.Helper()

	key, err := conductor.ResolveLogFileKeyForTest(registry, logPath)
	require.NoError(rt, err)
	return registryRapidKeyState{logPath: logPath, key: key}
}

type registryCloseRapidFixture struct {
	conductor        *conductor.Conductor
	registry         *conductor.LogBuffersRegistry
	state            registryRapidKeyState
	nanoNs           *int64
	resourceLingerNs int64
}

func newRegistryCloseRapidFixture(
	tb testing.TB,
	rt *rapid.T,
) registryCloseRapidFixture {
	tb.Helper()
	rt.Helper()

	resourceLingerNs := drawRegistryRapidPositiveLingerNs(rt, "resourceLingerNs")

	dir := tb.TempDir()
	logPath := filepath.Join(dir, "close-rapid.log")
	writeMinimalLogFileForRegistryTest(tb, logPath)

	lifecycle := &lingerPublicationLifecycle{registryKey: logPath}
	conductorInstance, nanoNs := newConductorWithResourceLingerForTest(tb, resourceLingerNs, lifecycle)
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	tb.Cleanup(conductorInstance.CloseLogBuffersForTest)

	buffers, err := conductorInstance.AcquireLogBuffersForTest(logPath)
	require.NoError(tb, err)

	conductorInstance.InsertPublicationForTest(
		registryRapidClosePublicationRegID,
		stubPublication{registrationID: registryRapidClosePublicationRegID},
	)
	require.Equal(tb, 1, conductorInstance.PublicationCountForTest())

	state := registryRapidKeyStateForLog(rt, registry, logPath)
	state.model = registryRapidModel{refCount: 1, live: buffers.MappedLogBuffer()}
	return registryCloseRapidFixture{
		conductor:        conductorInstance,
		registry:         registry,
		state:            state,
		nanoNs:           nanoNs,
		resourceLingerNs: resourceLingerNs,
	}
}

func newRegistryRapidFixture(
	tb testing.TB,
	rt *rapid.T,
) (*conductor.LogBuffersRegistry, registryRapidKeyState) {
	tb.Helper()
	rt.Helper()

	registry, logPath := newRegistryWithLogFile(tb, "rapid.log")
	return registry, registryRapidKeyStateForLog(rt, registry, logPath)
}

func newMultiKeyRegistryRapidFixture(
	tb testing.TB,
	rt *rapid.T,
	keyCount int,
) (*conductor.LogBuffersRegistry, []registryRapidKeyState) {
	tb.Helper()
	rt.Helper()

	dir := tb.TempDir()
	registry := conductor.NewLogBuffersRegistry(dir)
	tb.Cleanup(func() { conductor.CloseLogBuffersRegistryForTest(registry) })
	states := make([]registryRapidKeyState, keyCount)
	for i := range keyCount {
		logPath := filepath.Join(dir, fmt.Sprintf("rapid-%d.log", i))
		writeMinimalLogFileForRegistryTest(tb, logPath)
		states[i] = registryRapidKeyStateForLog(rt, registry, logPath)
	}
	return registry, states
}

func assertOtherRegistryRapidKeysUnchanged(
	rt *rapid.T,
	registry *conductor.LogBuffersRegistry,
	states []registryRapidKeyState,
	targetIndex int,
	before []registryRapidModel,
) {
	rt.Helper()

	for i, state := range states {
		if i == targetIndex {
			continue
		}
		assertRegistryRapidInvariants(rt, registry, state.logPath, before[i], true)
	}
}

func assertRegistryRapidLingerTotal(
	rt *rapid.T,
	registry *conductor.LogBuffersRegistry,
	states []registryRapidKeyState,
) {
	rt.Helper()

	total := 0
	for _, state := range states {
		total += len(state.model.lingering)
	}
	require.Equal(rt, total, conductor.LogBuffersRegistryCountScheduledLingersForTest(registry))
}

func TestLogBuffersRegistry_acquireReleaseLinger_invariants_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		t := testutil.IterationTB(t, rt)

		registry, state := newRegistryRapidFixture(t, rt)
		model := registryRapidModel{}

		steps := rapid.IntRange(1, 24).Draw(rt, "steps")
		for step := range steps {
			op := rapid.SampledFrom(legalRegistryRapidOps(model)).Draw(rt, fmt.Sprintf("op%d", step))
			runRegistryRapidStep(rt, registryRapidStepTarget{
				registry: registry,
				logPath:  state.logPath,
				key:      state.key,
				model:    &model,
			}, op, step)
			assertRegistryRapidInvariants(rt, registry, state.logPath, model, true)
		}
	})
}

func TestLogBuffersRegistry_closePublicationLinger_invariants_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		t := testutil.IterationTB(t, rt)

		fixture := newRegistryCloseRapidFixture(t, rt)
		model := fixture.state.model
		publicationInMap := true

		steps := rapid.IntRange(1, 24).Draw(rt, "steps")
		for step := range steps {
			op := rapid.SampledFrom(legalRegistryRapidOpsWithClose(model, publicationInMap)).
				Draw(rt, fmt.Sprintf("op%d", step))
			runRegistryRapidStep(rt, registryRapidStepTarget{
				registry:         fixture.registry,
				logPath:          fixture.state.logPath,
				key:              fixture.state.key,
				model:            &model,
				conductor:        fixture.conductor,
				nanoNs:           fixture.nanoNs,
				resourceLingerNs: fixture.resourceLingerNs,
				publicationInMap: &publicationInMap,
			}, op, step)
			assertRegistryRapidInvariants(rt, fixture.registry, fixture.state.logPath, model, false)
		}
	})
}

func TestLogBuffersRegistry_multiKeyIsolation_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		t := testutil.IterationTB(t, rt)

		keyCount := rapid.IntRange(2, 8).Draw(rt, "keyCount")
		registry, states := newMultiKeyRegistryRapidFixture(t, rt, keyCount)

		steps := rapid.IntRange(1, 24).Draw(rt, "steps")
		for step := range steps {
			targetIndex := rapid.IntRange(0, keyCount-1).Draw(rt, fmt.Sprintf("targetIndex%d", step))
			target := &states[targetIndex]
			op := rapid.SampledFrom(legalRegistryRapidOps(target.model)).Draw(rt, fmt.Sprintf("op%d", step))

			if op == registryRapidOpSweep {
				nowNs := drawRegistryRapidNowNs(rt, fmt.Sprintf("sweepNowOffset%d", step))
				sweepAllRegistryRapidKeys(rt, registry, states, nowNs)
				assertRegistryRapidLingerTotal(rt, registry, states)
				continue
			}

			if op == registryRapidOpUnmapLingering {
				sweepAllRegistryRapidKeys(rt, registry, states, math.MaxInt64)
				require.False(rt, registry.HasScheduledLinger())
				continue
			}

			before := make([]registryRapidModel, len(states))
			for i, state := range states {
				before[i] = state.model
			}

			runRegistryRapidStep(rt, registryRapidStepTarget{
				registry: registry,
				logPath:  target.logPath,
				key:      target.key,
				model:    &target.model,
			}, op, step)

			assertOtherRegistryRapidKeysUnchanged(rt, registry, states, targetIndex, before)
			assertRegistryRapidInvariants(rt, registry, target.logPath, target.model, true)
			assertRegistryRapidLingerTotal(rt, registry, states)
		}
	})
}

func TestLogBuffersRegistry_unmapper_invariants_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		t := testutil.IterationTB(t, rt)

		dir := t.TempDir()
		logPath := filepath.Join(dir, "unmapper-rapid.log")
		writeMinimalLogFileForRegistryTest(t, logPath)
		registry := conductor.NewUnmappingLogBuffersRegistryForTest(dir)
		t.Cleanup(func() { conductor.CloseLogBuffersRegistryForTest(registry) })
		state := registryRapidKeyStateForLog(rt, registry, logPath)
		model := registryRapidModel{}

		steps := rapid.IntRange(1, 24).Draw(rt, "steps")
		for step := range steps {
			ops := append(slices.Clip(legalRegistryRapidOps(model)), registryRapidOpStopUnmapper)
			op := rapid.SampledFrom(ops).Draw(rt, fmt.Sprintf("op%d", step))
			if op == registryRapidOpStopUnmapper {
				conductor.StopLogBuffersRegistryUnmapperForTest(registry)
				assertRegistryRapidInvariants(rt, registry, state.logPath, model, true)
				continue
			}

			runRegistryRapidStep(rt, registryRapidStepTarget{
				registry: registry,
				logPath:  state.logPath,
				key:      state.key,
				model:    &model,
			}, op, step)
			assertRegistryRapidInvariants(rt, registry, state.logPath, model, false)
		}

		conductor.StopLogBuffersRegistryUnmapperForTest(registry)
		assertRegistryRapidInvariants(rt, registry, state.logPath, model, true)
	})
}
