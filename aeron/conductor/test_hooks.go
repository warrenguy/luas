//go:build testhooks

package conductor

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/atomicbuffer"
	"github.com/warrenguy/luas/internal/testhook"
)

func WriteMinimalLogFileForTest(tb testing.TB, path string) {
	tb.Helper()

	const termLength = logbuffer.TermMinLength
	const pageSize = logbuffer.PageMinSize
	computed, err := logbuffer.ComputeLogLength(termLength, pageSize)
	require.NoError(tb, err)

	buf := make([]byte, computed)
	metaStart := logbuffer.LogMetaDataOffset(computed)
	meta := atomicbuffer.Wrap(buf[metaStart : metaStart+logbuffer.MetaDataLength])
	meta.PutInt32Release(logbuffer.OffsetTermLength, termLength)
	meta.PutInt32(logbuffer.OffsetPageSize, pageSize)
	meta.PutInt32(logbuffer.OffsetMTULength, 1408)
	meta.PutInt32(logbuffer.OffsetInitialTermID, 7)
	meta.PutInt32(logbuffer.OffsetDefaultFrameHeaderLength, logbuffer.DataHeaderLength)
	require.NoError(tb, os.WriteFile(path, buf, 0o644))
}

func assertInvokeImageHandlerUnderWriteLock(savedDepth int32) {
	if savedDepth <= 0 {
		panic("conductor: invokeImageHandler called without resource write lock")
	}
}

// RegisterSubscriptionForTest is for cross-package tests behind the testhooks tag.
//
// conductor_test uses InsertSubscriptionForTest in export_test.go instead.
func RegisterSubscriptionForTest(
	conductor *Conductor,
	registrationID int64,
	sub SubscriptionHandle,
) {
	registerSubscriptionHook(conductor, registrationID, sub)
}

// AcquireLogBuffersForTest is for cross-package tests behind the testhooks tag.
//
// conductor_test uses (*Conductor).AcquireLogBuffersForTest in export_test.go instead.
func AcquireLogBuffersForTest(
	conductor *Conductor,
	logFileName string,
) (*logbuffer.LogBuffers, error) {
	return acquireLogBuffersHook(conductor, logFileName)
}

// RegisterPublicationForTest is for cross-package tests behind the testhooks tag.
//
// conductor_test uses InsertPublicationForTest in export_test.go instead.
func RegisterPublicationForTest(
	conductor *Conductor,
	registrationID int64,
	publication PublicationHandle,
) {
	registerPublicationHook(conductor, registrationID, publication)
}

// RemovePublicationForTest is for cross-package tests behind the testhooks tag.
//
// conductor_test uses (*Conductor).RemovePublicationForTest in export_test.go instead.
func RemovePublicationForTest(conductor *Conductor, registrationID int64) {
	removePublicationHook(conductor, registrationID)
}

// RemoveSubscriptionForTest is for cross-package tests behind the testhooks tag.
//
// conductor_test uses (*Conductor).RemoveSubscriptionForTest in export_test.go instead.
func RemoveSubscriptionForTest(conductor *Conductor, registrationID int64) {
	removeSubscriptionHook(conductor, registrationID)
}

// PublicationCountForTest is for cross-package tests behind the testhooks tag.
//
// conductor_test uses (*Conductor).PublicationCountForTest in export_test.go instead.
func PublicationCountForTest(conductor *Conductor) int {
	return publicationCountHook(conductor)
}

// SubscriptionCountForTest is for cross-package tests behind the testhooks tag.
//
// conductor_test uses (*Conductor).SubscriptionCountForTest in export_test.go instead.
func SubscriptionCountForTest(conductor *Conductor) int {
	return subscriptionCountHook(conductor)
}

func LogBuffersRegistryForTest(conductor *Conductor) *LogBuffersRegistry {
	return logBuffersRegistryHook(conductor)
}

func LogBuffersRegistryEntryCountForTest(registry *LogBuffersRegistry) int {
	return len(registry.entries) + len(registry.lingering)
}

func LogBuffersRegistryRefCountForTest(
	registry *LogBuffersRegistry,
	logFileName string,
) int32 {
	return logBuffersRegistryRefCountHook(registry, logFileName)
}

func LogBuffersRegistryContainsForTest(
	registry *LogBuffersRegistry,
	logFileName string,
) bool {
	return logBuffersRegistryContainsHook(registry, logFileName)
}

func LogBuffersRegistryLingerScheduledForTest(registry *LogBuffersRegistry) int {
	return logBuffersRegistryLingerScheduledHook(registry)
}

func LogBuffersRegistryCountScheduledLingersForTest(registry *LogBuffersRegistry) int {
	return logBuffersRegistryCountScheduledLingersHook(registry)
}

func LingerDeadlineNsForTest(registry *LogBuffersRegistry, logFileName string) int64 {
	return lingerDeadlineNsHook(registry, logFileName)
}

// SetLingerSweepBlockerForTest installs blocker on conductor.
// service runs blocker before the linger sweep, under the resource write lock.
//
// Lifetime: the blocker stays set for the rest of tb, including subtests.
func (conductor *Conductor) SetLingerSweepBlockerForTest(
	tb testing.TB,
	blocker func(),
) {
	tb.Helper()
	testhook.SetForTest(tb, lingerSweepBlockers, conductor, blocker)
}

// SetReceiveNextHookForTest installs hook on copyReceiver.
// pollAndCopy runs hook after ReceiveNext and before the lap check.
//
// Lifetime: the hook stays set for the rest of tb, including subtests.
func (copyReceiver *CopyBroadcastReceiver) SetReceiveNextHookForTest(
	tb testing.TB,
	hook func(),
) {
	tb.Helper()
	testhook.SetForTest(tb, receiveNextHooks, copyReceiver, hook)
}

// SetReceiveCopiedHookForTest installs hook on copyReceiver.
// pollAndCopy runs hook after the copy and before Validate.
//
// Lifetime: the hook stays set for the rest of tb, including subtests.
func (copyReceiver *CopyBroadcastReceiver) SetReceiveCopiedHookForTest(
	tb testing.TB,
	hook func(),
) {
	tb.Helper()
	testhook.SetForTest(tb, receiveCopiedHooks, copyReceiver, hook)
}

// SetReceiveValidatedHookForTest installs hook on copyReceiver.
// deliverCopied runs hook after Validate and before the handler.
//
// Lifetime: the hook stays set for the rest of tb, including subtests.
func (copyReceiver *CopyBroadcastReceiver) SetReceiveValidatedHookForTest(
	tb testing.TB,
	hook func(),
) {
	tb.Helper()
	testhook.SetForTest(tb, receiveValidatedHooks, copyReceiver, hook)
}

// ClearReceiveHooksForTest removes the three receive hooks from copyReceiver.
// The SetReceive*HookForTest cleanups then do nothing.
func (copyReceiver *CopyBroadcastReceiver) ClearReceiveHooksForTest() {
	receiveNextHooks.Delete(copyReceiver)
	receiveCopiedHooks.Delete(copyReceiver)
	receiveValidatedHooks.Delete(copyReceiver)
}

func (conductor *Conductor) SetCountersReaderForTest(reader *cnc.CountersReader) {
	setCountersReaderHook(conductor, reader)
}

// SetErrorHandlerForTest replaces the conductor ErrorHandler. Nil clears it.
func (conductor *Conductor) SetErrorHandlerForTest(handler func(error)) {
	setErrorHandlerHook(conductor, handler)
}

func (conductor *Conductor) SetHandlerScopeForTest(scope HandlerScope) {
	conductor.config.HandlerScope = scope
}

func (conductor *Conductor) EventsAdapterForTest() *DriverEventsAdapter {
	return eventsAdapterHook(conductor)
}

func (adapter *DriverEventsAdapter) CopyReceiverForTest() *CopyBroadcastReceiver {
	return copyReceiverHook(adapter)
}

// NewConductorUnchecked builds a conductor with synthetic driver I/O for tests.
func NewConductorUnchecked(
	config Config,
	countersReader *cnc.CountersReader,
) (*Conductor, error) {
	return newConductorUncheckedHook(config, countersReader)
}

func (conductor *Conductor) SetReceivedCorrelationIDForTest(correlationID int64) {
	setReceivedCorrelationIDHook(conductor, correlationID)
}

func (conductor *Conductor) CompletePendingForTest(
	correlationID int64,
	int32Value int32,
	err error,
) error {
	return completePendingHook(conductor, correlationID, int32Value, err)
}

// PublicationReadyForTest completes PendingKindAddPublication.
//
// Cross-package tests behind the testhooks tag. conductor_test uses
// (*Conductor).PublicationReadyForTest in export_test.go instead.
func PublicationReadyForTest(conductor *Conductor, event PublicationBuffersReady) {
	publicationReadyHook(conductor, event)
}

// ExclusivePublicationReadyForTest completes PendingKindAddExclusivePublication.
//
// Cross-package tests behind the testhooks tag. conductor_test uses
// (*Conductor).ExclusivePublicationReadyForTest in export_test.go instead.
func ExclusivePublicationReadyForTest(conductor *Conductor, event PublicationBuffersReady) {
	exclusivePublicationReadyHook(conductor, event)
}

// SubscriptionReadyForTest is for cross-package tests behind the testhooks tag.
//
// conductor_test uses (*Conductor).SubscriptionReadyForTest in export_test.go instead.
func SubscriptionReadyForTest(conductor *Conductor, event SubscriptionReady) {
	conductor.handleSubscriptionReady(event)
}
