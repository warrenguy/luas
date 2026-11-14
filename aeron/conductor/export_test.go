package conductor

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/atomicbuffer"
	"github.com/warrenguy/luas/broadcast"
	"github.com/warrenguy/luas/ringbuffer"
)

func (conductor *Conductor) DriverProxyForTest() *DriverProxy {
	return conductor.driverProxy
}

func (conductor *Conductor) CountersReaderForTest() *cnc.CountersReader {
	return conductor.countersReader
}

func (conductor *Conductor) SetHeartbeatCounterIDForTest(counterID int32) {
	conductor.heartbeatCounterID = counterID
}

func (conductor *Conductor) TimeOfLastKeepAliveNsForTest() int64 {
	return conductor.timeOfLastKeepAliveNs
}

func (conductor *Conductor) StoreClientHeartbeatForTest(nowNs, nowMs int64) int {
	return conductor.storeClientHeartbeat(nowNs, nowMs)
}

func (conductor *Conductor) SetTerminatingForTest(terminating bool) {
	conductor.terminating.Store(terminating)
}

func (proxy *DriverProxy) Ring() *ringbuffer.ManyToOneRingBuffer {
	return proxy.ring
}

func NewCopyBroadcastReceiverWithScratchLength(
	receiver *broadcast.BroadcastReceiver,
	scratchLength int,
) *CopyBroadcastReceiver {
	return newCopyBroadcastReceiver(receiver, scratchLength)
}

func (proxy *DriverProxy) OfferCorrelatedForTest(msgTypeID int) error {
	return proxy.offerCorrelated(msgTypeID, false)
}

func (proxy *DriverProxy) OfferCorrelatedSilentForTest(msgTypeID int) error {
	return proxy.offerCorrelated(msgTypeID, true)
}

func (proxy *DriverProxy) OfferCorrelatedBodyForTest(
	msgTypeID int,
	bodyLength int,
	correlationID int64,
	silent bool,
	afterEncode func(encodedOffset int) error,
) error {
	return proxy.offerCorrelatedBody(msgTypeID, bodyLength, correlationID, silent, afterEncode)
}

func (proxy *DriverProxy) OfferBodyForTest(
	msgTypeID int,
	bodyLength int,
	silent bool,
	encode func(encodedOffset int) error,
) error {
	return proxy.offerBody(msgTypeID, bodyLength, silent, encode)
}

func NewConductorForTest(
	tb testing.TB,
	config Config,
	countersReader *cnc.CountersReader,
) *Conductor {
	tb.Helper()

	conductor, err := NewConductorUnchecked(config, countersReader)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		conductor.underResourceWriteLock(conductor.logBuffersRegistry.stopUnmapper)
	})
	return conductor
}

func DecodeTrailingStringASCIIForTest(
	buf atomicbuffer.AtomicBuffer,
	offset, bodyLength, stringOffset int,
	wrapped error,
) (string, error) {
	return decodeTrailingStringASCII(buf, offset, bodyLength, stringOffset, wrapped)
}

func SetLogBuffersRegistryRefCountForTest(
	registry *LogBuffersRegistry,
	logFileName string,
	refCount int32,
) error {
	key, err := registry.resolveLogFileKey(logFileName)
	if err != nil {
		return err
	}
	entry := registry.entries[key]
	if entry == nil {
		return fmt.Errorf("conductor: log file %q not in registry", logFileName)
	}
	entry.refCount = refCount
	return nil
}

func ResolveLogFileKeyForTest(
	registry *LogBuffersRegistry,
	logFileName string,
) (string, error) {
	return registry.resolveLogFileKey(logFileName)
}

func ReleaseResolvedKeyForTest(
	registry *LogBuffersRegistry,
	key string,
	lingerNs int64,
) {
	registry.releaseResolvedKey(key, lingerNs)
}

func ReleaseResolvedKeyAtForTest(
	registry *LogBuffersRegistry,
	key string,
	nowNs int64,
	lingerNs int64,
) {
	registry.releaseResolvedKeyAt(key, nowNs, lingerNs)
}

func SweepLingeringLogBuffersForTest(registry *LogBuffersRegistry, nowNs int64) int {
	return registry.SweepLingering(nowNs)
}

func LogBuffersRegistryRefCountByResolvedKeyForTest(
	registry *LogBuffersRegistry,
	key string,
) int32 {
	if registry == nil {
		return 0
	}
	if entry := registry.entries[key]; entry != nil {
		return entry.refCount
	}
	return 0
}

func NewUnmappingLogBuffersRegistryForTest(mediaDriverDir string) *LogBuffersRegistry {
	return newUnmappingLogBuffersRegistry(mediaDriverDir)
}

func StopLogBuffersRegistryUnmapperForTest(registry *LogBuffersRegistry) {
	registry.stopUnmapper()
}

// CloseLogBuffersRegistryForTest unmaps every entry, live or lingering. It then
// stops the unmapper. Call it after the test stops reading the buffers.
func CloseLogBuffersRegistryForTest(registry *LogBuffersRegistry) {
	for key, entry := range registry.entries {
		delete(registry.entries, key)
		registry.unmap(entry.buffers)
	}
	registry.SweepLingering(math.MaxInt64)
	registry.stopUnmapper()
}

// CloseLogBuffersForTest runs CloseLogBuffersRegistryForTest on the conductor
// registry under the resource write lock.
func (conductor *Conductor) CloseLogBuffersForTest() {
	conductor.underResourceWriteLock(func() {
		CloseLogBuffersRegistryForTest(conductor.logBuffersRegistry)
	})
}

func IsLogBuffersRegistryUnmapperRunningForTest(registry *LogBuffersRegistry) bool {
	return registry.unmapper != nil && registry.unmapper.queue != nil
}

func SeedLogBuffersRegistryForTest(
	registry *LogBuffersRegistry,
	logFileName string,
	buffers *logbuffer.LogBuffers,
) error {
	if registry == nil {
		return errors.New("conductor: log buffers registry is nil")
	}
	if logFileName == "" {
		return errors.New("conductor: log file name is empty")
	}
	if buffers == nil {
		return errors.New("conductor: log buffers is nil")
	}
	key, err := registry.resolveLogFileKey(logFileName)
	if err != nil {
		return err
	}
	if registry.entries[key] != nil {
		return fmt.Errorf("conductor: log file %q already registered", logFileName)
	}
	registry.entries[key] = &logBufferEntry{
		buffers:  buffers,
		refCount: 1,
	}
	return nil
}

// ForceCloseResourcesForTest retires every publication and subscription.
// Does not offer REMOVE. Production terminate uses terminateWithError.
func (conductor *Conductor) ForceCloseResourcesForTest() {
	conductor.forceCloseResources()
}

// ForceClosePublicationForTest force-closes one publication. Does not offer
// REMOVE.
func (conductor *Conductor) ForceClosePublicationForTest(publication PublicationHandle) {
	conductor.retirePublication(publication, conductor.config.ResourceLingerNs)
}

// ForceCloseSubscriptionForTest force-closes one subscription. Does not offer
// REMOVE.
func (conductor *Conductor) ForceCloseSubscriptionForTest(subscription SubscriptionHandle) {
	conductor.retireSubscription(subscription, conductor.config.ResourceLingerNs)
}

// InsertPublicationForTest is for conductor_test.
//
// Cross-package callers use RegisterPublicationForTest behind the testhooks build tag.
func (conductor *Conductor) InsertPublicationForTest(
	registrationID int64,
	publication PublicationHandle,
) {
	conductor.underResourceWriteLock(func() {
		conductor.insertPublication(registrationID, publication)
	})
}

func (conductor *Conductor) InsertSubscriptionForTest(
	registrationID int64,
	subscription SubscriptionHandle,
) {
	conductor.underResourceWriteLock(func() {
		conductor.insertSubscription(registrationID, subscription)
	})
}

func (conductor *Conductor) PreInsertSubscriptionForTest(
	correlationID int64,
	streamID int32,
	channel string,
) error {
	return conductor.preInsertSubscription(correlationID, streamID, channel)
}

func (conductor *Conductor) RegisterPendingEntryForTest(
	registration PendingRegistration,
	wantAdd bool,
) error {
	return conductor.registerPendingEntry(registration, wantAdd)
}

func AddPendingKindForTest(kind PendingKind) bool {
	return addPendingKind(kind)
}

func AddPublicationPendingKindForTest(kind PendingKind) bool {
	return addPublicationPendingKind(kind)
}

func (conductor *Conductor) RemovePublicationForTest(registrationID int64) {
	conductor.underResourceWriteLock(func() {
		conductor.removePublication(registrationID)
	})
}

func (conductor *Conductor) RemoveSubscriptionForTest(registrationID int64) {
	conductor.underResourceWriteLock(func() {
		conductor.removeSubscription(registrationID)
	})
}

func (conductor *Conductor) PublicationCountForTest() int {
	return withResourceRead(conductor, conductor.publicationCount)
}

func (conductor *Conductor) SubscriptionCountForTest() int {
	return withResourceRead(conductor, conductor.subscriptionCount)
}

func (conductor *Conductor) ResourceWriteLockDepthForTest() int32 {
	return conductor.resourceWriteLockDepth
}

// TryResourceRLockForTest reports whether resourceMu can be read-locked without
// blocking. True means no write holder. Unlocks immediately on success.
//
// Concurrent unit tests call this instead of wall-clock waits. A false result
// asserts a write lock during linger sweep or other mutation scopes.
func (conductor *Conductor) TryResourceRLockForTest() bool {
	if !conductor.resourceMu.TryRLock() {
		return false
	}
	conductor.resourceMu.RUnlock()
	return true
}

func (conductor *Conductor) InsertNilPublicationForTest(registrationID int64) {
	conductor.InsertPublicationForTest(registrationID, nil)
}

func (conductor *Conductor) InsertNilSubscriptionForTest(registrationID int64) {
	conductor.InsertSubscriptionForTest(registrationID, nil)
}

func (conductor *Conductor) AcquireLogBuffersForTest(
	logFileName string,
) (*logbuffer.LogBuffers, error) {
	var buffers *logbuffer.LogBuffers
	var err error
	conductor.underResourceWriteLock(func() {
		buffers, err = conductor.acquireLogBuffers(logFileName)
	})
	return buffers, err
}

func (conductor *Conductor) ReleaseLogBuffersForTest(logFileName string, lingerNs int64) {
	conductor.underResourceWriteLock(func() {
		conductor.releaseLogBuffers(logFileName, lingerNs)
	})
}

func (conductor *Conductor) SweepLingeringLogBuffersAtForTest(nowNs int64) int {
	return underResourceWriteLockResult(conductor, func() int {
		return conductor.sweepLingeringLogBuffers(nowNs)
	})
}

func ValidateLogFileNameForTest(logFileName, mediaDriverDir string) error {
	return validateLogFileName(logFileName, mediaDriverDir)
}

// CorrelatedForTest reports whether correlationID passes the async ready peek gate.
//
// A true result means correlationID equals activeCorrelationID (including NullCorrelationID)
// or the adapter pending registry contains correlationID.
func (adapter *DriverEventsAdapter) CorrelatedForTest(
	activeCorrelationID int64,
	correlationID int64,
) bool {
	return adapter.correlated(activeCorrelationID, correlationID)
}

// OperationSuccessCorrelatedForTest reports whether ON_OPERATION_SUCCESS for
// correlationID passes the adapter check. It adds no unhandled count.
func (adapter *DriverEventsAdapter) OperationSuccessCorrelatedForTest(
	activeCorrelationID int64,
	correlationID int64,
) bool {
	return adapter.isOperationSuccessCorrelated(activeCorrelationID, correlationID)
}

// PublicationReadyForTest completes PendingKindAddPublication.
func (conductor *Conductor) PublicationReadyForTest(event PublicationBuffersReady) {
	publicationReadyHook(conductor, event)
}

// ExclusivePublicationReadyForTest completes PendingKindAddExclusivePublication.
func (conductor *Conductor) ExclusivePublicationReadyForTest(event PublicationBuffersReady) {
	exclusivePublicationReadyHook(conductor, event)
}

func (conductor *Conductor) SubscriptionReadyForTest(event SubscriptionReady) {
	conductor.handleSubscriptionReady(event)
}

func (conductor *Conductor) RollbackAddPendingResourcesForTest(
	registration PendingRegistration,
) {
	conductor.RollbackAddPendingResources(registration)
}

func (conductor *Conductor) AvailableImageForTest(event AvailableImage) {
	conductor.handleAvailableImage(event)
}

func (conductor *Conductor) InvokeImageHandlerForTest(handler func()) {
	conductor.invokeImageHandler(handler)
}

func NewSyntheticLogBuffersForTest(tb testing.TB) *logbuffer.LogBuffers {
	tb.Helper()
	return newSyntheticLogBuffersForTest(tb, logbuffer.LogBufferTypeConcurrentPublication)
}

func NewSyntheticExclusiveLogBuffersForTest(tb testing.TB) *logbuffer.LogBuffers {
	tb.Helper()
	return newSyntheticLogBuffersForTest(tb, logbuffer.LogBufferTypeExclusivePublication)
}

func newSyntheticLogBuffersForTest(tb testing.TB, logType byte) *logbuffer.LogBuffers {
	const (
		termLength    = 65536
		pageSize      = 4096
		initialTermID = int32(7)
	)
	computed, err := logbuffer.ComputeLogLength(termLength, pageSize)
	if err != nil {
		tb.Fatal(err)
	}

	buf := make([]byte, computed)
	metaStart := logbuffer.LogMetaDataOffset(computed)
	meta := atomicbuffer.Wrap(buf[metaStart : metaStart+logbuffer.MetaDataLength])
	rawTails := [logbuffer.PartitionCount]int64{
		logbuffer.PackRawTail(initialTermID, 0),
		logbuffer.PackRawTail(initialTermID-2, 0),
		logbuffer.PackRawTail(initialTermID-1, 0),
	}
	for partition := range logbuffer.PartitionCount {
		_ = logbuffer.PutRawTail(meta, partition, rawTails[partition])
	}
	meta.PutInt32Release(logbuffer.OffsetActiveTermCount, 0)
	meta.PutInt32Release(logbuffer.OffsetTermLength, termLength)
	meta.PutInt32(logbuffer.OffsetPageSize, pageSize)
	meta.PutInt32(logbuffer.OffsetMTULength, 1408)
	meta.PutInt32(logbuffer.OffsetInitialTermID, initialTermID)
	meta.PutInt32(logbuffer.OffsetDefaultFrameHeaderLength, logbuffer.DataHeaderLength)
	meta.Bytes()[logbuffer.OffsetLogType] = logType
	if err := logbuffer.WriteDataHeaderUnfragmented(
		meta,
		logbuffer.OffsetDefaultFrameHeader,
		1001,
		2002,
		initialTermID,
		0,
		logbuffer.Unfragmented,
	); err != nil {
		tb.Fatal(err)
	}
	return newSyntheticLogBuffersFromMeta(tb, buf)
}

func newSyntheticLogBuffersFromMeta(
	tb testing.TB,
	buf []byte,
) *logbuffer.LogBuffers {
	mapped, err := logbuffer.MapLogBufferFromBytes(buf)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = mapped.Close() })

	logBuffers, err := logbuffer.NewLogBuffers(mapped)
	if err != nil {
		tb.Fatal(err)
	}
	return logBuffers
}
