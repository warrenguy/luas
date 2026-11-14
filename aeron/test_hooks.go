//go:build testhooks

package aeron

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/conductor"
	"github.com/warrenguy/luas/aeron/internal/testutil"
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/atomicbuffer"
	"github.com/warrenguy/luas/record"
)

// HeaderSetFragmentedFrameLenForTest seeds fragmentedFrameLen on a poll flyweight.
func HeaderSetFragmentedFrameLenForTest(header *Header, fragmentedFrameLen int32) {
	header.fragmentedFrameLen = fragmentedFrameLen
}

// HeaderFragmentedFrameLenForTest returns fragmentedFrameLen on a completed header.
func HeaderFragmentedFrameLenForTest(header *Header) int32 {
	return header.fragmentedFrameLen
}

// Cross-package test hooks. Publication-related IDs align with in-mem Test* fixtures;
// subscription/image/counter IDs use separate namespace for cross-package hook tests
// (not full mirror of export_test). Helpers live here because _test.go is not visible
// to external test packages.
const (
	testHookIPCChannel = "aeron:ipc"

	testHookSubscriptionRegistrationID int64 = 9002
	testHookSubscriptionStreamID       int32 = 2003

	testHookPublicationRegistrationID int64 = 9001
	testHookPublicationLimitCounterID int32 = 1
	testHookPublicationSessionID      int32 = 1001
	testHookPublicationStreamID       int32 = 2002
	testHookPublicationInitialTermID  int32 = 7
	testHookPublicationMTULength      int32 = 1408
	testHookPublicationLimitPosition  int64 = 1 << 40

	testHookMappedLogFileName = "test-hook.log"

	testHookImageCorrelationID          int64 = 9102
	testHookImageSessionID              int32 = 1002
	testHookSubscriberPositionCounterID int32 = 4

	// TestHookSubscriptionRegistrationID is the registration id for cross-package subscription hooks.
	TestHookSubscriptionRegistrationID = testHookSubscriptionRegistrationID
	// TestHookSubscriptionStreamID is the stream id for cross-package subscription hooks.
	TestHookSubscriptionStreamID = testHookSubscriptionStreamID
)

// NewTestSubscriptionForHooks builds a subscription for cross-package close tests.
func NewTestSubscriptionForHooks(tb testing.TB) *Subscription {
	tb.Helper()

	return NewSubscriptionForHooks(
		testHookSubscriptionRegistrationID,
		testHookSubscriptionStreamID,
		testHookIPCChannel,
	)
}

// NewSubscriptionForHooks builds a subscription with caller-supplied channel URI.
func NewSubscriptionForHooks(
	registrationID int64,
	streamID int32,
	channel string,
) *Subscription {
	return newSubscription(registrationID, streamID, channel, nil, nil)
}

// RecordingReceiveDestinationHook records AddDestination and RemoveDestination
// calls on a [Subscription] or [ExclusivePublication] for unit tests that do
// not drive a media driver.
type RecordingReceiveDestinationHook struct {
	Adds      []string
	Removes   []string
	AddErr    error
	RemoveErr error

	// FailNextAddErr fails the next AddDestination, then clears. That call
	// does not record its channel: the destination did not attach.
	FailNextAddErr error
}

// AddDestination records channel and returns AddErr.
//
// FailNextAddErr, when set, fails once without recording, then clears.
func (h *RecordingReceiveDestinationHook) AddDestination(channel string) error {
	if h == nil {
		return nil
	}
	if h.FailNextAddErr != nil {
		err := h.FailNextAddErr
		h.FailNextAddErr = nil
		return err
	}
	h.Adds = append(h.Adds, channel)
	return h.AddErr
}

// RemoveDestination records channel and returns RemoveErr.
func (h *RecordingReceiveDestinationHook) RemoveDestination(channel string) error {
	if h == nil {
		return nil
	}
	h.Removes = append(h.Removes, channel)
	return h.RemoveErr
}

// SubscriptionSetReceiveDestinationHookForHooks installs a destination-command
// override on sub. Nil clears the override. The production path is used when
// the override is unset.
func SubscriptionSetReceiveDestinationHookForHooks(
	sub *Subscription,
	hook receiveDestinationHook,
) {
	if sub == nil {
		return
	}
	sub.receiveDestinationHook = hook
}

var _ sendDestinationHook = (*RecordingReceiveDestinationHook)(nil)

// ExclusivePublicationSetSendDestinationHookForHooks installs a send
// destination override on ep. Nil clears the override.
func ExclusivePublicationSetSendDestinationHookForHooks(
	ep *ExclusivePublication,
	hook sendDestinationHook,
) {
	if ep == nil {
		return
	}
	ep.sendDestinationHook = hook
}

// SubscriptionApplyReadyForHooks applies SubscriptionReady channel-status counter wiring.
func SubscriptionApplyReadyForHooks(sub *Subscription, channelStatusCounterID int32) {
	sub.applySubscriptionReady(channelStatusCounterID)
}

// SubscriptionSetCountersForHooks binds a counters reader for channel-status reads.
func SubscriptionSetCountersForHooks(sub *Subscription, counters *cnc.CountersReader) {
	sub.counters = counters
}

// SubscriptionCountersForHooks returns the counters reader on sub.
func SubscriptionCountersForHooks(sub *Subscription) *cnc.CountersReader {
	if sub == nil {
		return nil
	}
	return sub.counters
}

// SubscriptionSetReceiveChannelStatusForHooks seeds the receive
// channel-status counter value.
func SubscriptionSetReceiveChannelStatusForHooks(sub *Subscription, value int64) {
	if sub == nil || sub.counters == nil {
		return
	}
	_ = sub.counters.SetCounterValue(sub.channelStatusCounterID.Load(), value)
}

// NewWildcardUDPReceiveCountersForHooks builds receive channel-status and optional
// LOCAL_SOCKADDR counters for UDP endpoint=:0 wildcard resolution tests.
func NewWildcardUDPReceiveCountersForHooks(
	tb testing.TB,
	channel string,
	channelStatusCounterID int32,
	bindAddress string,
) *cnc.CountersReader {
	tb.Helper()
	require.Equal(tb, int32(0), channelStatusCounterID)

	meta := make([]byte, cnc.CounterMetadataLength*2)
	values := make([]byte, cnc.CounterValueLength*2)
	require.NoError(tb, cnc.WriteChannelStatusCounterRecord(
		meta,
		values,
		0,
		cnc.CounterRecordAllocated,
		cnc.DriverReceiveChannelStatusTypeID,
		channel,
		9001,
		cnc.ChannelEndpointStatusActive,
	))
	if bindAddress != "" {
		require.NoError(tb, cnc.WriteLocalSocketAddressCounterRecord(
			meta,
			values,
			1,
			cnc.CounterRecordAllocated,
			9001,
			channelStatusCounterID,
			bindAddress,
			cnc.ChannelEndpointStatusActive,
		))
	}

	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(tb, err)
	return reader
}

// WireSubscriptionUDPWildcardForHooks applies subscription-ready wiring and counters
// for UDP wildcard receive resolution tests.
func WireSubscriptionUDPWildcardForHooks(
	sub *Subscription,
	counters *cnc.CountersReader,
	receiveChannelActive bool,
) {
	SubscriptionApplyReadyForHooks(sub, 0)
	SubscriptionSetCountersForHooks(sub, counters)
	if receiveChannelActive {
		SubscriptionSetReceiveChannelStatusForHooks(sub, cnc.ChannelEndpointStatusActive)
	}
}

// NewFileBackedPublicationForHooks builds a file-backed publication for linger close tests.
func NewFileBackedPublicationForHooks(tb testing.TB, logPath string) *Publication {
	tb.Helper()

	writeTestHookLogFile(tb, logPath)

	log, err := logbuffer.MapLogFile(logPath)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = log.Close() })

	pub, err := newPublicationFromReady(
		testHookPublicationBuffersReady(logPath),
		log,
		newTestHookCountersReader(tb),
		testHookIPCChannel,
	)
	require.NoError(tb, err)
	return pub
}

// NewTestExclusivePublicationForHooks builds a connected exclusive publication
// for cross-package connect and ingress tests.
func NewTestExclusivePublicationForHooks(tb testing.TB) *ExclusivePublication {
	tb.Helper()

	return NewTestExclusivePublicationForHooksWithConnected(tb, true)
}

// NewTestExclusivePublicationForHooksWithConnected builds an exclusive publication
// with the log metadata is-connected flag set for connect-path tests.
//
// Uses the default high publication-limit counter so Offer succeeds at position 0.
func NewTestExclusivePublicationForHooksWithConnected(
	tb testing.TB,
	connected bool,
) *ExclusivePublication {
	tb.Helper()

	return NewTestExclusivePublicationForHooksWithLimit(
		tb,
		testHookPublicationLimitPosition,
		connected,
	)
}

// NewTestExclusivePublicationForHooksWithLimit builds an exclusive publication
// with caller-chosen publication-limit counter value and is-connected flag.
//
// Cross-package consensus tests use limit 0 to hit the publication limit
// check at position 0 (retriable NotConnected or BackPressured).
func NewTestExclusivePublicationForHooksWithLimit(
	tb testing.TB,
	limitPosition int64,
	connected bool,
) *ExclusivePublication {
	tb.Helper()

	log := newTestHookMappedLogWithConnected(
		tb,
		logbuffer.LogBufferTypeExclusivePublication,
		connected,
	)
	pub, err := newExclusivePublicationFromReady(
		testHookPublicationBuffersReady(testHookMappedLogFileName),
		log,
		newTestHookCountersReaderWithLimit(tb, limitPosition),
		testHookIPCChannel,
	)
	require.NoError(tb, err)
	return pub
}

// MaxMessageLengthForTest returns maxMessageLength for hook-built exclusive publications.
func (ep *ExclusivePublication) MaxMessageLengthForTest() int {
	if ep == nil {
		return 0
	}
	return ep.maxMessageLength
}

// NewTestPublicationForHooks builds a disconnected concurrent publication for
// cross-package close tests. Connect-path tests that require IsConnected() ==
// true must use NewTestPublicationForHooksWithConnected(tb, true).
func NewTestPublicationForHooks(tb testing.TB) *Publication {
	tb.Helper()

	return NewTestPublicationForHooksWithConnected(tb, false)
}

// NewTestPublicationForHooksWithConnected builds a concurrent publication with
// the log metadata is-connected flag set for connect-path tests.
//
// Uses the default high publication-limit counter so Offer succeeds at
// position 0.
func NewTestPublicationForHooksWithConnected(tb testing.TB, connected bool) *Publication {
	tb.Helper()

	return NewTestPublicationForHooksWithLimit(
		tb,
		testHookPublicationLimitPosition,
		connected,
	)
}

// NewTestPublicationForHooksWithLimit builds a concurrent publication with a
// caller-chosen publication-limit counter value and is-connected flag.
//
// Cross-package tests use limit 0. The publication limit check at position 0
// then returns retriable NotConnected or BackPressured.
func NewTestPublicationForHooksWithLimit(
	tb testing.TB,
	limitPosition int64,
	connected bool,
) *Publication {
	tb.Helper()

	log := newTestHookMappedLogWithConnected(
		tb,
		logbuffer.LogBufferTypeConcurrentPublication,
		connected,
	)
	pub, err := newPublicationFromReady(
		testHookPublicationBuffersReady(testHookMappedLogFileName),
		log,
		newTestHookCountersReaderWithLimit(tb, limitPosition),
		testHookIPCChannel,
	)
	require.NoError(tb, err)
	return pub
}

func testHookPublicationBuffersReady(logFileName string) conductor.PublicationBuffersReady {
	return conductor.PublicationBuffersReady{
		RegistrationID:            testHookPublicationRegistrationID,
		SessionID:                 testHookPublicationSessionID,
		StreamID:                  testHookPublicationStreamID,
		PublicationLimitCounterID: testHookPublicationLimitCounterID,
		ChannelStatusCounterID:    cnc.NullCounterID,
		LogFileName:               logFileName,
	}
}

func allocTestHookLogBuffer(tb testing.TB) ([]byte, atomicbuffer.AtomicBuffer) {
	tb.Helper()

	computed, err := logbuffer.ComputeLogLength(logbuffer.TermMinLength, logbuffer.PageMinSize)
	require.NoError(tb, err)

	buf := make([]byte, computed)
	metaStart := logbuffer.LogMetaDataOffset(computed)
	return buf, atomicbuffer.Wrap(buf[metaStart : metaStart+logbuffer.MetaDataLength])
}

func initTestHookLogMetadata(meta atomicbuffer.AtomicBuffer) {
	meta.PutInt32Release(logbuffer.OffsetTermLength, logbuffer.TermMinLength)
	meta.PutInt32(logbuffer.OffsetPageSize, logbuffer.PageMinSize)
	meta.PutInt32(logbuffer.OffsetMTULength, testHookPublicationMTULength)
	meta.PutInt32(logbuffer.OffsetInitialTermID, testHookPublicationInitialTermID)
	meta.PutInt32(logbuffer.OffsetDefaultFrameHeaderLength, logbuffer.DataHeaderLength)
	meta.PutInt64Release(logbuffer.OffsetEndOfStreamPosition, math.MaxInt64)

	// Default frame header template feeds the publication HeaderWriter; without
	// it committed claims carry frame type 0 (PAD) instead of HdrTypeData.
	// Mirrors writeInMemPublicationMetadata.
	if err := logbuffer.WriteDataHeaderUnfragmented(
		meta,
		logbuffer.OffsetDefaultFrameHeader,
		testHookPublicationSessionID,
		testHookPublicationStreamID,
		testHookPublicationInitialTermID,
		0,
		logbuffer.Unfragmented,
	); err != nil {
		panic(err)
	}
}

func seedTestHookPartitionRawTails(meta atomicbuffer.AtomicBuffer) {
	rawTails := [logbuffer.PartitionCount]int64{
		logbuffer.PackRawTail(testHookPublicationInitialTermID, 0),
		logbuffer.PackRawTail(testHookPublicationInitialTermID-2, 0),
		logbuffer.PackRawTail(testHookPublicationInitialTermID-1, 0),
	}
	for partition := range logbuffer.PartitionCount {
		_ = logbuffer.PutRawTail(meta, partition, rawTails[partition])
	}
	meta.PutInt32Release(logbuffer.OffsetActiveTermCount, 0)
}

func prepareTestHookLogBuffer(tb testing.TB, logType byte) []byte {
	tb.Helper()

	return prepareTestHookLogBufferWithConnected(tb, logType, false)
}

func testHookConnectedInt32(connected bool) int32 {
	if connected {
		return 1
	}
	return 0
}

func prepareTestHookLogBufferWithConnected(tb testing.TB, logType byte, connected bool) []byte {
	tb.Helper()

	buf, meta := allocTestHookLogBuffer(tb)
	initTestHookLogMetadata(meta)
	seedTestHookPartitionRawTails(meta)
	meta.Bytes()[logbuffer.OffsetLogType] = logType
	meta.PutInt32Release(logbuffer.OffsetIsConnected, testHookConnectedInt32(connected))
	return buf
}

func writeTestHookLogFile(tb testing.TB, path string) {
	tb.Helper()

	buf := prepareTestHookLogBuffer(tb, logbuffer.LogBufferTypeConcurrentPublication)
	require.NoError(tb, os.WriteFile(path, buf, 0o644))
}

func newTestHookMappedLog(tb testing.TB, logType byte) *logbuffer.MappedLogBuffer {
	tb.Helper()

	return newTestHookMappedLogWithConnected(tb, logType, false)
}

func newTestHookMappedLogWithConnected(
	tb testing.TB,
	logType byte,
	connected bool,
) *logbuffer.MappedLogBuffer {
	tb.Helper()

	buf := prepareTestHookLogBufferWithConnected(tb, logType, connected)

	log, err := logbuffer.MapLogBufferFromBytes(buf)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = log.Close() })
	return log
}

// NewTestImageForHooks builds an in-memory image wired to sub for cross-package poll tests.
// Caller must register the image via SubscriptionAddImageForHooks before Subscription.Poll.
func NewTestImageForHooks(tb testing.TB, sub *Subscription, joinPosition int64) *Image {
	tb.Helper()

	return NewTestImageForSubscriptionHooks(tb, sub, joinPosition)
}

// NewTestImageForSubscriptionHooks builds an image whose registration id matches sub.
func NewTestImageForSubscriptionHooks(tb testing.TB, sub *Subscription, joinPosition int64) *Image {
	tb.Helper()

	return NewTestImageForSubscriptionHooksWithCorrelation(
		tb,
		sub,
		joinPosition,
		testHookImageCorrelationID,
	)
}

// NewTestImageForSubscriptionHooksWithCorrelation builds an image with an explicit correlation id.
func NewTestImageForSubscriptionHooksWithCorrelation(
	tb testing.TB,
	sub *Subscription,
	joinPosition int64,
	correlationID int64,
) *Image {
	tb.Helper()

	return NewTestImageForSubscriptionHooksWithSessionID(
		tb,
		sub,
		testHookImageSessionID,
		correlationID,
		joinPosition,
	)
}

// NewTestImageForSubscriptionHooksWithSessionID builds an image with explicit session and
// correlation ids wired to sub.
func NewTestImageForSubscriptionHooksWithSessionID(
	tb testing.TB,
	sub *Subscription,
	sessionID int32,
	correlationID int64,
	joinPosition int64,
) *Image {
	tb.Helper()

	log := newTestHookMappedLog(tb, logbuffer.LogBufferTypeConcurrentPublication)
	logBuffers, err := logbuffer.NewLogBuffers(log)
	require.NoError(tb, err)

	event := testHookAvailableImage(correlationID)
	event.SessionID = sessionID
	if sub != nil {
		event.SubscriptionRegistrationID = sub.RegistrationID()
		event.StreamID = sub.StreamID()
	}
	image, err := newImage(
		sub,
		event,
		logBuffers,
		newTestHookCountersReaderWithSubscriberRegistration(
			tb,
			event.SubscriptionRegistrationID,
			joinPosition,
		),
		nil,
	)
	require.NoError(tb, err)
	return image
}

// SubscriptionInjectNilImageAtIndexForHooks stores a new image slice with a
// nil slot at index. index may equal the current length. An out-of-range
// index or a nil sub does nothing.
func SubscriptionInjectNilImageAtIndexForHooks(sub *Subscription, index int) {
	if sub == nil {
		return
	}

	current := sub.imagesSnapshot()
	if index < 0 || index > len(current) {
		return
	}

	next := slices.Insert(slices.Clone(current), index, nil)
	sub.images.Store(&next)
}

// NewImageWithCommittedDataFrameForHooks returns an image with one committed data frame.
func NewImageWithCommittedDataFrameForHooks(
	tb testing.TB,
	joinPosition int64,
	frameOffset int,
) (*Image, int32) {
	tb.Helper()

	sub := NewSubscriptionForHooks(
		testHookSubscriptionRegistrationID,
		testHookSubscriptionStreamID,
		testHookIPCChannel,
	)
	image := NewTestImageForSubscriptionHooks(tb, sub, joinPosition)
	termBuffer := ImageTermBufferForHooks(image)
	payload := []byte("payload")
	WriteCommittedDataFrameForHooks(
		termBuffer,
		frameOffset,
		image.SessionID(),
		image.StreamID(),
		testHookPublicationInitialTermID,
		int32(frameOffset),
		logbuffer.Unfragmented,
		payload,
	)
	return image, int32(len(payload) + logbuffer.DataHeaderLength)
}

// SubscriptionAddImageForHooks registers image on sub. False when image is nil
// or correlation ID is already registered.
func SubscriptionAddImageForHooks(sub *Subscription, image *Image) bool {
	return sub.addImage(image)
}

// SubscriptionUnavailableImageHandlerForHooks returns the unavailable-image
// handler set on sub. Returns nil when sub is nil or no handler is set.
func SubscriptionUnavailableImageHandlerForHooks(sub *Subscription) UnavailableImageHandler {
	if sub == nil {
		return nil
	}
	return sub.loadUnavailableImageHandler()
}

// ImageTermBufferForHooks returns the term buffer at partition 0.
func ImageTermBufferForHooks(image *Image) atomicbuffer.AtomicBuffer {
	return ImageTermBufferAtForHooks(image, 0)
}

// ImageTermBufferAtForHooks returns the term buffer at partition index.
func ImageTermBufferAtForHooks(image *Image, index int) atomicbuffer.AtomicBuffer {
	return image.termBuffers[index]
}

// WriteCommittedPaddingFrameForHooks commits a PAD frame at frameOffset.
// The committed frame length is paddingLength. Panics when the header
// write fails.
func WriteCommittedPaddingFrameForHooks(
	termBuffer atomicbuffer.AtomicBuffer,
	frameOffset int,
	termID, termOffset, paddingLength int32,
) {
	if err := logbuffer.WritePaddingFrame(termBuffer, frameOffset, termID, termOffset); err != nil {
		panic(err)
	}
	logbuffer.PutFrameLengthOrdered(termBuffer, frameOffset, paddingLength)
}

// FragmentAssemblerHasBuilderForTest reports whether assembler has a builder for sessionID.
func FragmentAssemblerHasBuilderForTest(assembler *FragmentAssembler, sessionID int32) bool {
	return assembler != nil && assembler.core.builders[sessionID] != nil
}

// ImageInternalCloseForHooks closes the image handle for cross-package tests.
//
// Same as in-package ImageInternalCloseForTest: internalClose only. Does not
// remove the image from subscription.images; conductor unavailable-image
// teardown splices via removeImage before internalClose. Intended for cluster
// tests that retire the connect-time egress handle and assert via
// AeronCluster.egressImage, not subscription membership.
func ImageInternalCloseForHooks(image *Image) {
	if image == nil {
		return
	}
	image.internalClose()
}

// ImageSetSubscriberPositionForHooks rewinds the subscriber position counter
// so repeated poll zero-alloc iterations re-dispatch the same committed frame.
func ImageSetSubscriberPositionForHooks(image *Image, position int64) {
	if image == nil {
		return
	}
	setInMemImageSubscriberPosition(image, position)
}

// ImageSetEndOfStreamPositionForHooks writes the end-of-stream word in the
// image metadata. Call it before image close. Close snapshots the word. A
// later write does not change [Image.IsEndOfStream] or
// [Image.EndOfStreamPosition]. A write after unmap faults.
func ImageSetEndOfStreamPositionForHooks(image *Image, position int64) {
	if image == nil {
		return
	}
	setInMemImageEndOfStreamPosition(image, position)
}

// WriteCommittedDataFrameForHooks commits a DATA frame at frameOffset. Unfragmented
// flags use WriteDataHeaderUnfragmented; other flags use WriteDataHeaderFragmented.
func WriteCommittedDataFrameForHooks(
	termBuffer atomicbuffer.AtomicBuffer,
	frameOffset int,
	sessionID int32,
	streamID int32,
	termID int32,
	termOffset int32,
	flags uint8,
	payload []byte,
) {
	writeCommittedDataFrameForTest(
		termBuffer,
		frameOffset,
		sessionID,
		streamID,
		termID,
		termOffset,
		flags,
		payload,
	)
}

func testHookAvailableImage(correlationID int64) conductor.AvailableImage {
	return conductor.AvailableImage{
		CorrelationID:              correlationID,
		SessionID:                  testHookImageSessionID,
		StreamID:                   testHookSubscriptionStreamID,
		SubscriptionRegistrationID: testHookSubscriptionRegistrationID,
		SubscriberPositionID:       testHookSubscriberPositionCounterID,
		LogFileName:                testHookMappedLogFileName,
		SourceIdentity:             testHookIPCChannel,
	}
}

func newTestHookCountersReaderFromRecords(
	tb testing.TB,
	counterCount int,
	writeRecords func(meta, values []byte),
) *cnc.CountersReader {
	tb.Helper()

	meta := make([]byte, cnc.CounterMetadataLength*counterCount)
	values := make([]byte, cnc.CounterValueLength*counterCount)
	writeRecords(meta, values)

	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(tb, err)
	return reader
}

func newTestHookCountersReader(tb testing.TB) *cnc.CountersReader {
	tb.Helper()

	return newTestHookCountersReaderWithLimit(tb, testHookPublicationLimitPosition)
}

func newTestHookCountersReaderWithLimit(
	tb testing.TB,
	limitPosition int64,
) *cnc.CountersReader {
	tb.Helper()

	return newTestHookCountersReaderFromRecords(
		tb,
		int(testHookPublicationLimitCounterID)+1,
		func(meta, values []byte) {
			cnc.WriteCounterRecord(
				meta,
				values,
				testHookPublicationLimitCounterID,
				cnc.CounterRecordAllocated,
				cnc.DriverPublisherLimitTypeID,
				testHookPublicationRegistrationID,
				limitPosition,
			)
		},
	)
}

// NewTestClientFromConductorFixture builds a transport client wired to a synthetic
// conductor fixture for cross-package connect handshake tests.
func NewTestClientFromConductorFixture(
	tb testing.TB,
	fixture *testutil.ConductorFixture,
) *Client {
	tb.Helper()

	return &Client{
		clientID:                testutil.TestClientID,
		conductor:               fixture.Conductor,
		terminationConductor:    fixture.Conductor,
		driverProxy:             fixture.DriverProxy,
		nanoClock:               fixture.NanoClock,
		driverTimeout:           testutil.TestDriverTimeout,
		pollTimeout:             defaultPollTimeout,
		publicationOfferTimeout: defaultPollTimeout,
		idleSleepDuration:       testutil.TestIdleSleepDuration,
	}
}

const (
	// AsyncConnectTestIngressRegID is the synthetic ingress publication registration id
	// used by AsyncConnectTestHarness.
	AsyncConnectTestIngressRegID int64 = 3001

	// AsyncConnectTestSessionID is the default session id AsyncConnectTestHarness
	// returns to a next session id request.
	AsyncConnectTestSessionID int32 = 5003

	asyncConnectTestIngressRegID = AsyncConnectTestIngressRegID
)

// AsyncConnectTestHarness wires a synthetic conductor fixture for cluster connect tests.
type AsyncConnectTestHarness struct {
	Client  *Client
	LogPath string

	// ConcurrentLogPath backs non-exclusive AddPublication responses.
	ConcurrentLogPath string

	// MediaDriverDir is the synthetic driver directory for member log paths.
	MediaDriverDir string

	fixture *testutil.ConductorFixture

	// registrationIDs holds the publication registration ids behind the
	// conductor counters reader.
	registrationIDs []int64

	// imageLogCount counts the image log files TransmitAvailableImage wrote.
	imageLogCount int

	nextSessionID int32

	// nextSessionIDErr fails each next session id request when non-nil.
	nextSessionIDErr error

	nextSessionIDRequests int

	// nextSessionIDStreamID is the stream id of the last answered request.
	nextSessionIDStreamID int32
}

// NewAsyncConnectTestHarness builds a synthetic driver-backed client for connect tests.
func NewAsyncConnectTestHarness(tb testing.TB) *AsyncConnectTestHarness {
	tb.Helper()

	dir := tb.TempDir()
	logPath := asyncConnectTestExclusiveLogPath(tb, dir)
	concurrentLogPath := asyncConnectTestConcurrentLogPath(tb, dir)
	config := testutil.DefaultConductorConfig(nil)
	config.MediaDriverDir = dir
	config.ImageLifecycle = ConductorImageLifecycle
	config.ResourceLifecycle = ConductorResourceLifecycle
	fixture := testutil.NewConductorFixture(tb, config)
	client := NewTestClientFromConductorFixture(tb, fixture)
	harness := &AsyncConnectTestHarness{
		Client:            client,
		LogPath:           logPath,
		ConcurrentLogPath: concurrentLogPath,
		MediaDriverDir:    dir,
		fixture:           fixture,
		nextSessionID:     AsyncConnectTestSessionID,
	}
	harness.SetCountersReaderForRegs(tb)
	setAwaitPollSleepingHook(client, harness.handleAwaitPollSleeping)
	tb.Cleanup(func() {
		if harness.Client != nil {
			setDoWorkAfterHook(harness.Client, nil)
			setAwaitPollSleepingHook(harness.Client, nil)
			_ = harness.Client.Close()
		}
	})
	return harness
}

// SetNextSessionIDResultForTest sets the answer to later next session id
// requests. A nil err answers with sessionID. A non-nil err fails the request.
// The harness clock never advances, so the harness answers every request.
func (harness *AsyncConnectTestHarness) SetNextSessionIDResultForTest(sessionID int32, err error) {
	if harness == nil {
		return
	}
	harness.nextSessionID = sessionID
	harness.nextSessionIDErr = err
}

// NextSessionIDRequestsForTest returns the number of next session id requests
// the harness answered.
func (harness *AsyncConnectTestHarness) NextSessionIDRequestsForTest() int {
	if harness == nil {
		return 0
	}
	return harness.nextSessionIDRequests
}

// NextSessionIDStreamIDForTest returns the stream id of the last answered next
// session id request, or 0 before the first answer.
func (harness *AsyncConnectTestHarness) NextSessionIDStreamIDForTest() int32 {
	if harness == nil {
		return 0
	}
	return harness.nextSessionIDStreamID
}

// handleAwaitPollSleeping completes the active pending entry when it is a next
// session id request. RespondPendingAdds later drops the command from the ring.
//
// Concurrency: the client calls it from awaitPendingPoll under clientLock, on
// the goroutine that called NextAvailableSessionID.
func (harness *AsyncConnectTestHarness) handleAwaitPollSleeping() {
	conductorInstance := harness.fixture.Conductor
	correlationID := conductorInstance.ActiveCorrelationID()
	pending, ok := conductorInstance.PendingResult(correlationID)
	if !ok || pending.Kind != conductor.PendingKindNextSessionID {
		return
	}
	err := conductorInstance.CompletePendingForTest(
		correlationID,
		harness.nextSessionID,
		harness.nextSessionIDErr,
	)
	if err == nil {
		harness.nextSessionIDRequests++
		harness.nextSessionIDStreamID = harness.peekNextSessionIDStreamID(correlationID)
	}
}

// peekNextSessionIDStreamID returns the stream id of the unread
// GET_NEXT_AVAILABLE_SESSION_ID command with correlationID, or 0 when the ring
// does not hold the command. It does not consume a record.
func (harness *AsyncConnectTestHarness) peekNextSessionIDStreamID(correlationID int64) int32 {
	ring := harness.fixture.Ring
	buffer := ring.Buffer()
	mask := ring.Capacity() - 1
	producerPosition := ring.ProducerPosition()
	for position := ring.ConsumerPosition(); position < producerPosition; {
		recordIndex := int(position) & mask
		recordLength := int(buffer.GetInt32Volatile(record.LengthOffset(recordIndex)))
		if recordLength <= 0 {
			return 0
		}
		position += int64(record.AlignRecordLength(recordLength))
		if buffer.GetInt32(record.TypeOffset(recordIndex)) != conductor.GetNextAvailableSessionID {
			continue
		}
		bodyOffset := record.EncodedMsgOffset(recordIndex)
		decoded, err := conductor.DecodeCorrelated(buffer, bodyOffset)
		if err != nil || decoded.CorrelationID != correlationID {
			continue
		}
		return buffer.GetInt32(bodyOffset + conductor.CorrelatedMessageLength)
	}
	return 0
}

// SetErrorHandler installs a conductor ErrorHandler. The handler maps each
// error through mapConductorError, then calls handler. Nil clears it.
func (harness *AsyncConnectTestHarness) SetErrorHandler(handler func(error)) {
	if harness == nil || harness.fixture == nil {
		return
	}
	harness.fixture.Conductor.SetErrorHandlerForTest(wrapConductorErrorHandler(handler))
}

// TransmitClientTimeout broadcasts a client timeout for the harness client.
// The next harness client DoWork terminates the client with
// conductor.ErrClientTimeout.
func (harness *AsyncConnectTestHarness) TransmitClientTimeout() error {
	return harness.fixture.Transmit(
		conductor.OnClientTimeout,
		testutil.ClientTimeoutPayload(harness.Client.ClientId()),
	)
}

// SetDoWorkAfterHook runs hook after each harness client DoWork, under
// clientLock. Nil clears it. Harness cleanup clears it.
func (harness *AsyncConnectTestHarness) SetDoWorkAfterHook(hook func()) {
	if harness == nil {
		return
	}
	setDoWorkAfterHook(harness.Client, hook)
}

// SetCountersReaderForRegs replaces the conductor counters reader for multi-member tests.
func (harness *AsyncConnectTestHarness) SetCountersReaderForRegs(
	tb testing.TB,
	registrationIDs ...int64,
) {
	if harness == nil || harness.fixture == nil {
		return
	}
	harness.registrationIDs = slices.Clone(asyncConnectTestRegistrationIDs(registrationIDs))
	harness.fixture.Conductor.SetCountersReaderForTest(
		asyncConnectTestCountersReaderForRegs(tb, harness.registrationIDs),
	)
}

// TransmitAvailableImage transmits ON_AVAILABLE_IMAGE for sub on the
// to-client broadcast. It writes a fresh image log file and adds a subscriber
// position counter first. The image attaches on the next conductor tick.
//
// The counters reader is rebuilt from the registration ids that
// SetCountersReaderForRegs stored. A reader installed on the fixture
// conductor directly is replaced.
func (harness *AsyncConnectTestHarness) TransmitAvailableImage(
	tb testing.TB,
	sub *Subscription,
) {
	tb.Helper()

	require.NotNil(tb, harness)
	require.NotNil(tb, sub)

	// Each call gets its own file. An attached image keeps the earlier one
	// mapped.
	harness.imageLogCount++
	logName := "available-image-" + strconv.Itoa(harness.imageLogCount) + ".log"
	logPath := filepath.Join(harness.MediaDriverDir, logName)
	writeTestHookLogFile(tb, logPath)

	// The subscriber position record follows the publication records, which
	// the rebuilt reader keeps.
	subscriberPositionID := int32(2 * len(harness.registrationIDs))
	reader := newTestHookCountersReaderFromRecords(
		tb,
		int(subscriberPositionID)+1,
		func(meta, values []byte) {
			writeAsyncConnectTestPublicationRecords(tb, meta, values, harness.registrationIDs)
			writeTestHookSubscriberPositionRecord(
				meta,
				values,
				subscriberPositionID,
				sub.RegistrationID(),
				0,
			)
		},
	)
	harness.fixture.Conductor.SetCountersReaderForTest(reader)

	event := testHookAvailableImage(testHookImageCorrelationID)
	event.SubscriptionRegistrationID = sub.RegistrationID()
	event.StreamID = sub.StreamID()
	event.SubscriberPositionID = subscriberPositionID
	event.LogFileName = logPath
	require.NoError(tb, harness.fixture.Transmit(
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(event),
	))
}

// Fixture exposes the synthetic conductor fixture for driver error injection in tests.
func (harness *AsyncConnectTestHarness) Fixture() *testutil.ConductorFixture {
	if harness == nil {
		return nil
	}
	return harness.fixture
}

// DoWork runs one transport client tick.
func (harness *AsyncConnectTestHarness) DoWork() {
	if harness != nil && harness.Client != nil {
		harness.Client.DoWork()
	}
}

// RespondPendingAdds completes subscription and publication add commands on the ring.
// Blocks until a matching command is handled or the ring poll times out; use when
// waiting for async commands to appear after DoWork.
func (harness *AsyncConnectTestHarness) RespondPendingAdds() {
	if harness == nil || harness.fixture == nil {
		return
	}
	harness.respondPendingAddsWithLog(harness.LogPath, asyncConnectTestIngressRegID)
}

// TryRespondPendingAdds performs one non-blocking ring read for pending add commands.
// Fast path for recovery poll loops; use RespondPendingAdds when waiting for async commands.
func (harness *AsyncConnectTestHarness) TryRespondPendingAdds() {
	if harness == nil || harness.fixture == nil {
		return
	}
	handler := harness.pendingAddsHandler(harness.LogPath, asyncConnectTestIngressRegID)
	harness.fixture.Ring.Read(func(
		msgTypeID int,
		buffer atomicbuffer.AtomicBuffer,
		index, length int,
	) {
		handler(msgTypeID, buffer, index, length)
	})
}

// RespondSubscriptionAdd completes only ADD_SUBSCRIPTION commands on the ring.
func (harness *AsyncConnectTestHarness) RespondSubscriptionAdd() {
	if harness == nil || harness.fixture == nil {
		return
	}
	harness.pollRing(
		500*time.Millisecond,
		func(msgTypeID int, buffer atomicbuffer.AtomicBuffer, index, length int) bool {
			if msgTypeID != conductor.AddSubscription {
				return false
			}
			return harness.completeSubscriptionAdd(buffer, index, length)
		},
	)
}

// RespondExclusiveAdds completes exclusive publication adds with per-offer log paths.
func (harness *AsyncConnectTestHarness) RespondExclusiveAdds(
	logPaths []string,
	registrationIDs []int64,
) {
	if harness == nil || harness.fixture == nil || len(logPaths) == 0 {
		return
	}
	registrationIDs = asyncConnectTestRegistrationIDs(registrationIDs)
	pubIdx := 0
	harness.pollRing(
		200*time.Millisecond,
		func(msgTypeID int, buffer atomicbuffer.AtomicBuffer, index, length int) bool {
			if msgTypeID != conductor.AddExclusivePublication {
				return false
			}
			decoded, err := conductor.DecodePublicationMessage(buffer, index, length)
			if err != nil {
				return false
			}
			path := logPaths[min(pubIdx, len(logPaths)-1)]
			regID := registrationIDs[min(pubIdx, len(registrationIDs)-1)]
			ready := asyncConnectTestExclusivePublicationReady(
				decoded.CorrelationID,
				path,
				regID,
				pubIdx,
			)
			conductor.ExclusivePublicationReadyForTest(harness.fixture.Conductor, ready)
			pubIdx++
			return true
		},
	)
}

// PendingResult reports whether a pending registration exists for correlationID.
func (harness *AsyncConnectTestHarness) PendingResult(
	correlationID int64,
) (conductor.PendingRegistration, bool) {
	if harness == nil || harness.fixture == nil {
		return conductor.PendingRegistration{}, false
	}
	return harness.fixture.Conductor.PendingResult(correlationID)
}

// PublicationCount reports registered publications on the synthetic conductor.
func (harness *AsyncConnectTestHarness) PublicationCount() int {
	if harness == nil || harness.fixture == nil {
		return 0
	}
	return conductor.PublicationCountForTest(harness.fixture.Conductor)
}

// ClientIsClosedForTest reports whether the transport client has completed Close.
func ClientIsClosedForTest(client *Client) bool {
	if client == nil {
		return false
	}
	return client.closed.Load()
}

// ExclusiveMemberAdd maps an ingress endpoint substring to a synthetic log path and
// registration id for multi-member connect tests.
type ExclusiveMemberAdd struct {
	EndpointSubstring string
	LogPath           string
	RegistrationID    int64
	// CounterIndex selects publication limit/status counter ids (index*2, index*2+1)
	// for SetCountersReaderForRegs ordering.
	CounterIndex int
}

// RespondSubscriptionAndExclusiveMemberAdds completes subscription and pending
// exclusive publication adds in one ring pass so mixed commands are not dropped.
func (harness *AsyncConnectTestHarness) RespondSubscriptionAndExclusiveMemberAdds(
	members []ExclusiveMemberAdd,
) {
	if harness == nil || harness.fixture == nil {
		return
	}
	harness.fixture.Ring.Read(func(
		msgTypeID int,
		buffer atomicbuffer.AtomicBuffer,
		index, length int,
	) {
		switch msgTypeID {
		case conductor.AddSubscription:
			harness.completeSubscriptionAdd(buffer, index, length)
		case conductor.AddExclusivePublication:
			decoded, err := conductor.DecodePublicationMessage(buffer, index, length)
			if err != nil {
				return
			}
			for _, member := range members {
				if member.LogPath == "" {
					continue
				}
				if member.EndpointSubstring != "" &&
					!strings.Contains(decoded.Channel, member.EndpointSubstring) {
					continue
				}
				ready := asyncConnectTestExclusivePublicationReady(
					decoded.CorrelationID,
					member.LogPath,
					member.RegistrationID,
					member.CounterIndex,
				)
				conductor.ExclusivePublicationReadyForTest(harness.fixture.Conductor, ready)
				return
			}
		}
	})
}

// MemberExclusiveLogPath writes an exclusive publication log for multi-member connect tests.
func MemberExclusiveLogPath(tb testing.TB, dir string, connected bool) string {
	tb.Helper()

	suffix := "disconnected"
	if connected {
		suffix = "connected"
	}
	logPath := filepath.Join(dir, "member-exclusive-"+suffix+".log")
	writeAsyncConnectExclusiveLogFile(tb, logPath, connected)
	return logPath
}

func (harness *AsyncConnectTestHarness) respondPendingAddsWithLog(
	defaultLogPath string,
	defaultRegID int64,
) {
	harness.pollRing(200*time.Millisecond, harness.pendingAddsHandler(defaultLogPath, defaultRegID))
}

func (harness *AsyncConnectTestHarness) pendingAddsHandler(
	defaultLogPath string,
	defaultRegID int64,
) func(msgTypeID int, buffer atomicbuffer.AtomicBuffer, index, length int) bool {
	return func(msgTypeID int, buffer atomicbuffer.AtomicBuffer, index, length int) bool {
		switch msgTypeID {
		case conductor.AddSubscription:
			return harness.completeSubscriptionAdd(buffer, index, length)
		case conductor.AddExclusivePublication:
			decoded, err := conductor.DecodePublicationMessage(buffer, index, length)
			if err != nil {
				return false
			}
			ready := asyncConnectTestPublicationReady(
				decoded.CorrelationID,
				defaultLogPath,
				defaultRegID,
			)
			conductor.ExclusivePublicationReadyForTest(harness.fixture.Conductor, ready)
			return true
		case conductor.AddPublication:
			decoded, err := conductor.DecodePublicationMessage(buffer, index, length)
			if err != nil {
				return false
			}
			logPath := defaultLogPath
			if harness.ConcurrentLogPath != "" {
				logPath = harness.ConcurrentLogPath
			}
			ready := asyncConnectTestPublicationReady(
				decoded.CorrelationID,
				logPath,
				defaultRegID,
			)
			conductor.PublicationReadyForTest(harness.fixture.Conductor, ready)
			return true
		default:
			return false
		}
	}
}

func (harness *AsyncConnectTestHarness) pollRing(
	timeout time.Duration,
	handler func(msgTypeID int, buffer atomicbuffer.AtomicBuffer, index, length int) bool,
) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var handled bool
		harness.fixture.Ring.Read(func(
			msgTypeID int,
			buffer atomicbuffer.AtomicBuffer,
			index, length int,
		) {
			if handler(msgTypeID, buffer, index, length) {
				handled = true
			}
		})
		if handled {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func (harness *AsyncConnectTestHarness) completeSubscriptionAdd(
	buffer atomicbuffer.AtomicBuffer,
	index, length int,
) bool {
	decoded, err := conductor.DecodeSubscriptionMessage(buffer, index, length)
	if err != nil {
		return false
	}
	conductor.SubscriptionReadyForTest(
		harness.fixture.Conductor,
		asyncConnectTestSubscriptionReady(decoded.CorrelationID),
	)
	return true
}

func asyncConnectTestCountersReaderForRegs(
	tb testing.TB,
	registrationIDs []int64,
) *cnc.CountersReader {
	tb.Helper()

	return newTestHookCountersReaderFromRecords(
		tb,
		len(registrationIDs)*2,
		func(meta, values []byte) {
			writeAsyncConnectTestPublicationRecords(tb, meta, values, registrationIDs)
		},
	)
}

// asyncConnectTestRegistrationIDs applies the single ingress default. It
// returns the caller's slice otherwise.
func asyncConnectTestRegistrationIDs(registrationIDs []int64) []int64 {
	if len(registrationIDs) == 0 {
		return []int64{asyncConnectTestIngressRegID}
	}
	return registrationIDs
}

// writeAsyncConnectTestPublicationRecords writes the publisher limit and send
// channel status records for each registration id.
func writeAsyncConnectTestPublicationRecords(
	tb testing.TB,
	meta, values []byte,
	registrationIDs []int64,
) {
	tb.Helper()

	for i, registrationID := range registrationIDs {
		limitID := int32(i * 2)
		statusID := int32(i*2 + 1)
		cnc.WriteCounterRecord(
			meta,
			values,
			limitID,
			cnc.CounterRecordAllocated,
			cnc.DriverPublisherLimitTypeID,
			registrationID,
			1<<40,
		)
		err := cnc.WriteChannelStatusCounterRecord(
			meta,
			values,
			statusID,
			cnc.CounterRecordAllocated,
			cnc.DriverSendChannelStatusTypeID,
			testHookIPCChannel,
			registrationID,
			1,
		)
		require.NoError(tb, err)
	}
}

func asyncConnectTestPublicationReady(
	correlationID int64,
	logPath string,
	registrationID int64,
) conductor.PublicationBuffersReady {
	return conductor.PublicationBuffersReady{
		CorrelationID:             correlationID,
		RegistrationID:            registrationID,
		SessionID:                 51,
		StreamID:                  101,
		PublicationLimitCounterID: 0,
		ChannelStatusCounterID:    1,
		LogFileName:               logPath,
	}
}

func asyncConnectTestExclusivePublicationReady(
	correlationID int64,
	logPath string,
	registrationID int64,
	counterIndex int,
) conductor.PublicationBuffersReady {
	ready := asyncConnectTestPublicationReady(correlationID, logPath, registrationID)
	ready.PublicationLimitCounterID = int32(counterIndex * 2)
	ready.ChannelStatusCounterID = int32(counterIndex*2 + 1)
	return ready
}

func asyncConnectTestSubscriptionReady(correlationID int64) conductor.SubscriptionReady {
	return conductor.SubscriptionReady{
		CorrelationID:          correlationID,
		ChannelStatusCounterID: 3,
	}
}

func asyncConnectTestExclusiveLogPath(tb testing.TB, dir string) string {
	tb.Helper()

	logPath := filepath.Join(dir, "exclusive-publication.log")
	writeAsyncConnectExclusiveLogFile(tb, logPath, true)
	return logPath
}

func asyncConnectTestConcurrentLogPath(tb testing.TB, dir string) string {
	tb.Helper()

	logPath := filepath.Join(dir, "concurrent-publication.log")
	cfg := defaultInMemPublicationLogConfig()
	require.NoError(tb, os.WriteFile(logPath, inMemPublicationLogBytes(cfg), 0o644))
	return logPath
}

func writeAsyncConnectExclusiveLogFile(tb testing.TB, logPath string, connected bool) {
	tb.Helper()

	cfg := defaultInMemExclusivePublicationLogConfig()
	cfg.IsConnected = testHookConnectedInt32(connected)
	require.NoError(tb, os.WriteFile(logPath, inMemPublicationLogBytes(cfg), 0o644))
}

func newTestHookCountersReaderWithSubscriberRegistration(
	tb testing.TB,
	subscriptionRegistrationID int64,
	joinPosition int64,
) *cnc.CountersReader {
	tb.Helper()

	return newTestHookCountersReaderFromRecords(
		tb,
		int(testHookSubscriberPositionCounterID)+1,
		func(meta, values []byte) {
			writeTestHookSubscriberPositionRecord(
				meta,
				values,
				testHookSubscriberPositionCounterID,
				subscriptionRegistrationID,
				joinPosition,
			)
		},
	)
}

func writeTestHookSubscriberPositionRecord(
	meta, values []byte,
	counterID int32,
	subscriptionRegistrationID, joinPosition int64,
) {
	cnc.WriteCounterRecord(
		meta,
		values,
		counterID,
		cnc.CounterRecordAllocated,
		cnc.DriverSubscriberPositionTypeID,
		subscriptionRegistrationID,
		joinPosition,
	)
}
