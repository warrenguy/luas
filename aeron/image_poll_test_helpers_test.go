package aeron_test

import (
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/atomicbuffer"
)

const (
	pollTestZeroAllocPayload     = aeron.PollTestZeroAllocPayload
	pollTestPaddingLength        = int32(2 * logbuffer.FrameAlignment)
	pollTestWidePaddingLength    = int32(3 * logbuffer.FrameAlignment)
	pollTestNonZeroFrameOffset   = logbuffer.FrameAlignment
	pollTestTailFrameLength      = 2 * logbuffer.FrameAlignment
	pollTestMinimalPadLength     = int32(8)
	pollTestTermEndFullPadLength = int32(40)

	// The payload is a FrameAlignment multiple, so header plus payload is the
	// aligned chunk advance writeDataFrameChain takes per frame.
	receivedPositionPayloadLength = 96
	receivedPositionFrameLength   = logbuffer.DataHeaderLength + receivedPositionPayloadLength
)

type pollCapture struct {
	payloads [][]byte
	flags    []byte
	headers  []aeron.Header
}

func (capture *pollCapture) reset() {
	capture.payloads = capture.payloads[:0]
	capture.flags = capture.flags[:0]
	capture.headers = capture.headers[:0]
}

func (capture *pollCapture) handler(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) {
	capture.payloads = append(capture.payloads, slices.Clone(buffer.Bytes()[offset:offset+length]))
	capture.flags = append(capture.flags, header.Flags())
	headerCopy := *header
	capture.headers = append(capture.headers, headerCopy)
}

type controlledPollCapture struct {
	pollCapture
	actions []aeron.ControlledAction
}

func (capture *controlledPollCapture) reset() {
	capture.pollCapture.reset()
	capture.actions = nil
}

func (capture *controlledPollCapture) handler(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) aeron.ControlledAction {
	capture.pollCapture.handler(buffer, offset, length, header)
	action := aeron.ControlledContinue
	i := len(capture.payloads) - 1
	if i < len(capture.actions) {
		action = capture.actions[i]
	}
	return action
}

func newTestPollImage(tb testing.TB, joinPosition int64) *aeron.Image {
	tb.Helper()
	return aeron.NewImageForTest(tb, testSubscription(tb), joinPosition)
}

func newTestPollImageWithErrorHandler(
	tb testing.TB,
	joinPosition int64,
) (*aeron.Image, func() error) {
	tb.Helper()

	var handled error
	image := aeron.NewImageForTestWithErrorHandler(
		tb,
		testSubscription(tb),
		joinPosition,
		func(err error) {
			handled = err
		},
	)
	return image, func() error { return handled }
}

func newControlledPollImage(
	tb testing.TB,
	joinPosition int64,
) (*aeron.Image, *controlledPollCapture) {
	tb.Helper()
	return newTestPollImage(tb, joinPosition), &controlledPollCapture{}
}

func newControlledPollImageWithErrorHandler(
	tb testing.TB,
	joinPosition int64,
) (*aeron.Image, *controlledPollCapture, func() error) {
	tb.Helper()

	image, handledErr := newTestPollImageWithErrorHandler(tb, joinPosition)
	return image, &controlledPollCapture{}, handledErr
}

func newDirectPollImage(
	tb testing.TB,
	joinPosition int64,
) (*aeron.Image, *cnc.CountersReader) {
	tb.Helper()

	sub := testSubscription(tb)
	logBuffers := aeron.NewTestLogBuffersForTest(tb, aeron.DefaultTestPublicationLogConfig())
	counters := aeron.NewTestCountersReaderWithSubscriberPositionForTest(tb, joinPosition)
	image, err := aeron.NewImageDirectForTest(sub, aeron.AvailableImageForTest(), logBuffers, counters)
	require.NoError(tb, err)
	return image, counters
}

func newPollImage(tb testing.TB, joinPosition int64) (*aeron.Image, *pollCapture) {
	tb.Helper()
	return newTestPollImage(tb, joinPosition), &pollCapture{}
}

func newPollImageWithErrorHandler(
	tb testing.TB,
	joinPosition int64,
) (*aeron.Image, *pollCapture, func() error) {
	tb.Helper()

	image, handledErr := newTestPollImageWithErrorHandler(tb, joinPosition)
	return image, &pollCapture{}, handledErr
}

func writePollTestDataFrame(
	termBuffer atomicbuffer.AtomicBuffer,
	frameOffset int,
	termOffset int32,
	flags uint8,
	payload []byte,
) {
	aeron.WriteCommittedDataFrameForTest(
		termBuffer,
		frameOffset,
		aeron.TestPublicationSessionID,
		aeron.TestPublicationStreamID,
		aeron.TestPublicationInitialTermID,
		termOffset,
		flags,
		payload,
	)
}

// writePollTestPayloads commits one unfragmented DATA frame per payload from
// startOffset. Returns the offset past the last frame.
func writePollTestPayloads(
	termBuffer atomicbuffer.AtomicBuffer,
	startOffset int,
	payloads ...[]byte,
) int {
	offset := startOffset
	for _, payload := range payloads {
		writePollTestDataFrame(termBuffer, offset, int32(offset), logbuffer.Unfragmented, payload)
		offset += logbuffer.AlignFragmentChunkLength(len(payload))
	}
	return offset
}

// writePollTestFragmentedThenNext commits BEGIN "hello-", END "world", then
// unfragmented "next". Returns the offset past the END frame.
func writePollTestFragmentedThenNext(termBuffer atomicbuffer.AtomicBuffer) int {
	begin := []byte("hello-")
	end := []byte("world")
	writePollTestDataFrame(termBuffer, 0, 0, logbuffer.BeginFlag, begin)
	endOffset := logbuffer.AlignFragmentChunkLength(len(begin))
	writePollTestDataFrame(termBuffer, endOffset, int32(endOffset), logbuffer.EndFlag, end)
	messageEnd := endOffset + logbuffer.AlignFragmentChunkLength(len(end))
	writePollTestPayloads(termBuffer, messageEnd, []byte("next"))
	return messageEnd
}

func writePollTestPaddingThenData(
	termBuffer atomicbuffer.AtomicBuffer,
	paddingLength int32,
	payload []byte,
) {
	aeron.WriteCommittedPaddingFrameForTest(
		termBuffer,
		0,
		aeron.TestPublicationInitialTermID,
		0,
		paddingLength,
	)
	writePollTestDataFrame(
		termBuffer,
		int(paddingLength),
		int32(paddingLength),
		logbuffer.Unfragmented,
		payload,
	)
}

// writePollTestCorruptFrame commits a data frame one byte shorter than a
// header, the smallest corrupt committed frame.
func writePollTestCorruptFrame(termBuffer atomicbuffer.AtomicBuffer, offset int) {
	logbuffer.PutFrameType(termBuffer, offset, logbuffer.HdrTypeData)
	logbuffer.PutFrameLengthOrdered(
		termBuffer,
		offset,
		int32(logbuffer.DataHeaderLength-1),
	)
}

func writePollTestCommittedThenCorrupt(
	termBuffer atomicbuffer.AtomicBuffer,
	payload []byte,
	corruptOffset int,
) {
	writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, payload)
	writePollTestCorruptFrame(termBuffer, corruptOffset)
}

func writeDataFrameChain(
	tb testing.TB,
	termBuffer atomicbuffer.AtomicBuffer,
	startOffset int,
	frameCount int,
	payload []byte,
) int {
	tb.Helper()

	offset := startOffset
	for range frameCount {
		writePollTestDataFrame(termBuffer, offset, int32(offset), logbuffer.Unfragmented, payload)
		offset += logbuffer.AlignFragmentChunkLength(len(payload))
	}
	return offset
}

// nthFramePanicHandler returns a handler that panics with err on call n.
func nthFramePanicHandler(n int, err error) aeron.FragmentHandler {
	attempts := 0
	return func(atomicbuffer.AtomicBuffer, int, int, *aeron.Header) {
		attempts++
		if attempts == n {
			panic(err)
		}
	}
}

// nthFrameControlledPanicHandler returns a handler that panics with err on
// call n. Every other call returns action.
func nthFrameControlledPanicHandler(
	n int,
	err error,
	action aeron.ControlledAction,
) aeron.ControlledFragmentHandler {
	handler := nthFramePanicHandler(n, err)
	return func(
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	) aeron.ControlledAction {
		handler(buffer, offset, length, header)
		return action
	}
}

var imagePollSink int

func imagePollZeroAllocHandler(
	_ atomicbuffer.AtomicBuffer,
	_ int,
	length int,
	_ *aeron.Header,
) {
	imagePollSink = length
}

type subscriptionPollStep struct {
	want     int
	payloads [][]byte
}

func addSubscriptionPollImage(
	tb testing.TB,
	sub *aeron.Subscription,
	correlationID int64,
	payloads ...[]byte,
) *aeron.Image {
	tb.Helper()

	image := aeron.NewImageForTestWithCorrelationID(tb, sub, 0, correlationID)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	offset := 0
	for _, payload := range payloads {
		writePollTestDataFrame(
			termBuffer,
			offset,
			int32(offset),
			logbuffer.Unfragmented,
			payload,
		)
		offset += logbuffer.AlignFragmentChunkLength(len(payload))
	}
	aeron.SubscriptionAddImageForTest(sub, image)
	return image
}

func addTwoStandardPollImages(tb testing.TB, sub *aeron.Subscription) {
	tb.Helper()

	addSubscriptionPollImage(
		tb,
		sub,
		aeron.TestImageCorrelationID,
		[]byte("img0-a"),
		[]byte("img0-b"),
	)
	addSubscriptionPollImage(
		tb,
		sub,
		aeron.TestImageCorrelationID+1,
		[]byte("img1-a"),
		[]byte("img1-b"),
	)
}

// addSubscriptionPollSlots fills the image slots in order. Each slot holds the
// frame payloads for one image. A nil slot injects a nil image entry.
func addSubscriptionPollSlots(tb testing.TB, sub *aeron.Subscription, slots [][][]byte) {
	tb.Helper()

	for index, payloads := range slots {
		if payloads == nil {
			aeron.SubscriptionInjectNilImageAtIndexForHooks(sub, index)
			continue
		}
		addSubscriptionPollImage(tb, sub, aeron.TestImageCorrelationID+int64(index), payloads...)
	}
	require.Equal(tb, len(slots), sub.ImageCount())
}

func requireSubscriptionPoll(
	tb testing.TB,
	sub *aeron.Subscription,
	capture *pollCapture,
	limit int,
	want int,
	wantPayloads [][]byte,
) {
	tb.Helper()

	require.Equal(tb, want, sub.Poll(capture.handler, limit))
	require.Equal(tb, wantPayloads, capture.payloads)
}

func requireSubscriptionPollSteps(
	tb testing.TB,
	sub *aeron.Subscription,
	capture *pollCapture,
	limit int,
	steps ...subscriptionPollStep,
) {
	tb.Helper()

	for _, step := range steps {
		capture.reset()
		requireSubscriptionPoll(tb, sub, capture, limit, step.want, step.payloads)
	}
}

func requireSubscriptionControlledPoll(
	tb testing.TB,
	sub *aeron.Subscription,
	capture *controlledPollCapture,
	limit int,
	want int,
	wantPayloads [][]byte,
) {
	tb.Helper()

	require.Equal(tb, want, sub.ControlledPoll(capture.handler, limit))
	require.Equal(tb, wantPayloads, capture.payloads)
}

func requireSubscriptionControlledPollSteps(
	tb testing.TB,
	sub *aeron.Subscription,
	capture *controlledPollCapture,
	limit int,
	steps ...subscriptionPollStep,
) {
	tb.Helper()

	for _, step := range steps {
		capture.reset()
		requireSubscriptionControlledPoll(tb, sub, capture, limit, step.want, step.payloads)
	}
}

func subscriptionPollOnFirstFragment(
	tb testing.TB,
	capture *pollCapture,
	onFirst func(tb testing.TB),
) aeron.FragmentHandler {
	tb.Helper()

	var handlerCalls int
	return func(
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	) {
		handlerCalls++
		if handlerCalls == 1 {
			onFirst(tb)
		}
		capture.handler(buffer, offset, length, header)
	}
}

func setupZeroAllocPollImage(tb testing.TB) *aeron.Image {
	tb.Helper()
	return primeZeroAllocPollImage(tb, newTestPollImage(tb, 0))
}

func setupZeroAllocPollImageWithErrorHandler(tb testing.TB) *aeron.Image {
	tb.Helper()

	image, _ := newTestPollImageWithErrorHandler(tb, 0)
	return primeZeroAllocPollImage(tb, image)
}

// primeZeroAllocPollImage warms the image: one committed zero-alloc frame,
// one Poll.
func primeZeroAllocPollImage(tb testing.TB, image *aeron.Image) *aeron.Image {
	tb.Helper()

	writeDataFrameChain(
		tb,
		aeron.ImageTermBuffer0ForTest(image),
		0,
		1,
		aeron.PollTestZeroAllocPayloadBytes(),
	)
	imagePollSink = image.Poll(imagePollZeroAllocHandler, 1)
	return image
}

func receivedPositionNoopHandler(
	_ atomicbuffer.AtomicBuffer,
	_ int,
	_ int,
	_ *aeron.Header,
) {
}

// pollImageToExhaustion is the ReceivedPosition oracle: repeated unbounded Poll
// until no call makes progress. A single Poll never crosses a term boundary, so
// a term crossing needs the loop.
//
// Valid only for layouts that stop inside the mapped term ring (or that do not
// fully commit every partition from the subscriber position). Poll has no ring
// bound: a term ring committed end to end never stalls, so a fixed call cap would
// return a mid-wrap position. Callers that fill the whole ring assert that case
// separately.
func pollImageToExhaustion(t require.TestingT, image *aeron.Image) int64 {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}

	const maxCalls = 2 * logbuffer.PartitionCount
	for range maxCalls {
		before := image.Position()
		image.Poll(receivedPositionNoopHandler, math.MaxInt)
		if image.Position() == before {
			return image.Position()
		}
	}
	require.Fail(t, "poll did not settle: layout must stop inside the term ring")
	return image.Position()
}

func writeReceivedPositionFrames(
	tb testing.TB,
	image *aeron.Image,
	termIndex int,
	startOffset int,
	frameCount int,
) int {
	tb.Helper()

	return writeDataFrameChain(
		tb,
		aeron.ImageTermBufferForTest(image, termIndex),
		startOffset,
		frameCount,
		make([]byte, receivedPositionPayloadLength),
	)
}

var imageReceivedPositionSink int64

// setupZeroAllocReceivedPositionImage lays out a scan that crosses a term
// boundary, so the allocation check covers the multi-term path.
func setupZeroAllocReceivedPositionImage(tb testing.TB) *aeron.Image {
	tb.Helper()

	start := aeron.TestPublicationTermLength - 2*receivedPositionFrameLength
	image, _ := newPollImage(tb, 0)
	aeron.ImageSetSubscriberPositionForTest(image, int64(start))
	writeReceivedPositionFrames(tb, image, 0, start, 2)
	writeReceivedPositionFrames(tb, image, 1, 0, 4)
	imageReceivedPositionSink = image.ReceivedPosition()
	return image
}
