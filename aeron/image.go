package aeron

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/conductor"
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/atomicbuffer"
)

// Image is one session over a conductor-mapped log.
//
// See [Subscription] for the poll rule.
//
// Image does not own the mapped log. Closing an Image keeps the term
// buffer, metadata, and header views.
type Image struct {
	subscription   *Subscription
	correlationID  int64
	sessionID      int32
	streamID       int32
	sourceIdentity string
	logFileName    string

	joinPosition  int64
	subscriberPos SubscriberPosition

	logBuffers  *logbuffer.LogBuffers
	termBuffers [logbuffer.PartitionCount]atomicbuffer.AtomicBuffer
	metaBuffer  atomicbuffer.AtomicBuffer

	initialTermID       int32
	termLengthMask      int32
	positionBitsToShift int
	mtuLength           int32

	header       Header
	errorHandler func(error)

	finalPosition       int64
	endOfStreamPosition int64
	isEndOfStream       bool

	lastCorruptReportPosition int64

	isClosed atomic.Bool
}

func newImage(
	sub *Subscription,
	event conductor.AvailableImage,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	errorHandler func(error),
) (*Image, error) {
	if sub == nil {
		return nil, errors.New("aeron: subscription is nil")
	}
	if logBuffers == nil {
		return nil, errors.New("aeron: log buffers is nil")
	}
	if countersReader == nil {
		return nil, errors.New("aeron: counters reader is nil")
	}
	if sub.RegistrationID() != event.SubscriptionRegistrationID {
		return nil, fmt.Errorf(
			"aeron: subscription registration %d does not match available image %d",
			sub.RegistrationID(),
			event.SubscriptionRegistrationID,
		)
	}
	if sub.StreamID() != event.StreamID {
		return nil, fmt.Errorf(
			"aeron: subscription stream %d does not match available image %d",
			sub.StreamID(),
			event.StreamID,
		)
	}

	mapped := logBuffers.MappedLogBuffer()
	if mapped == nil {
		return nil, errors.New("aeron: mapped log buffer is nil")
	}

	if err := requireActiveCounter(
		countersReader,
		event.SubscriberPositionID,
		event.SubscriptionRegistrationID,
		cnc.DriverSubscriberPositionTypeID,
		"subscriber position",
	); err != nil {
		return nil, err
	}

	meta, err := logBuffers.MetaDataBuffer()
	if err != nil {
		return nil, err
	}

	metadata, err := logbuffer.ParseMetadataView(meta)
	if err != nil {
		return nil, err
	}

	termLength := mapped.TermLength()
	if int(metadata.TermLength) != termLength {
		return nil, fmt.Errorf(
			"aeron: metadata term length %d does not match mapped log %d",
			metadata.TermLength,
			termLength,
		)
	}
	positionBitsToShift := logbuffer.PositionBitsToShift(termLength)
	//nolint:gosec // G115: CheckTermLength bounds the term length
	termLengthMask := int32(termLength) - 1

	termBuffers, err := logBuffers.TermBuffers()
	if err != nil {
		return nil, err
	}

	subscriberPos := newSubscriberPosition(
		event.SubscriberPositionID,
		countersReader.CounterValuesBuffer(),
	)
	joinPosition := subscriberPos.Get()

	image := &Image{
		subscription:        sub,
		correlationID:       event.CorrelationID,
		sessionID:           event.SessionID,
		streamID:            event.StreamID,
		sourceIdentity:      event.SourceIdentity,
		logFileName:         event.LogFileName,
		joinPosition:        joinPosition,
		subscriberPos:       subscriberPos,
		logBuffers:          logBuffers,
		termBuffers:         termBuffers,
		metaBuffer:          meta,
		initialTermID:       metadata.InitialTermID,
		termLengthMask:      termLengthMask,
		positionBitsToShift: positionBitsToShift,
		mtuLength:           metadata.MTULength,
		errorHandler:        errorHandler,

		lastCorruptReportPosition: -1,
	}
	image.header = Header{
		initialTermID:       metadata.InitialTermID,
		positionBitsToShift: positionBitsToShift,
		context:             image,
		fragmentedFrameLen:  headerFragmentedFrameNotSet,
	}

	return image, nil
}

func (image *Image) CorrelationID() int64 {
	return image.correlationID
}

func (image *Image) SessionID() int32 {
	return image.sessionID
}

func (image *Image) StreamID() int32 {
	return image.streamID
}

func (image *Image) SourceIdentity() string {
	return image.sourceIdentity
}

func (image *Image) LogFileName() string {
	return image.logFileName
}

// TermBufferLength returns the term buffer length of the image log.
//
// Reads the length from the term length mask (termLength - 1). Returns zero on
// a nil receiver.
func (image *Image) TermBufferLength() int32 {
	if image == nil {
		return 0
	}
	return image.termLengthMask + 1
}

// JoinPosition returns the subscriber counter value captured at image construction.
//
// The value does not change when the live counter advances during Poll.
func (image *Image) JoinPosition() int64 {
	return image.joinPosition
}

func (image *Image) Position() int64 {
	if image.isClosed.Load() {
		return image.finalPosition
	}
	return image.subscriberPos.Get()
}

// ReceivedPosition returns the stream position of the end of the contiguous
// committed data the media driver has placed in this image's term buffers.
//
// The scan starts at the subscriber position and stops at the first uncommitted
// frame. It reports the position that repeated unbounded [Image.Poll] calls
// settle at, without consuming a fragment and without advancing the subscriber
// position. A single Poll never crosses a term boundary; one ReceivedPosition
// call scans up to [logbuffer.PartitionCount] terms and stops there, so a term
// ring committed end to end reports the ring rather than looping.
//
// A corrupt or truncated committed frame stops the scan at that frame and is
// not reported. The next Poll over the same offset reports it through the
// subscriber error handler.
//
// A closed image reports its final position, the value [Image.Position] reports.
//
// ReceivedPosition is not thread-safe. Same concurrency contract as
// [Image.Poll]; see the [Image] type godoc.
func (image *Image) ReceivedPosition() int64 {
	if image.isClosed.Load() {
		return image.finalPosition
	}

	position := image.subscriberPos.Get()
	for range logbuffer.PartitionCount {
		termBuffer := image.activeTermBuffer(position)
		capacity := termBuffer.Capacity()
		offset := int(position) & int(image.termLengthMask)

		for offset < capacity {
			advance, ok := receivedFrameAdvance(termBuffer, offset, capacity)
			if !ok {
				return position
			}
			offset += advance
			position += int64(advance)
		}
	}
	return position
}

func (image *Image) SubscriberPositionID() int32 {
	return image.subscriberPos.CounterID()
}

func (image *Image) IsClosed() bool {
	return image.isClosed.Load()
}

// IsEndOfStream reports whether the subscriber position has reached the
// end-of-stream position.
//
// A closed image returns the value read at close.
//
// IsEndOfStream is not thread-safe. Same concurrency contract as
// [Image.Poll]; see the [Image] type godoc.
func (image *Image) IsEndOfStream() bool {
	if image.isClosed.Load() {
		return image.isEndOfStream
	}
	return image.subscriberPos.Get() >= logbuffer.EndOfStreamPositionVolatile(image.metaBuffer)
}

// EndOfStreamPosition returns the position the publisher signalled as end
// of stream, or math.MaxInt64 before the signal.
//
// A closed image returns the value read at close.
//
// EndOfStreamPosition is not thread-safe. Same concurrency contract as
// [Image.Poll]; see the [Image] type godoc.
func (image *Image) EndOfStreamPosition() int64 {
	if image.isClosed.Load() {
		return image.endOfStreamPosition
	}
	return logbuffer.EndOfStreamPositionVolatile(image.metaBuffer)
}

// ActiveTransportCount returns the number of transports the media driver saw
// within the image liveness timeout.
//
// A closed image returns 0. An open image may return 0 before the first
// datagram. An IPC image returns 0.
//
// ActiveTransportCount is not thread-safe. Same concurrency contract as
// [Image.Poll].
func (image *Image) ActiveTransportCount() int32 {
	if image.isClosed.Load() {
		return 0
	}
	return logbuffer.ActiveTransportCountVolatile(image.metaBuffer)
}

func (image *Image) activeTermBuffer(position int64) atomicbuffer.AtomicBuffer {
	termIndex := logbuffer.IndexByPosition(position, image.positionBitsToShift)
	return image.termBuffers[termIndex]
}

func termEndSlackAdvance(
	offset int,
	capacity int,
	frameLength int32,
) (advance int, ok bool) {
	remaining := capacity - offset
	if remaining <= 0 || remaining >= logbuffer.DataHeaderLength {
		return 0, false
	}
	if frameLength > 0 && int(frameLength) != remaining {
		return 0, false
	}
	return remaining, true
}

func paddingAdvance(
	frameOffset int,
	frameLength int32,
	capacity int,
) (advance int, ok bool) {
	if frameLength <= 0 || frameOffset+int(frameLength) > capacity {
		return 0, false
	}
	if frameLength < int32(logbuffer.DataHeaderLength) {
		return 0, false
	}
	// Term-end PAD: committed raw span fills the remaining term bytes (e.g.
	// 40-byte PAD at termLength-40). Advance by frameLength, not alignment.
	if frameOffset+int(frameLength) == capacity {
		return int(frameLength), true
	}
	advance = logbuffer.AlignFrameLength(int(frameLength))
	if frameOffset+advance > capacity {
		return 0, false
	}
	return advance, true
}

// pollConsumePosition maps a term offset back to the subscriber log position for a
// single-term poll scan. Callers must pass initialPosition and initialOffset from
// poll entry; offset must remain within the same term as initialOffset.
func pollConsumePosition(initialPosition int64, initialOffset, offset int) int64 {
	return initialPosition + int64(offset-initialOffset)
}

func dataFrameNextOffset(
	frameOffset int,
	frameLength int32,
	capacity int,
) (nextOffset int, ok bool) {
	if frameLength < int32(logbuffer.DataHeaderLength) {
		return 0, false
	}
	frameLengthInt := int(frameLength)
	if frameOffset+frameLengthInt > capacity {
		return 0, false
	}
	nextOffset = frameOffset + logbuffer.AlignFrameLength(frameLengthInt)
	if nextOffset > capacity {
		return 0, false
	}
	return nextOffset, true
}

// receivedFrameAdvance returns the byte advance across the committed frame at
// offset. Poll's loop is the authority on these rules. The property that compares
// ReceivedPosition to repeated unbounded Poll asserts they agree.
func receivedFrameAdvance(
	termBuffer atomicbuffer.AtomicBuffer,
	offset int,
	capacity int,
) (advance int, ok bool) {
	frameLength := logbuffer.CommittedFrameLengthVolatile(termBuffer, offset)
	if slack, slackOK := termEndSlackAdvance(offset, capacity, frameLength); slackOK {
		return slack, true
	}
	if frameLength <= 0 {
		return 0, false
	}
	if logbuffer.IsPaddingFrameInTerm(termBuffer, offset) {
		return paddingAdvance(offset, frameLength, capacity)
	}
	nextOffset, dataOK := dataFrameNextOffset(offset, frameLength, capacity)
	if !dataOK {
		return 0, false
	}
	return nextOffset - offset, true
}

func (image *Image) reportCorruptCommittedFrame(
	initialPosition int64,
	initialOffset int,
	offset int,
	frameLength int32,
) {
	consumePosition := pollConsumePosition(initialPosition, initialOffset, offset)
	if consumePosition == image.lastCorruptReportPosition {
		return
	}
	image.lastCorruptReportPosition = consumePosition
	image.invokeSubscriberErrorHandler(fmt.Errorf(
		"aeron: corrupt committed frame at term offset %d length %d",
		offset,
		frameLength,
	))
}

func (image *Image) invokeSubscriberErrorHandler(err error) {
	if image.errorHandler == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	image.errorHandler(err)
}

// reportFragmentHandlerPanic sends a recovered fragment handler panic to the
// subscriber error handler. Nil recovered is a no-op.
func (image *Image) reportFragmentHandlerPanic(recovered any) {
	if recovered == nil {
		return
	}
	err, isErr := recovered.(error)
	if !isErr {
		err = fmt.Errorf("aeron: fragment handler panic: %v", recovered)
	}
	image.invokeSubscriberErrorHandler(err)
}

func preparePollFragment(
	termBuffer atomicbuffer.AtomicBuffer,
	frameOffset int,
	frameLength int32,
	header *Header,
) (payloadOffset, payloadLength int) {
	header.SetBuffer(termBuffer)
	header.ResetPollFragmentState()
	header.SetOffset(frameOffset)
	payloadOffset = frameOffset + logbuffer.OffsetFramePayload
	payloadLength = int(frameLength) - logbuffer.DataHeaderLength
	return payloadOffset, payloadLength
}

func (image *Image) invokeFragmentHandler(
	handler FragmentHandler,
	termBuffer atomicbuffer.AtomicBuffer,
	frameOffset int,
	frameLength int32,
	header *Header,
) {
	payloadOffset, payloadLength := preparePollFragment(
		termBuffer,
		frameOffset,
		frameLength,
		header,
	)
	handler(termBuffer, payloadOffset, payloadLength, header)
}

// Poll scans one active log term and delivers up to fragmentLimit committed DATA
// fragments via handler. Padding frames advance the consume position without
// invoking the handler or incrementing the fragment count. A single poll call
// does not span terms; the next poll selects the active term from the updated
// subscriber position.
//
// Poll reads subscriber position with a volatile load at entry and publishes an
// advanced position with SetRelease when bytes were consumed and the image is
// still open. PAD-only scans with zero fragments still advance and store.
// Term-end PAD at capacity advances by raw committed padding bytes, not
// alignment.
//
// A handler panic consumes the failing DATA frame. The frame counts toward
// fragmentsRead. The scan stops. Poll stores the position past the frame
// before it returns or the panic propagates. With [Config.ErrorHandler]
// set, Poll reports the panic value to it as an error. Without one, the
// panic propagates to the caller. Poll drops a panic raised by the error
// handler.
func (image *Image) Poll(handler FragmentHandler, fragmentLimit int) int {
	return image.pollBounded(handler, fragmentLimit, false, 0)
}

// BoundedPoll is [Image.Poll] with a stream-position upper bound.
//
// The call does not deliver a frame that starts at or past limitPosition.
// The call delivers the whole frame when it starts below limitPosition, even
// when it ends past limitPosition. A padding frame that starts below
// limitPosition advances past its end.
//
// When the subscriber position is already at or past limitPosition, the call
// returns 0 without storing the subscriber position.
//
// One call stays inside the current term, even when limitPosition is past the
// term end.
//
// Returns 0 when:
//   - The image is closed.
//   - The handler is nil.
//   - fragmentLimit is zero or less.
//
// BoundedPoll is not thread-safe. Same concurrency contract as [Image.Poll].
func (image *Image) BoundedPoll(
	handler FragmentHandler,
	limitPosition int64,
	fragmentLimit int,
) int {
	return image.pollBounded(handler, fragmentLimit, true, limitPosition)
}

func (image *Image) pollBounded(
	handler FragmentHandler,
	fragmentLimit int,
	bounded bool,
	limitPosition int64,
) (fragmentsRead int) {
	if image.isClosed.Load() || handler == nil || fragmentLimit <= 0 {
		return 0
	}

	initialPosition := image.subscriberPos.Get()
	if bounded && initialPosition >= limitPosition {
		return 0
	}

	initialOffset := int(initialPosition) & int(image.termLengthMask)
	offset := initialOffset
	termBuffer := image.activeTermBuffer(initialPosition)
	capacity := termBuffer.Capacity()
	limitOffset := capacity
	if bounded {
		limitOffset = computeLimitOffset(capacity, offset, initialPosition, limitPosition)
	}

	header := &image.header

	defer func() {
		if image.errorHandler != nil {
			image.reportFragmentHandlerPanic(recover())
		}
		image.publishPollConsumePosition(initialPosition, initialOffset, offset)
	}()

	for fragmentsRead < fragmentLimit && offset < limitOffset && !image.isClosed.Load() {
		frameLength := logbuffer.CommittedFrameLengthVolatile(termBuffer, offset)
		if advance, ok := termEndSlackAdvance(offset, capacity, frameLength); ok {
			offset += advance
			continue
		}
		if frameLength <= 0 {
			break
		}

		if logbuffer.IsPaddingFrameInTerm(termBuffer, offset) {
			advance, ok := paddingAdvance(offset, frameLength, capacity)
			if !ok {
				image.reportCorruptCommittedFrame(
					initialPosition,
					initialOffset,
					offset,
					frameLength,
				)
				break
			}
			offset += advance
			trackPollAfterPaddingAdvance(image)
			continue
		}

		nextOffset, ok := dataFrameNextOffset(offset, frameLength, capacity)
		if !ok {
			image.reportCorruptCommittedFrame(
				initialPosition,
				initialOffset,
				offset,
				frameLength,
			)
			break
		}

		// Move past the frame before the call, so a handler panic consumes it.
		frameOffset := offset
		offset = nextOffset
		fragmentsRead++
		image.invokeFragmentHandler(handler, termBuffer, frameOffset, frameLength, header)
	}

	return fragmentsRead
}

// Shared store for Poll and controlled scan: one SetRelease + corrupt debounce path.
// Returns the computed consume position (whether or not a store ran).
func (image *Image) publishPollConsumePosition(
	initialPosition int64,
	initialOffset int,
	offset int,
) int64 {
	newPosition := pollConsumePosition(initialPosition, initialOffset, offset)
	if newPosition > initialPosition && !image.isClosed.Load() {
		image.subscriberPos.SetRelease(newPosition)
		if image.lastCorruptReportPosition >= 0 && newPosition > image.lastCorruptReportPosition {
			image.lastCorruptReportPosition = -1
		}
	}
	return newPosition
}

func (image *Image) internalClose() {
	if image.isClosed.Load() {
		return
	}
	image.finalPosition = image.subscriberPos.Get()
	image.endOfStreamPosition = logbuffer.EndOfStreamPositionVolatile(image.metaBuffer)
	image.isEndOfStream = image.finalPosition >= image.endOfStreamPosition
	image.isClosed.Store(true)
}
