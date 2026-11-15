package aeron

import (
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/atomicbuffer"
)

const fragmentBuilderIdleNextTermOffset int32 = -1

const fragmentBuilderInitMinCapacity = 4096

// FragmentAssembler reassembles BEGIN/middle/END fragmented DATA frames and
// forwards whole payloads to the delegate FragmentHandler. Unfragmented frames
// reach the delegate without a copy. Reassembled deliveries use an
// assembler-owned buffer slice and &builder.completedHeader; both are valid
// only for the delegate callback and must not be retained past return.
//
// Reassembly runs on a ControlledFragmentAssembler core. Every delivery returns
// ControlledContinue because FragmentHandler has no poll action.
//
// Each session keeps its builder, and its payload capacity, after delivery.
// Call [FragmentAssembler.FreeSessionBuffer] or [FragmentAssembler.Clear] on
// the poll goroutine to release it. Free a session after its image closes.
//
// Concurrency: not safe for concurrent use. Delegate callbacks must not poll
// the same assembler reentrantly.
type FragmentAssembler struct {
	core     *ControlledFragmentAssembler
	delegate FragmentHandler
}

type fragmentBufferBuilder struct {
	payload        []byte
	limit          int
	nextTermOffset int32
	// firstFrameLength is the committed BEGIN frame length at capture time.
	// ConfigureCompletedFragment patches frame length in place on
	// capturedFrameHeader; ControlledFragmentAssembler Abort redelivery
	// restores this value so span and Position recompute correctly.
	firstFrameLength    int32
	capturedFrameHeader [logbuffer.DataHeaderLength]byte
	completedHeader     Header
}

// NewFragmentAssembler returns a FragmentAssembler with default options.
func NewFragmentAssembler(delegate FragmentHandler) *FragmentAssembler {
	return NewFragmentAssemblerWithOptions(delegate, 0, false)
}

// NewFragmentAssemblerWithOptions constructs a FragmentAssembler.
//
// directAssemblers is stored for Java API parity; reassembly uses heap buffers only.
// initialBufferLength is clamped: negative values become 0; values above
// logbuffer.MaxMessageLength become logbuffer.MaxMessageLength.
func NewFragmentAssemblerWithOptions(
	delegate FragmentHandler,
	initialBufferLength int,
	directAssemblers bool,
) *FragmentAssembler {
	assembler := &FragmentAssembler{delegate: delegate}
	assembler.core = NewControlledFragmentAssemblerWithOptions(
		assembler.handleControlledFragment,
		initialBufferLength,
		directAssemblers,
	)
	return assembler
}

// Handler returns the FragmentHandler passed to Subscription.Poll.
func (assembler *FragmentAssembler) Handler() FragmentHandler {
	return assembler.handleFragment
}

// DirectAssemblers reports the isDirectAssemblers policy. Heap buffers only until
// a future direct-backing optimization lands.
func (assembler *FragmentAssembler) DirectAssemblers() bool {
	return assembler.core.DirectAssemblers()
}

// Clear evicts every session builder, in progress or idle.
func (assembler *FragmentAssembler) Clear() {
	assembler.core.Clear()
}

// FreeSessionBuffer evicts the reassembly builder for sessionID.
func (assembler *FragmentAssembler) FreeSessionBuffer(sessionID int32) bool {
	return assembler.core.FreeSessionBuffer(sessionID)
}

func (assembler *FragmentAssembler) handleFragment(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *Header,
) {
	assembler.core.handleFragment(buffer, offset, length, header)
}

// handleControlledFragment is the ControlledFragmentAssembler delegate. Invokes
// the plain FragmentHandler and returns ControlledContinue.
func (assembler *FragmentAssembler) handleControlledFragment(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *Header,
) ControlledAction {
	assembler.delegate(buffer, offset, length, header)
	return ControlledContinue
}

func (builder *fragmentBufferBuilder) reset() {
	builder.limit = 0
	builder.nextTermOffset = fragmentBuilderIdleNextTermOffset
	builder.firstFrameLength = 0
	builder.completedHeader.resetCompletedFragment()
}

func (builder *fragmentBufferBuilder) captureHeader(
	header *Header,
	buffer atomicbuffer.AtomicBuffer,
) {
	frameOffset := header.Offset()
	copy(
		builder.capturedFrameHeader[:],
		buffer.Bytes()[frameOffset:frameOffset+logbuffer.DataHeaderLength],
	)
	builder.firstFrameLength = logbuffer.FrameLength(
		atomicbuffer.Wrap(builder.capturedFrameHeader[:]),
		0,
	)
}

// restoreFirstFrameLength writes the BEGIN frame length back into
// capturedFrameHeader. Call before ConfigureCompletedFragment when the
// builder may have been used for a prior completed-header patch (Abort
// redelivery).
func (builder *fragmentBufferBuilder) restoreFirstFrameLength() {
	if builder.firstFrameLength <= 0 {
		return
	}
	logbuffer.PutFrameLengthOrdered(
		atomicbuffer.Wrap(builder.capturedFrameHeader[:]),
		0,
		builder.firstFrameLength,
	)
}

func (builder *fragmentBufferBuilder) appendAtExpectedOffset(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	termOffset int32,
	initialBufferLength int,
) bool {
	if termOffset != builder.nextTermOffset {
		builder.reset()
		return false
	}
	return builder.append(buffer, offset, length, initialBufferLength)
}

func (builder *fragmentBufferBuilder) append(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	initialBufferLength int,
) bool {
	bytes := buffer.Bytes()
	if length < 0 || offset < 0 || offset+length > len(bytes) {
		builder.reset()
		return false
	}
	if !builder.ensureCapacity(length, initialBufferLength) {
		builder.reset()
		return false
	}
	copy(builder.payload[builder.limit:builder.limit+length], bytes[offset:offset+length])
	builder.limit += length
	return true
}

func (builder *fragmentBufferBuilder) ensureCapacity(
	additional int,
	initialBufferLength int,
) bool {
	required := builder.limit + additional
	if required > logbuffer.MaxMessageLength {
		return false
	}
	if required <= cap(builder.payload) {
		return true
	}

	next := cap(builder.payload)
	if next == 0 {
		next = max(initialBufferLength, fragmentBuilderInitMinCapacity)
	}
	for next < required {
		next *= 2
		if next > logbuffer.MaxMessageLength {
			next = logbuffer.MaxMessageLength
			break
		}
	}
	grown := make([]byte, next)
	copy(grown, builder.payload[:builder.limit])
	builder.payload = grown
	return true
}
