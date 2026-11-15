package service

import "github.com/warrenguy/luas/aeron"

// defaultControlFragmentLimit is the max fragments drained per control poll.
// Matches the tree control batch size of 10. Upstream ServiceAdapter uses
// fragment limit 1; Luas uses the tree batch under FIFO delivery.
const defaultControlFragmentLimit = 10

// ControlIngress polls one duty-cycle batch of whole control messages
// (stream 104). onFragment runs once per assembled message with the buffer at
// MessageHeader offset.
//
// Implementations: [QueueControlIngress], [SubscriptionControlIngress].
//
// Poll returns the work count (implementation-defined: queue = messages
// delivered; subscription = poller fragment count). onFragment runs once per
// whole message.
//
// Buffer lifetime is implementation-defined. [SubscriptionControlIngress]
// slices are valid only for the callback. [QueueControlIngress] passes the
// caller-owned enqueued buffer. Production onFragment must decode in the
// callback and must not retain the slice across implementations.
//
// Concurrency: invoker-only. Must run on the same goroutine as the service
// agent duty cycle.
type ControlIngress interface {
	Poll(fragmentLimit int, onFragment func(buf []byte, offset int)) int
}

// QueueControlIngress is an in-memory [ControlIngress] for unit inject.
// Each element is a full SBE message including the MessageHeader.
//
// Concurrency: invoker-only. Not safe for concurrent Poll or Enqueue.
type QueueControlIngress struct {
	msgs [][]byte
}

// Enqueue appends one full message buffer for a later [QueueControlIngress.Poll].
//
// Ownership: retains msg without clone. The caller must not mutate msg until
// after Poll has delivered it, or must pass a dedicated or cloned buffer.
// A nil msg is stored and delivered as nil.
func (q *QueueControlIngress) Enqueue(msg []byte) {
	if q == nil {
		return
	}
	q.msgs = append(q.msgs, msg)
}

// Poll drains up to fragmentLimit queued messages, calling onFragment for each
// at offset 0. Returns the number of messages delivered.
//
// Returns 0 when the receiver is nil, onFragment is nil, or fragmentLimit is
// not positive.
func (q *QueueControlIngress) Poll(fragmentLimit int, onFragment func([]byte, int)) int {
	if q == nil || onFragment == nil || fragmentLimit <= 0 {
		return 0
	}
	n := 0
	for n < fragmentLimit && len(q.msgs) > 0 {
		m := q.msgs[0]
		q.msgs = q.msgs[1:]
		onFragment(m, 0)
		n++
	}
	return n
}

var _ ControlIngress = (*QueueControlIngress)(nil)

// ControlPoller delivers committed fragments from a subscription.
//
// [*aeron.Subscription] satisfies this interface as-built. The compile-time
// assertion below fails the build if either side moves.
//
// The seam exists so fragment translation is testable without a media driver. A
// zero-value [*aeron.Subscription] is safe to Poll but always reports zero
// fragments, so tests can exercise the early returns and nothing else.
//
// Concurrency: implementations are not required to be safe for concurrent use.
// [SubscriptionControlIngress] calls ControlPoller only from the invoker
// goroutine.
type ControlPoller interface {
	Poll(handler aeron.FragmentHandler, fragmentLimit int) int
}

var _ ControlPoller = (*aeron.Subscription)(nil)

// SubscriptionControlIngress polls a subscription and delivers whole control
// messages. Fragmented trains reassemble through one [aeron.FragmentAssembler].
//
// Used for the module-to-service control stream (default 104). Reassembles so
// variable-length JoinLog frames that exceed payload length still deliver as
// one message. The consensus control twin on stream 105 skips fragmented
// frames: those producers claim under the payload bound.
//
// Behaviour: one Poll is one poller Poll through the assembler handler. Each
// whole message reaches onFragment as a view of the assembled payload span, at
// offset 0. Unfragmented frames pass through without a copy. Fragmented trains
// reassemble and deliver once at END.
//
// Buffer ownership: the slice passed to onFragment is valid only for the
// duration of the callback. Unfragmented deliveries are zero-copy views of the
// driver's term buffer. Reassembled deliveries use an assembler-owned buffer.
// Callers must decode inside the callback and must not retain, mutate, or
// append to the slice. This differs from [QueueControlIngress], which passes a
// buffer the caller enqueued and still owns.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe from
// other goroutines.
//
// Lifetime: the caller owns the subscription and closes it. This type closes
// nothing. Call Clear when the ingress is unbound.
//
// Do not copy. The assembler and fragment handler bind to the construction
// receiver. A value copy has its own onFragment field, so Poll on the copy sets
// one field and handleBytes reads the other. Pass [*SubscriptionControlIngress]
// only.
type SubscriptionControlIngress struct {
	poller    ControlPoller
	assembler *aeron.FragmentAssembler
	// handler is the assembler FragmentHandler, cached at construction so
	// Poll does not allocate a method value each duty cycle.
	handler aeron.FragmentHandler

	// onFragment is set for the duration of one Poll and cleared on return.
	onFragment func(buf []byte, offset int)
}

var _ ControlIngress = (*SubscriptionControlIngress)(nil)

// NewSubscriptionControlIngress returns an ingress over poller.
//
// The assembler and handler bind once here, not per Poll. A nil poller is
// allowed and makes every Poll report zero.
//
// Must use NewSubscriptionControlIngress only. A zero-value has a nil
// handler; Poll then reports zero work and does not deliver messages even
// when poller is non-nil. That is silent incorrect use, not a panic.
func NewSubscriptionControlIngress(poller ControlPoller) *SubscriptionControlIngress {
	s := &SubscriptionControlIngress{poller: poller}
	s.assembler = aeron.NewFragmentAssembler(aeron.AdaptFragmentHandlerBytes(s.handleBytes))
	s.handler = s.assembler.Handler()
	return s
}

// Poll drains up to fragmentLimit committed fragments through the assembler
// and calls onFragment once per whole message.
//
// Returns the fragment count the poller reported. The count includes fragments
// consumed into an in-progress reassembly as work: the stream position advanced
// past those frames either way.
//
// Returns 0 without polling when the receiver is nil, the poller is nil, the
// cached handler is nil, onFragment is nil, or fragmentLimit is not positive.
// Shared early returns match [QueueControlIngress]. A nil poller or a nil
// handler (zero-value construction) is the extra seam check and also returns
// 0 without polling.
//
// Concurrency: invoker-only. Reentrant Poll from inside onFragment is
// unsupported. Reentrant Poll would overwrite the in-flight handler.
func (s *SubscriptionControlIngress) Poll(fragmentLimit int, onFragment func([]byte, int)) int {
	if s == nil || s.poller == nil || s.handler == nil || onFragment == nil || fragmentLimit <= 0 {
		return 0
	}
	s.onFragment = onFragment

	// Deferred clear, not an inline func: Image.Poll panic recovery is conditional
	// on a subscriber error handler, and a fake poller has none. A skipped clear
	// would retain the caller closure. A named method avoids a per-Poll heap
	// allocation.
	defer s.clearOnFragment()

	return s.poller.Poll(s.handler, fragmentLimit)
}

func (s *SubscriptionControlIngress) clearOnFragment() {
	s.onFragment = nil
}

// handleBytes translates one assembler delivery into an onFragment call.
//
// Whole messages only. The [aeron.FragmentAssembler] delivers unfragmented
// frames and completed reassemblies. payload is the span
// [aeron.AdaptFragmentHandlerBytes] already bounds-checked; nil marks an
// invalid or empty span and is skipped, so a decoder cannot read into the next
// frame. Delivery is at offset 0.
func (s *SubscriptionControlIngress) handleBytes(payload []byte, _ *aeron.Header) {
	if s.onFragment == nil || payload == nil {
		return
	}
	s.onFragment(payload, 0)
}

// FreeSessionBuffer drops the reassembly builder for sessionID, in progress
// or idle.
//
// Behaviour: returns true when a builder was present and removed. Returns false
// when no builder existed for sessionID. An idle builder keeps its payload
// capacity until this call or Clear.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe from
// other goroutines.
//
// A nil receiver returns false.
func (s *SubscriptionControlIngress) FreeSessionBuffer(sessionID int32) bool {
	if s == nil || s.assembler == nil {
		return false
	}
	return s.assembler.FreeSessionBuffer(sessionID)
}

// Clear drops every reassembly builder, in progress or idle.
//
// Behaviour: after Clear, FreeSessionBuffer returns false for every session id
// until a new BEGIN arrives.
//
// Concurrency: invoker-only. Same goroutine as Poll and the service agent duty
// cycle.
//
// A nil receiver is a silent no-op.
func (s *SubscriptionControlIngress) Clear() {
	if s == nil || s.assembler == nil {
		return
	}
	s.assembler.Clear()
}
