//go:build bench

package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/atomicbuffer"
)

type inMemBenchTermLog struct {
	meta             atomicbuffer.AtomicBuffer
	termBuffers      [logbuffer.PartitionCount]atomicbuffer.AtomicBuffer
	termBufferLength int
}

func newInMemBenchTermLog(
	meta atomicbuffer.AtomicBuffer,
	termBuffers [logbuffer.PartitionCount]atomicbuffer.AtomicBuffer,
	termBufferLength int,
) inMemBenchTermLog {
	return inMemBenchTermLog{
		meta:             meta,
		termBuffers:      termBuffers,
		termBufferLength: termBufferLength,
	}
}

func benchConfigWithModifiedInitialTerm(
	defaultCfg func() inMemPublicationLogConfig,
) inMemPublicationLogConfig {
	cfg := defaultCfg()
	cfg.InitialTermID = inMemPublicationInitialTermID + 11
	cfg.RawTails[0] = logbuffer.PackRawTail(cfg.InitialTermID, 128)
	return cfg
}

func TestResetInMemPublicationTermForBench_preservesCreationConfig(t *testing.T) {
	cfg := benchConfigWithModifiedInitialTerm(DefaultBenchPublicationConfig)
	pub := NewInMemPublicationForBench(cfg, BenchPublicationLimitPosition)
	log := newInMemBenchTermLog(pub.meta, pub.termBuffers, pub.termBufferLength)
	require.Equal(t, cfg.InitialTermID, log.meta.GetInt32(logbuffer.OffsetInitialTermID))

	ResetInMemPublicationTermForBench(pub)

	requireInMemBenchTermMetadataRestored(t, log, cfg)
}

func TestResetInMemExclusivePublicationTermForBench_preservesCreationConfig(t *testing.T) {
	cfg := benchConfigWithModifiedInitialTerm(DefaultBenchExclusivePublicationConfig)
	ep := NewInMemExclusivePublicationForBench(cfg, BenchPublicationLimitPosition)
	log := newInMemBenchTermLog(ep.meta, ep.termBuffers, ep.termBufferLength)
	require.Equal(t, cfg.InitialTermID, log.meta.GetInt32(logbuffer.OffsetInitialTermID))

	ResetInMemExclusivePublicationTermForBench(ep)

	requireInMemBenchTermMetadataRestored(t, log, cfg)
}

func TestResetInMemPublicationTermForBench_allowsOfferAfterTermFill(t *testing.T) {
	cfg := DefaultBenchPublicationConfig()
	pub := NewInMemPublicationForBench(cfg, BenchPublicationLimitPosition)
	log := newInMemBenchTermLog(pub.meta, pub.termBuffers, pub.termBufferLength)
	payload := make([]byte, 64)

	requireInMemBenchTermFilled(t, log, func() int64 { return pub.Offer(payload) })
	ResetInMemPublicationTermForBench(pub)
	requireInMemBenchTermRestored(t, log, cfg)

	pos := pub.Offer(payload)
	require.Positive(t, pos)
}

func TestResetInMemPublicationTermForBench_pairOfferPollAfterFill(t *testing.T) {
	cfg := DefaultBenchPublicationConfig()
	pub, image := NewInMemPublicationImagePairForBench(cfg, BenchPublicationLimitPosition)
	log := newInMemBenchTermLog(pub.meta, pub.termBuffers, pub.termBufferLength)
	payload := []byte{1, 2, 3, 4}

	requireInMemBenchTermFilled(t, log, func() int64 { return pub.Offer(payload) })

	ResetInMemPublicationTermForBench(pub)
	SetImageSubscriberPositionForBench(image, 0)

	pos := pub.Offer(payload)
	require.Positive(t, pos)

	var polledLength int
	polled := image.Poll(func(
		_ atomicbuffer.AtomicBuffer,
		_ int,
		length int,
		_ *Header,
	) {
		polledLength = length
	}, 1)
	require.Equal(t, 1, polled)
	require.Equal(t, len(payload), polledLength)
}

func TestResetInMemExclusivePublicationTermForBench_allowsOfferAfterTermFill(t *testing.T) {
	cfg := DefaultBenchExclusivePublicationConfig()
	ep := NewInMemExclusivePublicationForBench(cfg, BenchPublicationLimitPosition)
	log := newInMemBenchTermLog(ep.meta, ep.termBuffers, ep.termBufferLength)
	payload := make([]byte, 64)

	resetInMemBenchExclusiveTermAfterFill(t, ep, log, cfg, func() int64 { return ep.Offer(payload) })

	pos := ep.Offer(payload)
	require.Positive(t, pos)
}

func TestResetInMemExclusivePublicationTermForBench_allowsTryClaimCommitAfterTermFill(
	t *testing.T,
) {
	cfg := DefaultBenchExclusivePublicationConfig()
	ep := NewInMemExclusivePublicationForBench(cfg, BenchPublicationLimitPosition)
	log := newInMemBenchTermLog(ep.meta, ep.termBuffers, ep.termBufferLength)
	payload := make([]byte, 32)

	resetInMemBenchExclusiveTermAfterFill(t, ep, log, cfg, func() int64 { return ep.Offer(payload) })

	termIndex, frameOffset, position, err := ep.TryClaim(len(payload))
	require.NoError(t, err)
	require.Positive(t, position)

	termBuf := ep.termBuffers[termIndex]
	termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, payload)
	require.NoError(t, ep.Commit(termIndex, frameOffset))
}

func pollBenchFragment(poll func(FragmentHandler, int) int) (fragments int, delivered []byte) {
	fragments = poll(AdaptFragmentHandlerBytes(func(payload []byte, _ *Header) {
		delivered = append(delivered, payload...)
	}), 10)
	return fragments, delivered
}

func TestNewInMemSubscriptionForBench_pollDeliversPayload(t *testing.T) {
	payload := []byte("bench-subscription")
	sub, image := NewInMemSubscriptionForBench(payload)
	require.Equal(t, []*Image{image}, sub.imagesSnapshot())

	// First poll: the frame reaches the handler.
	fragments, delivered := pollBenchFragment(sub.Poll)
	require.Equal(t, 1, fragments)
	require.Equal(t, payload, delivered)

	// Poll at the tail: the image holds one frame only.
	fragments, delivered = pollBenchFragment(sub.Poll)
	require.Equal(t, 0, fragments)
	require.Empty(t, delivered)

	// Poll after the rewind: the same frame reaches the handler again.
	SetImageSubscriberPositionForBench(image, 0)
	fragments, delivered = pollBenchFragment(sub.Poll)
	require.Equal(t, 1, fragments)
	require.Equal(t, payload, delivered)
}

func TestDefaultBenchPollImageForBench_deliversZeroAllocPayload(t *testing.T) {
	fragments, delivered := pollBenchFragment(DefaultBenchPollImageForBench().Poll)
	require.Equal(t, 1, fragments)
	require.Equal(t, inMemPollZeroAllocPayloadBytes, delivered)
}

func TestBenchFixture_nilInputs(t *testing.T) {
	tests := []struct {
		name string
		fn   func()
	}{
		{name: "publication reset", fn: func() { ResetInMemPublicationTermForBench(nil) }},
		{
			name: "exclusive publication reset",
			fn:   func() { ResetInMemExclusivePublicationTermForBench(nil) },
		},
		{name: "image", fn: func() { NewInMemImageForBench(nil) }},
		{name: "image subscriber position", fn: func() { SetImageSubscriberPositionForBench(nil, 0) }},
		{name: "offer retry", fn: func() { OfferRetryForBench(nil, nil) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Panics(t, tt.fn)
		})
	}
}

func TestBenchFixture_untrackedPublicationsPanic(t *testing.T) {
	tests := []struct {
		name string
		fn   func()
	}{
		{
			name: "publication",
			fn: func() {
				cfg := DefaultBenchPublicationConfig()
				log := newInMemMappedLog(cfg)
				pub := newInMemPublicationFromLog(
					log,
					inMemCountersReaderWithoutTB(BenchPublicationLimitPosition),
				)
				ResetInMemPublicationTermForBench(pub)
			},
		},
		{
			name: "exclusive publication",
			fn: func() {
				cfg := DefaultBenchExclusivePublicationConfig()
				log := newInMemMappedLog(cfg)
				ep := newInMemExclusivePublicationFromLog(
					log,
					inMemCountersReaderWithoutTB(BenchPublicationLimitPosition),
				)
				ResetInMemExclusivePublicationTermForBench(ep)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Panics(t, tt.fn)
		})
	}
}

func fillUntilNegativeOffer(offer func() int64) int64 {
	var last int64
	for range 10_000 {
		last = offer()
		if last < 0 {
			break
		}
	}
	return last
}

func mustInMemBenchRawTail(t *testing.T, log inMemBenchTermLog, partition int) int64 {
	t.Helper()

	rawTail, err := logbuffer.GetRawTail(log.meta, partition)
	require.NoError(t, err)
	return rawTail
}

func requireInMemBenchTermFilled(t *testing.T, log inMemBenchTermLog, offer func() int64) {
	t.Helper()

	last := fillUntilNegativeOffer(offer)
	require.Negative(t, last)
	require.NotZero(t, log.termBuffers[0].GetInt32Volatile(0))
}

func resetInMemBenchExclusiveTermAfterFill(
	t *testing.T,
	ep *ExclusivePublication,
	log inMemBenchTermLog,
	cfg inMemPublicationLogConfig,
	offer func() int64,
) {
	t.Helper()

	requireInMemBenchTermFilled(t, log, offer)
	ResetInMemExclusivePublicationTermForBench(ep)
	requireInMemBenchExclusiveTermRestored(t, log, ep, cfg)
}

func requireInMemBenchTermMetadataRestored(
	t *testing.T,
	log inMemBenchTermLog,
	cfg inMemPublicationLogConfig,
) {
	t.Helper()

	require.Equal(t, cfg.InitialTermID, log.meta.GetInt32(logbuffer.OffsetInitialTermID))
	require.Equal(t, int32(0), logbuffer.ActiveTermCount(log.meta))
	for partition := range logbuffer.PartitionCount {
		require.Equal(t, cfg.RawTails[partition], mustInMemBenchRawTail(t, log, partition))
	}
}

func requireInMemBenchTermRestored(
	t *testing.T,
	log inMemBenchTermLog,
	cfg inMemPublicationLogConfig,
) {
	t.Helper()

	requireInMemBenchTermMetadataRestored(t, log, cfg)
	requireInMemBenchTermBuffersCleared(t, log)
}

func requireInMemBenchExclusiveTermRestored(
	t *testing.T,
	log inMemBenchTermLog,
	ep *ExclusivePublication,
	cfg inMemPublicationLogConfig,
) {
	t.Helper()

	requireInMemBenchTermMetadataRestored(t, log, cfg)
	requireExclusiveWriterCacheMatchesConfig(t, ep, cfg)
	requireInMemBenchTermBuffersCleared(t, log)
}

func requireExclusiveWriterCacheMatchesConfig(
	t *testing.T,
	ep *ExclusivePublication,
	cfg inMemPublicationLogConfig,
) {
	t.Helper()

	activePartitionIndex, termID, termOffset, termBeginPosition := exclusiveWriterCacheFromConfig(
		cfg,
		ep.termBufferLength,
		ep.positionBitsToShift,
	)
	require.Equal(t, activePartitionIndex, ep.activePartitionIndex)
	require.Equal(t, termID, ep.termID)
	require.Equal(t, termOffset, ep.termOffset)
	require.Equal(t, termBeginPosition, ep.termBeginPosition)
}

func requireInMemBenchTermBuffersCleared(t *testing.T, log inMemBenchTermLog) {
	t.Helper()

	for partition := range logbuffer.PartitionCount {
		termBuf := log.termBuffers[partition]
		for offset := range log.termBufferLength {
			if termBuf.Bytes()[offset] != 0 {
				require.Failf(
					t,
					"term buffer not cleared",
					"partition=%d offset=%d",
					partition,
					offset,
				)
			}
		}
	}
}
