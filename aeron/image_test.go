package aeron_test

import (
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/conductor"
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/atomicbuffer"
)

func TestImage_newImage_recordsJoinPosition(t *testing.T) {
	sub := testSubscription(t)
	image := aeron.NewImageForTest(t, sub, aeron.TestSubscriberPositionAtJoin)

	require.Equal(t, aeron.TestSubscriberPositionAtJoin, image.JoinPosition())
	require.Equal(t, aeron.TestSubscriberPositionAtJoin, image.Position())
}

func TestImage_accessorsReturnWiredFields(t *testing.T) {
	sub := testSubscription(t)
	image := aeron.NewImageForTest(t, sub, aeron.TestSubscriberPositionAtJoin)

	require.Equal(t, aeron.TestImageCorrelationID, image.CorrelationID())
	require.Equal(t, aeron.TestPublicationSessionID, image.SessionID())
	require.Equal(t, aeron.TestPublicationStreamID, image.StreamID())
	require.Equal(t, aeron.TestImageSourceIdentity, image.SourceIdentity())
	require.Equal(t, "test-image.log", image.LogFileName())
	require.Equal(t, aeron.TestSubscriberPositionCounterID, image.SubscriberPositionID())
	require.False(t, image.IsClosed())
}

func TestImage_Position_tracksSubscriberCounter(t *testing.T) {
	sub := testSubscription(t)
	image := aeron.NewImageForTest(t, sub, aeron.TestSubscriberPositionAtJoin)

	const updatedPosition int64 = 512
	aeron.ImageSetSubscriberPositionForTest(image, updatedPosition)

	require.Equal(t, aeron.TestSubscriberPositionAtJoin, image.JoinPosition())
	require.Equal(t, updatedPosition, image.Position())
}

func TestImage_internalClose_closedSemantics(t *testing.T) {
	sub := testSubscription(t)
	image := aeron.NewImageForTest(t, sub, aeron.TestSubscriberPositionAtJoin)

	const positionAtClose int64 = 256
	aeron.ImageSetSubscriberPositionForTest(image, positionAtClose)

	aeron.ImageHeaderForTest(image).SetBuffer(aeron.ImageTermBuffer0ForTest(image))
	termCapacity := aeron.ImageTermBuffer0CapacityForTest(image)
	headerCapacity := aeron.ImageHeaderBufferCapacityForTest(image)
	require.Positive(t, termCapacity)
	require.Positive(t, headerCapacity)

	aeron.ImageInternalCloseForTest(image)

	require.True(t, image.IsClosed())
	require.Equal(t, positionAtClose, image.Position())
	require.Equal(t, termCapacity, aeron.ImageTermBuffer0CapacityForTest(image))
	require.Equal(t, headerCapacity, aeron.ImageHeaderBufferCapacityForTest(image))

	aeron.ImageSetSubscriberPositionForTest(image, 512)
	require.Equal(t, positionAtClose, image.Position())
	require.Equal(t, 0, image.Poll(nil, 1))
}

func TestImage_endOfStream(t *testing.T) {
	const noWrite int64 = -1

	tests := []struct {
		name            string
		endOfStreamWord int64
		position        int64
		closed          bool
		laterPosition   int64
		wantEndOfStream bool
		wantPosition    int64
	}{
		{
			name:            "defaultWord",
			endOfStreamWord: noWrite,
			position:        256,
			laterPosition:   noWrite,
			wantEndOfStream: false,
			wantPosition:    math.MaxInt64,
		},
		{
			name:            "wordBelowPosition",
			endOfStreamWord: 128,
			position:        256,
			laterPosition:   noWrite,
			wantEndOfStream: true,
			wantPosition:    128,
		},
		{
			name:            "wordAtPosition",
			endOfStreamWord: 256,
			position:        256,
			laterPosition:   noWrite,
			wantEndOfStream: true,
			wantPosition:    256,
		},
		{
			name:            "wordAbovePosition",
			endOfStreamWord: 512,
			position:        256,
			laterPosition:   noWrite,
			wantEndOfStream: false,
			wantPosition:    512,
		},
		{
			name:            "positionMovesToWordWhileOpen",
			endOfStreamWord: 256,
			position:        128,
			laterPosition:   256,
			wantEndOfStream: true,
			wantPosition:    256,
		},
		{
			name:            "closedAfterPositionReachedWord",
			endOfStreamWord: 256,
			position:        256,
			closed:          true,
			laterPosition:   noWrite,
			wantEndOfStream: true,
			wantPosition:    256,
		},
		{
			name:            "closedBeforePositionReachedWord",
			endOfStreamWord: 512,
			position:        256,
			closed:          true,
			laterPosition:   noWrite,
			wantEndOfStream: false,
			wantPosition:    512,
		},
		{
			name:            "counterWriteAfterCloseChangesNeither",
			endOfStreamWord: 512,
			position:        256,
			closed:          true,
			laterPosition:   1024,
			wantEndOfStream: false,
			wantPosition:    512,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			image := aeron.NewImageForTest(t, testSubscription(t), tc.position)
			if tc.endOfStreamWord != noWrite {
				aeron.ImageSetEndOfStreamPositionForTest(image, tc.endOfStreamWord)
			}
			if tc.closed {
				aeron.ImageInternalCloseForTest(image)
			}
			if tc.laterPosition != noWrite {
				aeron.ImageSetSubscriberPositionForTest(image, tc.laterPosition)
			}

			require.Equal(t, tc.closed, image.IsClosed())
			require.Equal(t, tc.wantEndOfStream, image.IsEndOfStream())
			require.Equal(t, tc.wantPosition, image.EndOfStreamPosition())
		})
	}
}

func TestImage_activeTransportCount(t *testing.T) {
	tests := []struct {
		name   string
		count  int32
		closed bool
		want   int32
	}{
		{name: "defaultWord", want: 0},
		{name: "wordTwo", count: 2, want: 2},
		{name: "closedWithWordTwo", count: 2, closed: true, want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			image := aeron.NewImageForTest(t, testSubscription(t), 0)
			aeron.ImageSetActiveTransportCountForTest(image, tc.count)
			if tc.closed {
				aeron.ImageInternalCloseForTest(image)
			}

			require.Equal(t, tc.want, image.ActiveTransportCount())
		})
	}
}

func TestImage_internalClose_idempotent(t *testing.T) {
	sub := testSubscription(t)
	image := aeron.NewImageForTest(t, sub, 100)

	aeron.ImageSetSubscriberPositionForTest(image, 100)
	aeron.ImageInternalCloseForTest(image)

	aeron.ImageSetSubscriberPositionForTest(image, 200)
	aeron.ImageInternalCloseForTest(image)

	require.Equal(t, int64(100), image.Position())
}

func TestImage_Header_readsCommittedDataFrame(t *testing.T) {
	t.Run("zeroTermOffset", func(t *testing.T) {
		image, frameLength := aeron.NewImageWithCommittedDataFrameForTest(t, 0, 0)

		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		header := aeron.ImageHeaderForTest(image)
		header.SetBuffer(termBuffer)
		header.SetOffset(0)

		require.Equal(t, frameLength, header.FrameLength())
		require.Equal(t, aeron.TestPublicationSessionID, header.SessionID())
		require.Equal(t, aeron.TestPublicationStreamID, header.StreamID())
		require.Equal(t, aeron.TestPublicationInitialTermID, header.TermID())
		require.Equal(t, int32(0), header.TermOffset())
		require.Equal(t, int32(logbuffer.HdrTypeData), header.Type())
		require.Equal(t, byte(logbuffer.Unfragmented), header.Flags())
		require.Equal(t, int64(logbuffer.DefaultReservedValue), header.ReservedValue())
		require.Equal(t, 0, header.Offset())
		require.Equal(
			t,
			int64(logbuffer.AlignFrameLength(int(frameLength))),
			header.Position(),
		)
	})

	t.Run("nonZeroTermOffset", func(t *testing.T) {
		const frameOffset = 64
		image, frameLength := aeron.NewImageWithCommittedDataFrameForTest(t, 0, frameOffset)

		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		header := aeron.ImageHeaderForTest(image)
		header.SetBuffer(termBuffer)
		header.SetOffset(frameOffset)

		expectedPosition := int64(
			logbuffer.AlignFrameLength(int(frameOffset) + int(frameLength)),
		)
		require.Equal(t, expectedPosition, header.Position())
		require.Equal(t, int32(frameOffset), header.TermOffset())
		require.Equal(
			t,
			int32(logbuffer.AlignFrameLength(int(frameOffset)+int(frameLength))),
			header.NextTermOffset(),
		)
	})
}

func TestImage_newImage_errors(t *testing.T) {
	sub := testSubscription(t)
	event := aeron.AvailableImageForTest()

	logBuffers := aeron.NewTestLogBuffersForTest(t, aeron.DefaultTestPublicationLogConfig())

	activeCounters := aeron.NewTestCountersReaderWithSubscriberPositionForTest(
		t,
		aeron.TestSubscriberPositionAtJoin,
	)

	closedLog := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestPublicationLogConfig())
	closedLogBuffers, err := logbuffer.NewLogBuffers(closedLog)
	require.NoError(t, err)
	require.NoError(t, closedLog.Close())

	inactiveCounters := aeron.NewTestCountersReaderLimitOnlyForTest(t, 0)

	tests := []struct {
		name         string
		sub          *aeron.Subscription
		logBuffers   *logbuffer.LogBuffers
		counters     *cnc.CountersReader
		event        conductor.AvailableImage
		wantContains string
	}{
		{
			name:         "nilSubscription",
			sub:          nil,
			logBuffers:   logBuffers,
			counters:     activeCounters,
			event:        event,
			wantContains: "subscription is nil",
		},
		{
			name:         "nilLogBuffers",
			sub:          sub,
			logBuffers:   nil,
			counters:     activeCounters,
			event:        event,
			wantContains: "log buffers is nil",
		},
		{
			name:         "nilCountersReader",
			sub:          sub,
			logBuffers:   logBuffers,
			counters:     nil,
			event:        event,
			wantContains: "counters reader is nil",
		},
		{
			name:         "inactiveSubscriberCounter",
			sub:          sub,
			logBuffers:   logBuffers,
			counters:     inactiveCounters,
			event:        event,
			wantContains: "subscriber position counter",
		},
		{
			name:         "outOfRangeSubscriberCounter",
			sub:          sub,
			logBuffers:   logBuffers,
			counters:     activeCounters,
			event:        aeron.AvailableImageWithSubscriberPositionIDForTest(99),
			wantContains: "subscriber position counter",
		},
		{
			name:         "closedLogBuffers",
			sub:          sub,
			logBuffers:   closedLogBuffers,
			counters:     activeCounters,
			event:        event,
			wantContains: "closed mapped log buffer",
		},
		{
			name:       "mismatchedSubscriberCounterRegistration",
			sub:        sub,
			logBuffers: logBuffers,
			counters: aeron.NewTestCountersReaderWithSubscriberPositionForTest(
				t,
				aeron.TestSubscriberPositionAtJoin,
				aeron.TestSubscriptionRegistrationID+1,
			),
			event:        event,
			wantContains: "subscriber position counter",
		},
		{
			name:       "mismatchedSubscriptionRegistration",
			sub:        sub,
			logBuffers: logBuffers,
			counters:   activeCounters,
			event: func() conductor.AvailableImage {
				mismatch := event
				mismatch.SubscriptionRegistrationID = aeron.TestSubscriptionRegistrationID + 99
				return mismatch
			}(),
			wantContains: "subscription registration",
		},
		{
			name:       "mismatchedSubscriptionStream",
			sub:        sub,
			logBuffers: logBuffers,
			counters:   activeCounters,
			event: func() conductor.AvailableImage {
				mismatch := event
				mismatch.StreamID = aeron.TestPublicationStreamID + 99
				return mismatch
			}(),
			wantContains: "subscription stream",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := aeron.NewImageDirectForTest(
				tc.sub,
				tc.event,
				tc.logBuffers,
				tc.counters,
			)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantContains)
		})
	}
}

func TestImage_newImage_metadataErrors(t *testing.T) {
	sub := testSubscription(t)

	tests := []struct {
		name         string
		mutate       func(atomicbuffer.AtomicBuffer)
		wantContains string
	}{
		{
			name: "parseMetadataFailure",
			mutate: func(meta atomicbuffer.AtomicBuffer) {
				meta.PutInt32(logbuffer.OffsetPageSize, 3)
			},
			wantContains: "page size",
		},
		{
			name: "metadataTermLengthMismatch",
			mutate: func(meta atomicbuffer.AtomicBuffer) {
				meta.PutInt32(logbuffer.OffsetTermLength, aeron.TestPublicationTermLength*2)
			},
			wantContains: "metadata term length",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logBuffers := aeron.NewTestLogBuffersForTest(t, aeron.DefaultTestPublicationLogConfig())

			meta, err := logBuffers.MetaDataBuffer()
			require.NoError(t, err)
			tc.mutate(meta)

			counters := aeron.NewTestCountersReaderWithSubscriberPositionForTest(
				t,
				aeron.TestSubscriberPositionAtJoin,
			)

			_, err = aeron.NewImageDirectForTest(sub, aeron.AvailableImageForTest(), logBuffers, counters)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantContains)
		})
	}
}

func TestImage_Poll_singleDataFrame(t *testing.T) {
	image, capture := newPollImage(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	payload := []byte("hello")
	writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, payload)

	fragments := image.Poll(capture.handler, 1)

	require.Equal(t, 1, fragments)
	require.Len(t, capture.payloads, 1)
	require.Equal(t, payload, capture.payloads[0])
	require.Len(t, capture.headers, 1)
	require.Equal(t, int32(logbuffer.HdrTypeData), capture.headers[0].Type())
	require.Equal(t, int64(logbuffer.AlignFragmentChunkLength(len(payload))), image.Position())
}

func TestImage_Poll_multipleFrames(t *testing.T) {
	image, capture := newPollImage(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	payload := []byte("x")
	const frameCount = 5
	endOffset := writeDataFrameChain(t, termBuffer, 0, frameCount, payload)

	fragments := image.Poll(capture.handler, frameCount)

	require.Equal(t, frameCount, fragments)
	require.Len(t, capture.payloads, frameCount)
	for _, got := range capture.payloads {
		require.Equal(t, payload, got)
	}
	require.Equal(t, int64(endOffset), image.Position())
}

func TestImage_Poll_stopsOnUncommitted(t *testing.T) {
	payload := []byte("committed")
	nextOffset := logbuffer.AlignFragmentChunkLength(len(payload))

	tests := []struct {
		name      string
		blockNext func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer)
	}{
		{
			name: "zeroFrameLength",
			blockNext: func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer) {
				require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuffer, nextOffset))
			},
		},
		{
			name: "negativeFrameLength",
			blockNext: func(_ *testing.T, termBuffer atomicbuffer.AtomicBuffer) {
				logbuffer.PutFrameLengthRelease(termBuffer, nextOffset, -int32(nextOffset))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			image, capture := newPollImage(t, 0)
			termBuffer := aeron.ImageTermBuffer0ForTest(image)
			writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, payload)
			tc.blockNext(t, termBuffer)

			fragments := image.Poll(capture.handler, 10)

			require.Equal(t, 1, fragments)
			require.Equal(t, int64(nextOffset), image.Position())
		})
	}
}

func TestImage_Poll_advancesSubscriberCounter(t *testing.T) {
	const joinPosition int64 = 0
	sub := testSubscription(t)
	logBuffers := aeron.NewTestLogBuffersForTest(t, aeron.DefaultTestPublicationLogConfig())
	counters := aeron.NewTestCountersReaderWithSubscriberPositionForTest(t, joinPosition)
	image, err := aeron.NewImageDirectForTest(sub, aeron.AvailableImageForTest(), logBuffers, counters)
	require.NoError(t, err)
	capture := &pollCapture{}
	termBuffer := aeron.ImageTermBuffer0ForTest(image)

	payload := []byte("counter")
	writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, payload)
	advance := int64(logbuffer.AlignFragmentChunkLength(len(payload)))

	fragments := image.Poll(capture.handler, 1)

	require.Equal(t, 1, fragments)
	gotCounter, err := counters.CounterValue(aeron.TestSubscriberPositionCounterID)
	require.NoError(t, err)
	require.Equal(t, joinPosition+advance, gotCounter)
	require.Equal(t, joinPosition+advance, image.Position())
}

func TestImage_Poll_resetsFragmentedFrameLen(t *testing.T) {
	image, capture := newPollImage(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	payload := []byte("payload")
	writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, payload)

	expectedNextTermOffset := int32(logbuffer.AlignFragmentChunkLength(len(payload)))
	expectedPosition := int64(expectedNextTermOffset)

	aeron.HeaderSetFragmentedFrameLenForTest(aeron.ImageHeaderForTest(image), 256)

	fragments := image.Poll(func(
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	) {
		require.Equal(t, expectedNextTermOffset, header.NextTermOffset())
		require.Equal(t, expectedPosition, header.Position())
		capture.handler(buffer, offset, length, header)
	}, 1)

	require.Equal(t, 1, fragments)
	require.Equal(t, payload, capture.payloads[0])
	require.Equal(t, expectedPosition, image.Position())
}

func TestImage_Poll_joinPositionImmutable(t *testing.T) {
	const joinPosition int64 = 0
	image, capture := newPollImage(t, joinPosition)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, []byte("x"))

	require.Equal(t, joinPosition, image.JoinPosition())
	_ = image.Poll(capture.handler, 1)
	require.Equal(t, joinPosition, image.JoinPosition())
}

func TestImage_Poll_closedReturnsZero(t *testing.T) {
	image, capture := newPollImage(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, []byte("closed"))

	positionBeforeClose := image.Position()
	aeron.ImageInternalCloseForTest(image)

	require.True(t, image.IsClosed())
	require.Equal(t, 0, image.Poll(capture.handler, 1))
	require.Empty(t, capture.payloads)
	require.Equal(t, positionBeforeClose, image.Position())
}

func TestImage_Poll_fragmentLimit(t *testing.T) {
	image, capture := newPollImage(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	const totalFrames = 10
	const fragmentLimit = 3
	payload := []byte("a")
	frameSpan := logbuffer.AlignFragmentChunkLength(len(payload))
	writeDataFrameChain(t, termBuffer, 0, totalFrames, payload)

	fragments := image.Poll(capture.handler, fragmentLimit)

	require.Equal(t, fragmentLimit, fragments)
	require.Len(t, capture.payloads, fragmentLimit)
	require.Equal(t, int64(frameSpan*fragmentLimit), image.Position())
}

func TestImage_Poll_termEnd(t *testing.T) {
	const termLength = aeron.TestPublicationTermLength
	const lastFrameOffset = termLength - pollTestTailFrameLength
	image, capture := newPollImage(t, int64(lastFrameOffset))
	term0 := aeron.ImageTermBuffer0ForTest(image)
	payload := []byte("term0-tail")
	writePollTestDataFrame(
		term0,
		lastFrameOffset,
		int32(lastFrameOffset),
		logbuffer.Unfragmented,
		payload,
	)

	fragments := image.Poll(capture.handler, 1)

	require.Equal(t, 1, fragments)
	require.Equal(t, payload, capture.payloads[0])
	require.Equal(t, int64(termLength), image.Position())

	term1 := aeron.ImageTermBufferForTest(image, 1)
	nextPayload := []byte("term1-head")
	aeron.WriteCommittedDataFrameForTest(
		term1,
		0,
		aeron.TestPublicationSessionID,
		aeron.TestPublicationStreamID,
		aeron.TestPublicationInitialTermID+1,
		0,
		logbuffer.Unfragmented,
		nextPayload,
	)

	capture.reset()
	fragments = image.Poll(capture.handler, 1)

	require.Equal(t, 1, fragments)
	require.Equal(t, nextPayload, capture.payloads[0])
	require.Equal(
		t,
		int64(termLength)+int64(logbuffer.AlignFragmentChunkLength(len(nextPayload))),
		image.Position(),
	)
}

func TestImage_Poll_nilHandlerAndZeroLimit(t *testing.T) {
	image, capture := newPollImage(t, 0)
	writePollTestPaddingThenData(
		aeron.ImageTermBuffer0ForTest(image),
		pollTestPaddingLength,
		[]byte("x"),
	)

	for _, tc := range []struct {
		name    string
		handler func(atomicbuffer.AtomicBuffer, int, int, *aeron.Header)
		limit   int
	}{
		{name: "nilHandler", handler: nil, limit: 1},
		{name: "zeroLimit", handler: capture.handler, limit: 0},
		{name: "negativeLimit", handler: capture.handler, limit: -1},
	} {
		require.Equal(t, 0, image.Poll(tc.handler, tc.limit), tc.name)
		require.Zero(t, image.Position(), tc.name)
	}
}

func TestImage_BoundedPoll_bounds(t *testing.T) {
	payload := []byte("a")
	frameSpan := logbuffer.AlignFragmentChunkLength(len(payload))
	require.GreaterOrEqual(t, frameSpan, 2)

	const termLength = aeron.TestPublicationTermLength
	tailPayload := []byte("tail")
	tailSpan := logbuffer.AlignFragmentChunkLength(len(tailPayload))
	tailOffset := termLength - 2*tailSpan
	require.GreaterOrEqual(t, tailOffset, 0)

	writeTwoFramesAtZero := func(t *testing.T, image *aeron.Image) {
		t.Helper()
		writeDataFrameChain(t, aeron.ImageTermBuffer0ForTest(image), 0, 2, payload)
	}

	tests := []struct {
		name          string
		joinPosition  int64
		limitPosition int64
		setup         func(t *testing.T, image *aeron.Image)
		wantFragments int
		wantPayloads  [][]byte
		wantPosition  int64
	}{
		{
			name:          "positionAtLimit",
			joinPosition:  0,
			limitPosition: 0,
			setup: func(t *testing.T, image *aeron.Image) {
				t.Helper()
				writePollTestPayloads(aeron.ImageTermBuffer0ForTest(image), 0, []byte("x"))
			},
			wantPosition: 0,
		},
		{
			name:          "positionPastLimit",
			joinPosition:  128,
			limitPosition: 96,
			setup: func(t *testing.T, image *aeron.Image) {
				t.Helper()
				writePollTestPayloads(aeron.ImageTermBuffer0ForTest(image), 128, []byte("x"))
			},
			wantPosition: 128,
		},
		{
			name:          "limitInsideFrame",
			joinPosition:  0,
			limitPosition: int64(frameSpan / 2),
			setup:         writeTwoFramesAtZero,
			wantFragments: 1,
			wantPayloads:  [][]byte{payload},
			wantPosition:  int64(frameSpan),
		},
		{
			name:          "limitAtFrameStart",
			joinPosition:  0,
			limitPosition: int64(frameSpan),
			setup:         writeTwoFramesAtZero,
			wantFragments: 1,
			wantPayloads:  [][]byte{payload},
			wantPosition:  int64(frameSpan),
		},
		{
			name:          "limitPastTermEnd",
			joinPosition:  int64(tailOffset),
			limitPosition: int64(tailOffset) + int64(termLength),
			setup: func(t *testing.T, image *aeron.Image) {
				t.Helper()
				termBuffer := aeron.ImageTermBuffer0ForTest(image)
				padOffset := writePollTestPayloads(termBuffer, tailOffset, tailPayload)
				aeron.WriteCommittedPaddingFrameForTest(
					termBuffer,
					padOffset,
					aeron.TestPublicationInitialTermID,
					int32(padOffset),
					int32(termLength-padOffset),
				)
			},
			wantFragments: 1,
			wantPayloads:  [][]byte{tailPayload},
			wantPosition:  int64(termLength),
		},
		{
			name:          "paddingStartsBelowLimit",
			joinPosition:  0,
			limitPosition: int64(pollTestWidePaddingLength / 2),
			setup: func(t *testing.T, image *aeron.Image) {
				t.Helper()
				aeron.WriteCommittedPaddingFrameForTest(
					aeron.ImageTermBuffer0ForTest(image),
					0,
					aeron.TestPublicationInitialTermID,
					0,
					pollTestWidePaddingLength,
				)
			},
			wantPosition: int64(pollTestWidePaddingLength),
		},
		{
			name:          "joinOffsetAboveZero",
			joinPosition:  int64(frameSpan),
			limitPosition: int64(2 * frameSpan),
			setup: func(t *testing.T, image *aeron.Image) {
				t.Helper()
				writeDataFrameChain(
					t,
					aeron.ImageTermBuffer0ForTest(image),
					frameSpan,
					2,
					payload,
				)
			},
			wantFragments: 1,
			wantPayloads:  [][]byte{payload},
			wantPosition:  int64(2 * frameSpan),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			image, capture := newPollImage(t, tc.joinPosition)
			tc.setup(t, image)

			fragments := image.BoundedPoll(capture.handler, tc.limitPosition, 10)

			require.Equal(t, tc.wantFragments, fragments)
			require.Equal(t, tc.wantPayloads, capture.payloads)
			require.Equal(t, tc.wantPosition, image.Position())
		})
	}
}

func TestImage_BoundedPoll_guards(t *testing.T) {
	image, capture := newPollImage(t, 0)
	writePollTestPayloads(aeron.ImageTermBuffer0ForTest(image), 0, []byte("x"))

	require.Equal(t, 0, image.BoundedPoll(nil, 100, 1))
	require.Equal(t, 0, image.BoundedPoll(capture.handler, 100, 0))
	require.Equal(t, 0, image.BoundedPoll(capture.handler, 100, -1))
	require.Zero(t, image.Position())
	require.Empty(t, capture.payloads)

	aeron.ImageInternalCloseForTest(image)
	require.Equal(t, 0, image.BoundedPoll(capture.handler, 100, 1))
}

func TestImage_Poll_handlerPanic(t *testing.T) {
	const wantErr = "poll handler failed"
	panicPayload := []byte("panic")
	panicSpan := int64(logbuffer.AlignFragmentChunkLength(len(panicPayload)))
	pollPanicHandler := func(atomicbuffer.AtomicBuffer, int, int, *aeron.Header) {
		panic(errors.New(wantErr))
	}

	t.Run("firstFrameWithErrorHandler", func(t *testing.T) {
		image, capture, handledErr := newPollImageWithErrorHandler(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		writePollTestPayloads(termBuffer, 0, panicPayload, []byte("after"))

		fragments := image.Poll(pollPanicHandler, 10)

		require.Equal(t, 1, fragments)
		require.Equal(t, panicSpan, image.Position())
		handled := handledErr()
		require.Error(t, handled)
		require.Equal(t, wantErr, handled.Error())

		fragments = image.Poll(capture.handler, 1)
		require.Equal(t, 1, fragments)
		require.Equal(t, []byte("after"), capture.payloads[0])
	})

	t.Run("boundedPollFirstFrameWithErrorHandler", func(t *testing.T) {
		image, _, handledErr := newPollImageWithErrorHandler(t, 0)
		writePollTestPayloads(
			aeron.ImageTermBuffer0ForTest(image),
			0,
			panicPayload,
			[]byte("after"),
		)
		limitPosition := panicSpan / 2
		require.Positive(t, limitPosition)
		require.Less(t, limitPosition, panicSpan)

		fragments := image.BoundedPoll(pollPanicHandler, limitPosition, 10)

		require.Equal(t, 1, fragments)
		require.Equal(t, panicSpan, image.Position())
		handled := handledErr()
		require.Error(t, handled)
		require.Equal(t, wantErr, handled.Error())
	})

	t.Run("fragmentedEndPanicDropsMessage", func(t *testing.T) {
		image, _, handledErr := newPollImageWithErrorHandler(t, 0)
		messageEnd := writePollTestFragmentedThenNext(aeron.ImageTermBuffer0ForTest(image))

		calls := 0
		var delivered [][]byte
		delegate := func(buffer atomicbuffer.AtomicBuffer, offset, length int, _ *aeron.Header) {
			calls++
			if calls == 1 {
				panic(errors.New(wantErr))
			}
			delivered = append(delivered, slices.Clone(buffer.Bytes()[offset:offset+length]))
		}
		handler := aeron.NewFragmentAssembler(delegate).Handler()

		// Phase 1: the delegate panics on the completed message.
		require.Equal(t, 2, image.Poll(handler, 10))
		require.Equal(t, int64(messageEnd), image.Position())
		require.Error(t, handledErr())
		require.Empty(t, delivered)

		// Phase 2: only the next message reaches the delegate.
		require.Equal(t, 1, image.Poll(handler, 10))
		require.Equal(t, [][]byte{[]byte("next")}, delivered)
	})

	t.Run("afterPaddingPartialAdvance", func(t *testing.T) {
		image, _, handledErr := newPollImageWithErrorHandler(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		writePollTestPaddingThenData(termBuffer, pollTestPaddingLength, panicPayload)

		fragments := image.Poll(pollPanicHandler, 10)

		require.Equal(t, 1, fragments)
		require.Equal(t, int64(pollTestPaddingLength)+panicSpan, image.Position())
		handled := handledErr()
		require.Error(t, handled)
		require.Equal(t, wantErr, handled.Error())
	})

	t.Run("nthFrameAfterSuccess", func(t *testing.T) {
		image, capture, handledErr := newPollImageWithErrorHandler(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		chainPayload := []byte("a")
		chainEnd := writeDataFrameChain(t, termBuffer, 0, 2, chainPayload)

		fragments := image.Poll(nthFramePanicHandler(2, errors.New(wantErr)), 10)

		require.Equal(t, 2, fragments)
		require.Equal(t, int64(chainEnd), image.Position())
		handled := handledErr()
		require.Error(t, handled)
		require.Equal(t, wantErr, handled.Error())

		fragments = image.Poll(capture.handler, 1)
		require.Zero(t, fragments)
		require.Empty(t, capture.payloads)
	})

	t.Run("nilErrorHandler", func(t *testing.T) {
		image, capture := newPollImage(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		writePollTestPayloads(termBuffer, 0, []byte("first"), panicPayload, []byte("third"))
		secondEnd := int64(logbuffer.AlignFragmentChunkLength(len("first"))) + panicSpan

		require.PanicsWithError(t, wantErr, func() {
			_ = image.Poll(nthFramePanicHandler(2, errors.New(wantErr)), 10)
		})
		require.Equal(t, secondEnd, image.Position())

		fragments := image.Poll(capture.handler, 10)
		require.Equal(t, 1, fragments)
		require.Equal(t, [][]byte{[]byte("third")}, capture.payloads)
	})

	t.Run("errorHandlerPanicDoesNotEscape", func(t *testing.T) {
		image := aeron.NewImageForTestWithErrorHandler(
			t,
			testSubscription(t),
			0,
			func(error) {
				panic("error handler panic")
			},
		)
		writePollTestDataFrame(
			aeron.ImageTermBuffer0ForTest(image),
			0,
			0,
			logbuffer.Unfragmented,
			panicPayload,
		)

		fragments := image.Poll(pollPanicHandler, 1)

		require.Equal(t, 1, fragments)
		require.Equal(t, panicSpan, image.Position())
	})

	t.Run("nonErrorPanicWrap", func(t *testing.T) {
		const panicValue = "boom"
		image, _, handledErr := newPollImageWithErrorHandler(t, 0)
		writePollTestDataFrame(
			aeron.ImageTermBuffer0ForTest(image),
			0,
			0,
			logbuffer.Unfragmented,
			panicPayload,
		)

		nonErrorPanicHandler := func(atomicbuffer.AtomicBuffer, int, int, *aeron.Header) {
			panic(panicValue)
		}

		fragments := image.Poll(nonErrorPanicHandler, 1)

		require.Equal(t, 1, fragments)
		require.Equal(t, panicSpan, image.Position())
		handled := handledErr()
		require.Error(t, handled)
		require.Equal(t, "aeron: fragment handler panic: boom", handled.Error())
	})

	t.Run("closeInHandlerThenPanic", func(t *testing.T) {
		image, _, handledErr := newPollImageWithErrorHandler(t, 0)
		writePollTestPayloads(aeron.ImageTermBuffer0ForTest(image), 0, panicPayload)
		before := image.Position()

		closeThenPanicHandler := func(atomicbuffer.AtomicBuffer, int, int, *aeron.Header) {
			aeron.ImageInternalCloseForTest(image)
			panic(errors.New(wantErr))
		}

		require.Equal(t, 1, image.Poll(closeThenPanicHandler, 10))

		handled := handledErr()
		require.Error(t, handled)
		require.Equal(t, wantErr, handled.Error())
		require.True(t, image.IsClosed())
		require.Equal(t, before, image.Position())
	})
}

func TestImage_Poll_paddingOnlyAdvancesPosition(t *testing.T) {
	image, capture := newPollImage(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	aeron.WriteCommittedPaddingFrameForTest(
		termBuffer,
		0,
		aeron.TestPublicationInitialTermID,
		0,
		pollTestWidePaddingLength,
	)

	fragments := image.Poll(capture.handler, 1)

	require.Zero(t, fragments)
	require.Empty(t, capture.payloads)
	require.Equal(t, int64(pollTestWidePaddingLength), image.Position())
}

func TestImage_Poll_fragmentLimitWithLeadingPadding(t *testing.T) {
	image, capture := newPollImage(t, 0)
	payload := []byte("data")
	writePollTestPaddingThenData(aeron.ImageTermBuffer0ForTest(image), pollTestPaddingLength, payload)

	fragments := image.Poll(capture.handler, 2)

	require.Equal(t, 1, fragments)
	require.Equal(t, payload, capture.payloads[0])
	require.Equal(
		t,
		int64(pollTestPaddingLength)+int64(logbuffer.AlignFragmentChunkLength(len(payload))),
		image.Position(),
	)
}

func TestImage_Poll_rejectsMidTermShortPadding(t *testing.T) {
	const (
		frameOffset = logbuffer.FrameAlignment * 2
		padLength   = int32(logbuffer.DataHeaderLength - 1)
		wantErr     = "aeron: corrupt committed frame at term offset 64 length 31"
	)
	image, capture, handledErr := newPollImageWithErrorHandler(t, int64(frameOffset))
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	aeron.WriteMinimalTermEndPaddingForTest(termBuffer, frameOffset, padLength)

	fragments := image.Poll(capture.handler, 1)

	require.Zero(t, fragments)
	require.Equal(t, int64(frameOffset), image.Position())
	handled := handledErr()
	require.Error(t, handled)
	require.Equal(t, wantErr, handled.Error())
}

func TestImage_Poll_closeSkipsCounterStore(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(t *testing.T, image *aeron.Image) aeron.FragmentHandler
		wantFragments int
	}{
		{
			name: "during padding advance",
			setup: func(t *testing.T, image *aeron.Image) aeron.FragmentHandler {
				t.Helper()
				aeron.WriteCommittedPaddingFrameForTest(
					aeron.ImageTermBuffer0ForTest(image),
					0,
					aeron.TestPublicationInitialTermID,
					0,
					pollTestPaddingLength,
				)
				aeron.SetPollAfterPaddingAdvanceForTest(image, func() {
					aeron.ImageInternalCloseForTest(image)
				})
				t.Cleanup(func() {
					aeron.SetPollAfterPaddingAdvanceForTest(image, nil)
				})
				capture := &pollCapture{}
				return capture.handler
			},
			wantFragments: 0,
		},
		{
			name: "during handler",
			setup: func(t *testing.T, image *aeron.Image) aeron.FragmentHandler {
				t.Helper()
				writePollTestDataFrame(
					aeron.ImageTermBuffer0ForTest(image),
					0,
					0,
					logbuffer.Unfragmented,
					[]byte("close-in-handler"),
				)
				return func(atomicbuffer.AtomicBuffer, int, int, *aeron.Header) {
					aeron.ImageInternalCloseForTest(image)
				}
			},
			wantFragments: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			image, _ := newPollImage(t, 0)
			positionBefore := image.Position()
			handler := tt.setup(t, image)

			fragments := image.Poll(handler, 1)

			require.Equal(t, tt.wantFragments, fragments)
			require.True(t, image.IsClosed())
			require.Equal(t, positionBefore, image.Position())
		})
	}
}

func TestImage_Poll_deliversFragmentedFramesIndividually(t *testing.T) {
	image, capture := newPollImage(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	fragments := []aeron.FragmentedFrameForTest{
		{Flags: logbuffer.BeginFlag, Payload: []byte("begin")},
		{Flags: 0, Payload: []byte("middle")},
		{Flags: logbuffer.EndFlag, Payload: []byte("end")},
	}

	aeron.WriteCommittedDataFramesForTest(termBuffer, 0, fragments)

	got := image.Poll(capture.handler, len(fragments))

	require.Equal(t, len(fragments), got)
	require.Len(t, capture.payloads, len(fragments))
	require.Equal(t, fragments[0].Payload, capture.payloads[0])
	require.Equal(t, fragments[1].Payload, capture.payloads[1])
	require.Equal(t, fragments[2].Payload, capture.payloads[2])
	require.Equal(t, byte(logbuffer.BeginFlag), capture.flags[0]&byte(logbuffer.BeginFlag))
	require.Zero(t, capture.flags[0]&byte(logbuffer.EndFlag))
	require.Zero(t, capture.flags[1]&(byte(logbuffer.BeginFlag)|byte(logbuffer.EndFlag)))
	require.Equal(t, byte(logbuffer.EndFlag), capture.flags[2]&byte(logbuffer.EndFlag))
	require.Zero(t, capture.flags[2]&byte(logbuffer.BeginFlag))
}

func TestImage_Poll_singleTermScanDoesNotSpanTerms(t *testing.T) {
	image, capture := newPollImage(t, 0)
	term0Payload := []byte("t0")
	writeDataFrameChain(t, aeron.ImageTermBuffer0ForTest(image), 0, 2, term0Payload)
	writeDataFrameChain(t, aeron.ImageTermBufferForTest(image, 1), 0, 2, []byte("t1"))

	fragments := image.Poll(capture.handler, 100)

	require.Equal(t, 2, fragments)
	require.Len(t, capture.payloads, 2)
	require.Equal(t, term0Payload, capture.payloads[0])
	require.Equal(t, term0Payload, capture.payloads[1])
	require.Equal(t, int64(2*logbuffer.AlignFragmentChunkLength(len(term0Payload))), image.Position())
}

func TestImage_Poll_rejectsOversizedTailPadding(t *testing.T) {
	const termLength = aeron.TestPublicationTermLength
	const tailOffset = termLength - logbuffer.FrameAlignment // 32 bytes remain; PAD claims 64
	image, capture := newPollImage(t, int64(tailOffset))
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	aeron.WriteCommittedPaddingFrameForTest(
		termBuffer,
		tailOffset,
		aeron.TestPublicationInitialTermID,
		int32(tailOffset),
		pollTestTailFrameLength,
	)

	fragments := image.Poll(capture.handler, 1)

	require.Zero(t, fragments)
	require.Equal(t, int64(tailOffset), image.Position())
}

func TestImage_Poll_rejectsCorruptFrameLength(t *testing.T) {
	const wantErr = "aeron: corrupt committed frame at term offset 0 length 31"
	image, capture, handledErr := newPollImageWithErrorHandler(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	logbuffer.PutFrameType(termBuffer, 0, logbuffer.HdrTypeData)
	logbuffer.PutFrameLengthOrdered(termBuffer, 0, int32(logbuffer.DataHeaderLength-1))

	fragments := image.Poll(capture.handler, 1)

	require.Zero(t, fragments)
	require.Zero(t, image.Position())
	handled := handledErr()
	require.Error(t, handled)
	require.Equal(t, wantErr, handled.Error())
}

func TestImage_Poll_consumesSubHeaderTermEndGap(t *testing.T) {
	const termLength = aeron.TestPublicationTermLength
	const slackOffset = termLength - 1

	image, capture := newPollImage(t, int64(slackOffset))
	term0 := aeron.ImageTermBuffer0ForTest(image)
	payload := []byte("term0-before-slack")
	writePollTestDataFrame(term0, 0, 0, logbuffer.Unfragmented, payload)

	fragments := image.Poll(capture.handler, 1)

	require.Zero(t, fragments)
	require.Empty(t, capture.payloads)
	require.Equal(t, int64(termLength), image.Position())

	term1 := aeron.ImageTermBufferForTest(image, 1)
	nextPayload := []byte("term1-after-slack")
	writePollTestDataFrame(
		term1,
		0,
		0,
		logbuffer.Unfragmented,
		nextPayload,
	)

	capture.reset()
	fragments = image.Poll(capture.handler, 1)

	require.Equal(t, 1, fragments)
	require.Equal(t, nextPayload, capture.payloads[0])
	require.Equal(
		t,
		int64(termLength)+int64(logbuffer.AlignFragmentChunkLength(len(nextPayload))),
		image.Position(),
	)
}

func TestImage_Poll_consumesTermEndPadding(t *testing.T) {
	const termLength = aeron.TestPublicationTermLength

	tests := []struct {
		name          string
		padLength     int32
		setup         func(termBuffer atomicbuffer.AtomicBuffer, paddingOffset int)
		assertPrePoll func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, paddingOffset int)
	}{
		{
			name:      "slack1",
			padLength: 1,
			setup: func(termBuffer atomicbuffer.AtomicBuffer, paddingOffset int) {
				aeron.WriteMinimalTermEndPaddingForTest(termBuffer, paddingOffset, 1)
			},
			assertPrePoll: func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, paddingOffset int) {
				require.Zero(t, logbuffer.FrameLengthVolatile(termBuffer, paddingOffset))
			},
		},
		{
			name:      "slack7",
			padLength: 7,
			setup: func(termBuffer atomicbuffer.AtomicBuffer, paddingOffset int) {
				aeron.WriteMinimalTermEndPaddingForTest(termBuffer, paddingOffset, 7)
			},
			assertPrePoll: func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, paddingOffset int) {
				require.Equal(t, int32(7), termBuffer.GetInt32(paddingOffset))
			},
		},
		{
			name:      "minimal",
			padLength: pollTestMinimalPadLength,
			setup: func(termBuffer atomicbuffer.AtomicBuffer, paddingOffset int) {
				aeron.WriteMinimalTermEndPaddingForTest(termBuffer, paddingOffset, pollTestMinimalPadLength)
			},
			assertPrePoll: func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, paddingOffset int) {
				require.Equal(t, pollTestMinimalPadLength, termBuffer.GetInt32(paddingOffset))
				require.Equal(t, int16(logbuffer.HdrTypePad), logbuffer.FrameType(termBuffer, paddingOffset))
			},
		},
		{
			name:      "full",
			padLength: pollTestTermEndFullPadLength,
			setup: func(termBuffer atomicbuffer.AtomicBuffer, paddingOffset int) {
				aeron.WriteCommittedPaddingFrameForTest(
					termBuffer,
					paddingOffset,
					aeron.TestPublicationInitialTermID,
					int32(paddingOffset),
					pollTestTermEndFullPadLength,
				)
			},
			assertPrePoll: func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, paddingOffset int) {
				require.Equal(t, pollTestTermEndFullPadLength, termBuffer.GetInt32(paddingOffset))
				require.True(t, logbuffer.IsPaddingFrame(termBuffer, paddingOffset))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			paddingOffset := termLength - int(tc.padLength)
			image, capture := newPollImage(t, int64(paddingOffset))
			termBuffer := aeron.ImageTermBuffer0ForTest(image)
			tc.setup(termBuffer, paddingOffset)
			tc.assertPrePoll(t, termBuffer, paddingOffset)

			fragments := image.Poll(capture.handler, 1)

			require.Zero(t, fragments)
			require.Empty(t, capture.payloads)
			require.Equal(t, int64(termLength), image.Position())
		})
	}
}

func TestImage_Poll_stopsAtCorruptFrameAfterCommitted(t *testing.T) {
	payload := []byte("ok")
	corruptOffset := logbuffer.AlignFragmentChunkLength(len(payload))
	const wantCorruptErr = "aeron: corrupt committed frame at term offset 64 length 31"

	assertStopsAtCorruptFrame := func(t *testing.T, image *aeron.Image, capture *pollCapture) {
		fragments := image.Poll(capture.handler, 10)
		require.Equal(t, 1, fragments)
		require.Equal(t, int64(corruptOffset), image.Position())

		fragments = image.Poll(capture.handler, 10)
		require.Zero(t, fragments)
		require.Equal(t, int64(corruptOffset), image.Position())
	}

	t.Run("withoutErrorHandler", func(t *testing.T) {
		image, capture := newPollImage(t, 0)
		writePollTestCommittedThenCorrupt(aeron.ImageTermBuffer0ForTest(image), payload, corruptOffset)
		assertStopsAtCorruptFrame(t, image, capture)
	})

	t.Run("withErrorHandler", func(t *testing.T) {
		var invocations int
		var handled error
		image := aeron.NewImageForTestWithErrorHandler(
			t,
			testSubscription(t),
			0,
			func(err error) {
				invocations++
				handled = err
			},
		)
		capture := &pollCapture{}
		writePollTestCommittedThenCorrupt(aeron.ImageTermBuffer0ForTest(image), payload, corruptOffset)
		assertStopsAtCorruptFrame(t, image, capture)
		require.Equal(t, 1, invocations)
		require.Error(t, handled)
		require.Equal(t, wantCorruptErr, handled.Error())
	})
}

func TestImage_Poll_rejectsAlignedSpanOverflow(t *testing.T) {
	const frameOffset = aeron.TestPublicationTermLength - 40
	const frameLength = int32(40)
	const wantErr = "aeron: corrupt committed frame at term offset 65496 length 40"
	image, capture, handledErr := newPollImageWithErrorHandler(t, int64(frameOffset))
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	if err := logbuffer.WriteDataHeaderUnfragmented(
		termBuffer,
		frameOffset,
		aeron.TestPublicationSessionID,
		aeron.TestPublicationStreamID,
		aeron.TestPublicationInitialTermID,
		int32(frameOffset),
		logbuffer.Unfragmented,
	); err != nil {
		t.Fatal(err)
	}
	termBuffer.PutBytes(frameOffset+logbuffer.OffsetFramePayload, []byte{1, 2, 3, 4, 5, 6, 7, 8})
	logbuffer.PutFrameLengthOrdered(termBuffer, frameOffset, frameLength)

	fragments := image.Poll(capture.handler, 1)

	require.Zero(t, fragments)
	require.Equal(t, int64(frameOffset), image.Position())
	handled := handledErr()
	require.Error(t, handled)
	require.Equal(t, wantErr, handled.Error())
}

func TestImage_Poll_headerFields(t *testing.T) {
	const frameOffset = pollTestNonZeroFrameOffset
	image, capture := newPollImage(t, int64(frameOffset))
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	payload := []byte("hdr")
	writePollTestDataFrame(
		termBuffer,
		frameOffset,
		int32(frameOffset),
		logbuffer.BeginFlag|logbuffer.EndFlag,
		payload,
	)

	fragments := image.Poll(capture.handler, 1)

	require.Equal(t, 1, fragments)
	header := capture.headers[0]
	require.Equal(t, aeron.TestPublicationSessionID, header.SessionID())
	require.Equal(t, aeron.TestPublicationStreamID, header.StreamID())
	require.Equal(t, aeron.TestPublicationInitialTermID, header.TermID())
	require.Equal(t, int32(frameOffset), header.TermOffset())
	require.Equal(t, byte(logbuffer.Unfragmented), header.Flags())
	require.Equal(
		t,
		int64(frameOffset+logbuffer.AlignFragmentChunkLength(len(payload))),
		header.Position(),
	)
}

func TestImage_ReceivedPosition(t *testing.T) {
	const termLength = aeron.TestPublicationTermLength
	const frame = receivedPositionFrameLength

	tests := []struct {
		name  string
		start int64
		setup func(t *testing.T, image *aeron.Image, start int64)
		want  int64
	}{
		{
			name:  "empty term reports the subscriber position",
			start: 0,
			setup: func(*testing.T, *aeron.Image, int64) {},
			want:  0,
		},
		{
			name:  "single term reaches the last committed frame",
			start: 0,
			setup: func(t *testing.T, image *aeron.Image, start int64) {
				writeReceivedPositionFrames(t, image, int(start)/termLength, int(start)%termLength, 5)
			},
			want: 5 * frame,
		},
		{
			name:  "scan starts at the subscriber position not the join position",
			start: termLength + 2*frame,
			setup: func(t *testing.T, image *aeron.Image, start int64) {
				writeReceivedPositionFrames(t, image, int(start)/termLength, int(start)%termLength, 4)
			},
			want: termLength + 6*frame,
		},
		{
			name:  "term rollover with an exact fill",
			start: termLength - 4*frame,
			setup: func(t *testing.T, image *aeron.Image, start int64) {
				termIndex := int(start) / termLength
				writeReceivedPositionFrames(t, image, termIndex, int(start)%termLength, 4)
				writeReceivedPositionFrames(t, image, termIndex+1, 0, 3)
			},
			want: termLength + 3*frame,
		},
		{
			name:  "term rollover across a term-end padding frame",
			start: termLength - 2*frame - logbuffer.FrameAlignment*2,
			setup: func(t *testing.T, image *aeron.Image, start int64) {
				termIndex := int(start) / termLength
				padOffset := writeReceivedPositionFrames(
					t,
					image,
					termIndex,
					int(start)%termLength,
					2,
				)
				aeron.WriteCommittedPaddingFrameForTest(
					aeron.ImageTermBufferForTest(image, termIndex),
					padOffset,
					aeron.TestPublicationInitialTermID,
					int32(padOffset),
					int32(termLength-padOffset),
				)
				writeReceivedPositionFrames(t, image, termIndex+1, 0, 2)
			},
			want: termLength + 2*frame,
		},
		{
			name:  "term rollover across a sub-header term-end gap",
			start: termLength - 7,
			setup: func(t *testing.T, image *aeron.Image, start int64) {
				termIndex := int(start) / termLength
				termOffset := int(start) % termLength
				aeron.WriteMinimalTermEndPaddingForTest(
					aeron.ImageTermBufferForTest(image, termIndex),
					termOffset,
					int32(termLength-termOffset),
				)
				writeReceivedPositionFrames(t, image, termIndex+1, 0, 1)
			},
			want: termLength + frame,
		},
		{
			name:  "stops at a term-end gap whose committed length is not the gap",
			start: termLength - 5,
			setup: func(t *testing.T, image *aeron.Image, start int64) {
				termIndex := int(start) / termLength
				termOffset := int(start) % termLength
				aeron.WriteMinimalTermEndPaddingForTest(
					aeron.ImageTermBufferForTest(image, termIndex),
					termOffset,
					4,
				)
				writeReceivedPositionFrames(t, image, termIndex+1, 0, 1)
			},
			want: termLength - 5,
		},
		{
			name:  "two term crossings in one call",
			start: termLength - frame,
			setup: func(t *testing.T, image *aeron.Image, start int64) {
				termIndex := int(start) / termLength
				writeReceivedPositionFrames(t, image, termIndex, int(start)%termLength, 1)
				writeReceivedPositionFrames(t, image, termIndex+1, 0, termLength/frame)
				writeReceivedPositionFrames(t, image, termIndex+2, 0, 2)
			},
			want: 2*termLength + 2*frame,
		},
		{
			name:  "stops at an uncommitted frame",
			start: 0,
			setup: func(t *testing.T, image *aeron.Image, start int64) {
				writeReceivedPositionFrames(t, image, int(start)/termLength, int(start)%termLength, 2)
				writeReceivedPositionFrames(t, image, int(start)/termLength, 3*frame, 2)
			},
			want: 2 * frame,
		},
		{
			name:  "stops at an in-flight frame",
			start: 0,
			setup: func(t *testing.T, image *aeron.Image, start int64) {
				termBuffer := aeron.ImageTermBufferForTest(image, int(start)/termLength)
				writeReceivedPositionFrames(t, image, int(start)/termLength, int(start)%termLength, 2)
				logbuffer.PutFrameLengthRelease(termBuffer, 2*frame, -frame)
			},
			want: 2 * frame,
		},
		{
			name:  "stops at a frame shorter than a header",
			start: 0,
			setup: func(t *testing.T, image *aeron.Image, start int64) {
				termIndex := int(start) / termLength
				writeReceivedPositionFrames(t, image, termIndex, int(start)%termLength, 2)
				writePollTestCorruptFrame(
					aeron.ImageTermBufferForTest(image, termIndex),
					int(start)%termLength+2*frame,
				)
			},
			want: 2 * frame,
		},
		{
			name:  "stops at a frame that overruns the term",
			start: 0,
			setup: func(t *testing.T, image *aeron.Image, start int64) {
				termIndex := int(start) / termLength
				termOffset := int(start) % termLength
				termBuffer := aeron.ImageTermBufferForTest(image, termIndex)
				writeReceivedPositionFrames(t, image, termIndex, termOffset, 2)
				logbuffer.PutFrameType(termBuffer, termOffset+2*frame, logbuffer.HdrTypeData)
				logbuffer.PutFrameLengthOrdered(termBuffer, termOffset+2*frame, int32(termLength))
			},
			want: 2 * frame,
		},
		{
			name:  "stops at a padding frame that overruns the term",
			start: 0,
			setup: func(t *testing.T, image *aeron.Image, start int64) {
				termIndex := int(start) / termLength
				termOffset := int(start) % termLength
				termBuffer := aeron.ImageTermBufferForTest(image, termIndex)
				writeReceivedPositionFrames(t, image, termIndex, termOffset, 2)
				aeron.WriteCommittedPaddingFrameForTest(
					termBuffer,
					termOffset+2*frame,
					aeron.TestPublicationInitialTermID,
					int32(termOffset+2*frame),
					int32(termLength),
				)
			},
			want: 2 * frame,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Join at 0 and move the subscriber so the scan uses the live
			// subscriber position rather than the join position.
			image, _ := newPollImage(t, 0)
			aeron.ImageSetSubscriberPositionForTest(image, tc.start)
			tc.setup(t, image, tc.start)

			received := image.ReceivedPosition()

			require.Equal(t, tc.want, received)
			require.Equal(t, tc.start, image.Position(), "the scan leaves the subscriber alone")
			require.Equal(t, received, image.ReceivedPosition(), "the scan is idempotent")
			require.Equal(
				t,
				tc.want,
				pollImageToExhaustion(t, image),
				"repeated unbounded poll settles at the received position",
			)
			require.Equal(t, tc.want, image.ReceivedPosition())
		})
	}
}

// TestImage_ReceivedPosition_boundsAtTermRing covers the one state where the
// scan deliberately stops short of what Poll would consume. Poll has no ring
// bound, so a term ring committed end to end loops it; the scan reports the ring.
func TestImage_ReceivedPosition_boundsAtTermRing(t *testing.T) {
	const termLength = aeron.TestPublicationTermLength
	const frame = receivedPositionFrameLength

	image, _ := newPollImage(t, 0)
	for index := range logbuffer.PartitionCount {
		writeReceivedPositionFrames(t, image, index, 0, termLength/frame)
	}

	require.Equal(t, int64(logbuffer.PartitionCount*termLength), image.ReceivedPosition())
	require.Zero(t, image.Position(), "the scan leaves the subscriber alone")

	pollCalls := 0
	for range logbuffer.PartitionCount + 1 {
		before := image.Position()
		image.Poll(receivedPositionNoopHandler, math.MaxInt)
		if image.Position() == before {
			break
		}
		pollCalls++
	}
	require.Equal(t, logbuffer.PartitionCount+1, pollCalls, "poll does not bound at the ring")
}

func TestImage_ReceivedPosition_closedImageReportsFinalPosition(t *testing.T) {
	const frame = receivedPositionFrameLength

	image, _ := newPollImage(t, 0)
	writeReceivedPositionFrames(t, image, 0, 0, 3)
	aeron.ImageSetSubscriberPositionForTest(image, frame)
	require.Equal(t, int64(3*frame), image.ReceivedPosition())

	aeron.ImageInternalCloseForTest(image)

	require.Equal(t, int64(frame), image.ReceivedPosition())
	require.Equal(t, image.Position(), image.ReceivedPosition())

	// The driver reclaims and reuses the subscriber counter slot after the
	// conductor releases the image, so a closed image must not read it.
	aeron.ImageSetSubscriberPositionForTest(image, 99*frame)

	require.Equal(t, int64(frame), image.ReceivedPosition())
	require.Equal(t, image.Position(), image.ReceivedPosition())
}

// TestImage_ReceivedPosition_doesNotReportCorruptFrame keeps the corrupt-frame
// report on the consuming path. Reporting from a query would allocate, and its
// debounce would silence the next Poll over the same offset.
func TestImage_ReceivedPosition_doesNotReportCorruptFrame(t *testing.T) {
	const wantErr = "aeron: corrupt committed frame at term offset 0 length 31"

	image, capture, handledErr := newPollImageWithErrorHandler(t, 0)
	writePollTestCorruptFrame(aeron.ImageTermBuffer0ForTest(image), 0)

	require.Zero(t, image.ReceivedPosition())
	require.NoError(t, handledErr(), "the scan does not invoke the subscriber error handler")

	require.Zero(t, image.Poll(capture.handler, 1))
	handled := handledErr()
	require.Error(t, handled)
	require.Equal(t, wantErr, handled.Error())
}

// TestImage_ReceivedPosition_runsNoPollHook asserts the scan calls no user or
// hook code. Nothing inside the loop can close the image or panic, which is why
// the scan carries neither an in-loop close check nor a recover.
func TestImage_ReceivedPosition_runsNoPollHook(t *testing.T) {
	const frame = receivedPositionFrameLength

	image, capture := newPollImage(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	aeron.WriteCommittedPaddingFrameForTest(
		termBuffer,
		0,
		aeron.TestPublicationInitialTermID,
		0,
		frame,
	)
	writeReceivedPositionFrames(t, image, 0, frame, 1)

	hookCalls := 0
	aeron.SetPollAfterPaddingAdvanceForTest(image, func() { hookCalls++ })
	t.Cleanup(func() { aeron.SetPollAfterPaddingAdvanceForTest(image, nil) })

	require.Equal(t, int64(2*frame), image.ReceivedPosition())
	require.Zero(t, hookCalls, "the scan crossed the padding frame without the poll hook")

	require.Equal(t, 1, image.Poll(capture.handler, 1))
	require.Equal(t, 1, hookCalls, "poll still fires the hook over the same padding frame")
}
