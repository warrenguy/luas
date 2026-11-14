package aeron_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/atomicbuffer"
)

func TestSubscription_newSubscription_fields(t *testing.T) {
	sub := testSubscription(t)

	require.Equal(t, aeron.TestSubscriptionRegistrationID, sub.RegistrationID())
	require.Equal(t, aeron.TestPublicationStreamID, sub.StreamID())
	require.Equal(t, aeron.TestImageSourceIdentity, sub.Channel())
	require.Equal(
		t,
		cnc.NullCounterID,
		aeron.SubscriptionChannelStatusCounterIDForTest(sub),
	)
	require.Equal(t, 0, sub.ImageCount())
	require.False(t, sub.IsConnected())
	require.False(t, sub.IsClosed())
}

func TestSubscription_removeImage_notFoundBeforeAdd(t *testing.T) {
	sub := testSubscription(t)

	require.Nil(t, aeron.SubscriptionRemoveImageForTest(sub, aeron.TestImageCorrelationID))
}

func TestSubscription_addImageRemoveImage(t *testing.T) {
	sub := testSubscription(t)
	image := addTestImage(t, sub)

	require.Equal(t, 1, sub.ImageCount())

	removed := aeron.SubscriptionRemoveImageForTest(sub, aeron.TestImageCorrelationID)
	require.Same(t, image, removed)
	require.Equal(t, 0, sub.ImageCount())
	require.False(t, image.IsClosed())

	require.Nil(t, aeron.SubscriptionRemoveImageForTest(sub, aeron.TestImageCorrelationID))
}

func TestSubscription_addImage_nilIgnored(t *testing.T) {
	sub := testSubscription(t)

	aeron.SubscriptionAddImageForTest(sub, nil)
	require.Equal(t, 0, sub.ImageCount())
	require.False(t, sub.IsConnected())
}

func TestSubscription_IsConnected_allImagesClosed(t *testing.T) {
	sub := testSubscription(t)
	closedImage := addTestImage(t, sub)
	aeron.ImageInternalCloseForTest(closedImage)

	require.Equal(t, 1, sub.ImageCount())
	require.False(t, sub.IsConnected())
}

func TestSubscription_ImageAt(t *testing.T) {
	sub := testSubscription(t)
	image := addTestImage(t, sub)
	aeron.SubscriptionInjectNilImageAtIndexForHooks(sub, 1)

	got, ok := sub.ImageAt(0)
	require.True(t, ok)
	require.Same(t, image, got)

	for _, idx := range []int{1, -1} {
		_, ok := sub.ImageAt(idx)
		require.False(t, ok)
	}

	aeron.ImageInternalCloseForTest(image)
	_, ok = sub.ImageAt(0)
	require.False(t, ok)
}

func TestSubscription_ImageAt_closedSubscription(t *testing.T) {
	sub := testSubscription(t)
	addTestImage(t, sub)

	require.NoError(t, sub.Close())
	_, ok := sub.ImageAt(0)
	require.False(t, ok)
}

func TestSubscription_ImageBySessionID(t *testing.T) {
	const (
		sessionA int32 = 11
		missing  int32 = 33
	)

	add := func(
		t *testing.T,
		sub *aeron.Subscription,
		sessionID int32,
		correlationID int64,
	) *aeron.Image {
		t.Helper()
		image := aeron.NewTestImageForSubscriptionHooksWithSessionID(
			t,
			sub,
			sessionID,
			correlationID,
			0,
		)
		aeron.SubscriptionAddImageForTest(sub, image)
		return image
	}

	t.Run("firstAddedWins", func(t *testing.T) {
		sub := testSubscription(t)
		first := add(t, sub, sessionA, 1)
		add(t, sub, sessionA, 2)

		got, ok := sub.ImageBySessionID(sessionA)
		require.True(t, ok)
		require.Same(t, first, got)
	})

	t.Run("otherSession", func(t *testing.T) {
		sub := testSubscription(t)
		add(t, sub, sessionA, 1)

		got, ok := sub.ImageBySessionID(missing)
		require.False(t, ok)
		require.Nil(t, got)
	})

	t.Run("closedFirstMatch", func(t *testing.T) {
		sub := testSubscription(t)
		first := add(t, sub, sessionA, 1)
		second := add(t, sub, sessionA, 2)
		aeron.ImageInternalCloseForTest(first)

		got, ok := sub.ImageBySessionID(sessionA)
		require.True(t, ok)
		require.Same(t, second, got)
	})

	t.Run("nilSlot", func(t *testing.T) {
		sub := testSubscription(t)
		aeron.SubscriptionInjectNilImageAtIndexForHooks(sub, 0)
		image := add(t, sub, sessionA, 1)

		got, ok := sub.ImageBySessionID(sessionA)
		require.True(t, ok)
		require.Same(t, image, got)
	})

	t.Run("closedSubscription", func(t *testing.T) {
		sub := testSubscription(t)
		add(t, sub, sessionA, 1)
		require.NoError(t, sub.Close())

		got, ok := sub.ImageBySessionID(sessionA)
		require.False(t, ok)
		require.Nil(t, got)
	})
}

func TestSubscription_ImageCountIsConnected(t *testing.T) {
	sub := testSubscription(t)
	openImage := testImage(t, sub)
	closedImage := aeron.NewImageForTestWithCorrelationID(
		t,
		sub,
		aeron.TestSubscriberPositionAtJoin+64,
		aeron.TestImageCorrelationID+1,
	)

	aeron.SubscriptionAddImageForTest(sub, openImage)
	aeron.SubscriptionAddImageForTest(sub, closedImage)
	aeron.ImageInternalCloseForTest(closedImage)

	require.Equal(t, 2, sub.ImageCount())
	require.True(t, sub.IsConnected())

	aeron.ImageInternalCloseForTest(openImage)
	require.False(t, sub.IsConnected())
}

func TestSubscription_Close(t *testing.T) {
	sub := testSubscription(t)

	require.NoError(t, sub.Close())
	require.True(t, sub.IsClosed())
}

func TestSubscription_addImage_duplicateCorrelationIDSkipped(t *testing.T) {
	sub := testSubscription(t)
	first := testImage(t, sub)
	second := testImage(t, sub)

	aeron.SubscriptionAddImageForTest(sub, first)
	aeron.SubscriptionAddImageForTest(sub, second)

	require.Equal(t, 1, sub.ImageCount())

	removed := aeron.SubscriptionRemoveImageForTest(sub, aeron.TestImageCorrelationID)
	require.NotNil(t, removed)
	require.Equal(t, 0, sub.ImageCount())
}

func TestSubscription_IsConnected_falseAfterClose(t *testing.T) {
	sub := testSubscription(t)
	addTestImage(t, sub)
	require.True(t, sub.IsConnected())

	require.NoError(t, sub.Close())
	require.False(t, sub.IsConnected())
}

func TestSubscription_Poll_closedReturnsZero(t *testing.T) {
	sub := testSubscription(t)
	addTestImage(t, sub)

	const wantIndex = 3
	aeron.SubscriptionSetRoundRobinIndexForTest(sub, wantIndex)

	require.NoError(t, sub.Close())
	require.Equal(t, 0, sub.Poll(nil, 1))
	require.Equal(t, wantIndex, aeron.SubscriptionRoundRobinIndexForTest(sub))
}

func TestSubscription_Poll_emptyImagesReturnsZero(t *testing.T) {
	sub := testSubscription(t)
	aeron.SubscriptionSetRoundRobinIndexForTest(sub, 5)

	require.Equal(t, 0, sub.Poll(nil, 1))
	require.Equal(t, 1, aeron.SubscriptionRoundRobinIndexForTest(sub))
}

func TestSubscription_Poll_roundRobin(t *testing.T) {
	sub := testSubscription(t)
	addTwoStandardPollImages(t, sub)

	capture := &pollCapture{}
	requireSubscriptionPollSteps(
		t, sub, capture, 1,
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img0-a")}},
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img1-a")}},
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img0-b")}},
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img1-b")}},
	)
}

func TestSubscription_Poll_multiImageLimit(t *testing.T) {
	sub := testSubscription(t)
	addTwoStandardPollImages(t, sub)

	capture := &pollCapture{}
	requireSubscriptionPoll(
		t, sub, capture, 3, 3,
		[][]byte{[]byte("img0-a"), []byte("img0-b"), []byte("img1-a")},
	)
}

func TestSubscription_Poll_deliveryOrder(t *testing.T) {
	sub := testSubscription(t)
	addTwoStandardPollImages(t, sub)

	capture := &pollCapture{}
	requireSubscriptionPoll(
		t, sub, capture, 4, 4,
		[][]byte{
			[]byte("img0-a"),
			[]byte("img0-b"),
			[]byte("img1-a"),
			[]byte("img1-b"),
		},
	)
}

func TestSubscription_Poll_partialLimitFairness(t *testing.T) {
	sub := testSubscription(t)
	addSubscriptionPollImage(
		t,
		sub,
		aeron.TestImageCorrelationID,
		[]byte("img0-a"),
		[]byte("img0-b"),
		[]byte("img0-c"),
	)
	addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID+1, []byte("img1-a"))

	capture := &pollCapture{}
	requireSubscriptionPollSteps(
		t, sub, capture, 2,
		subscriptionPollStep{want: 2, payloads: [][]byte{[]byte("img0-a"), []byte("img0-b")}},
		subscriptionPollStep{want: 2, payloads: [][]byte{[]byte("img1-a"), []byte("img0-c")}},
	)

	capture.reset()
	require.Equal(t, 0, sub.Poll(capture.handler, 1))
}

func TestSubscription_Poll_singleImageDelegates(t *testing.T) {
	sub := testSubscription(t)
	image := addSubscriptionPollImage(
		t,
		sub,
		aeron.TestImageCorrelationID,
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
	)

	capture := &pollCapture{}
	wantFragments := image.Poll(capture.handler, 3)
	expectedPayloads := slices.Clone(capture.payloads)

	aeron.ImageSetSubscriberPositionForTest(image, 0)
	capture.reset()
	require.Equal(t, wantFragments, sub.Poll(capture.handler, 3))
	require.Equal(t, expectedPayloads, capture.payloads)
}

func TestSubscription_Poll_nilHandlerAndZeroLimit(t *testing.T) {
	capture := &pollCapture{}
	for _, tc := range []struct {
		name    string
		handler aeron.FragmentHandler
		limit   int
	}{
		{name: "nilHandler", handler: nil, limit: 1},
		{name: "zeroLimit", handler: capture.handler, limit: 0},
		{name: "negativeLimit", handler: capture.handler, limit: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sub := testSubscription(t)
			image0 := addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID, []byte("img0"))
			image1 := addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID+1, []byte("img1"))

			capture.reset()
			require.Equal(t, 0, sub.Poll(tc.handler, tc.limit), tc.name)
			require.Zero(t, image0.Position(), tc.name)
			require.Zero(t, image1.Position(), tc.name)
			if tc.limit <= 0 {
				require.Empty(t, capture.payloads, tc.name)
			}

			capture.reset()
			require.Equal(t, 1, sub.Poll(capture.handler, 1))
			require.Equal(t, [][]byte{[]byte("img1")}, capture.payloads)
		})
	}
}

func TestSubscription_Poll_skipsClosedImages(t *testing.T) {
	sub := testSubscription(t)
	image0 := addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID, []byte("img0"))
	addSubscriptionPollImage(
		t,
		sub,
		aeron.TestImageCorrelationID+1,
		[]byte("img1-a"),
		[]byte("img1-b"),
	)
	aeron.ImageInternalCloseForTest(image0)

	capture := &pollCapture{}
	requireSubscriptionPollSteps(
		t, sub, capture, 1,
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img1-a")}},
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img1-b")}},
	)
}

func TestSubscription_Poll_allImagesClosedReturnsZero(t *testing.T) {
	sub := testSubscription(t)
	image0 := addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID, []byte("img0"))
	image1 := addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID+1, []byte("img1"))
	aeron.ImageInternalCloseForTest(image0)
	aeron.ImageInternalCloseForTest(image1)

	capture := &pollCapture{}
	require.Equal(t, 0, sub.Poll(capture.handler, 5))
	require.Empty(t, capture.payloads)

	for i, payload := range [][]byte{[]byte("fresh0"), []byte("fresh1")} {
		require.NotNil(
			t,
			aeron.SubscriptionRemoveImageForTest(sub, aeron.TestImageCorrelationID+int64(i)),
		)
		addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID+int64(i), payload)
	}

	capture.reset()
	requireSubscriptionPoll(t, sub, capture, 1, 1, [][]byte{[]byte("fresh1")})
}

func TestSubscription_Poll_threeImageLimit(t *testing.T) {
	sub := testSubscription(t)
	for i, payloads := range [][][]byte{
		{[]byte("img0-a"), []byte("img0-b")},
		{[]byte("img1-a"), []byte("img1-b")},
		{[]byte("img2-a"), []byte("img2-b")},
	} {
		addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID+int64(i), payloads...)
	}

	capture := &pollCapture{}
	requireSubscriptionPoll(
		t, sub, capture, 4, 4,
		[][]byte{
			[]byte("img0-a"),
			[]byte("img0-b"),
			[]byte("img1-a"),
			[]byte("img1-b"),
		},
	)
}

func TestSubscription_Poll_roundRobinThreeImages(t *testing.T) {
	sub := testSubscription(t)
	for i, payloads := range [][][]byte{
		{[]byte("img0-a"), []byte("img0-b")},
		{[]byte("img1-a")},
		{[]byte("img2-a")},
	} {
		addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID+int64(i), payloads...)
	}

	capture := &pollCapture{}
	requireSubscriptionPollSteps(
		t, sub, capture, 1,
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img0-a")}},
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img1-a")}},
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img2-a")}},
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img0-b")}},
	)
}

func TestSubscription_Poll_roundRobinIndexWrapAfterRemoval(t *testing.T) {
	sub := testSubscription(t)
	image0 := addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID, []byte("img0"))
	addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID+1, []byte("img1"))

	capture := &pollCapture{}
	requireSubscriptionPoll(t, sub, capture, 1, 1, [][]byte{[]byte("img0")})

	removed := aeron.SubscriptionRemoveImageForTest(sub, aeron.TestImageCorrelationID)
	require.Same(t, image0, removed)
	require.Equal(t, 1, sub.ImageCount())

	capture.reset()
	requireSubscriptionPoll(t, sub, capture, 1, 1, [][]byte{[]byte("img1")})
}

func TestSubscription_Poll_roundRobinIndexAtImageCount(t *testing.T) {
	sub := testSubscription(t)
	addTwoStandardPollImages(t, sub)

	// Phase: two polls leave the index at the image count.
	capture := &pollCapture{}
	requireSubscriptionPollSteps(
		t, sub, capture, 1,
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img0-a")}},
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img1-a")}},
	)
	require.Equal(t, 2, aeron.SubscriptionRoundRobinIndexForTest(sub), "index at image count")

	// Phase: a third image makes the stored index valid again.
	addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID+2, []byte("img2-a"))

	// Phase: the next poll starts at the new image without a fold.
	capture.reset()
	requireSubscriptionPoll(t, sub, capture, 1, 1, [][]byte{[]byte("img2-a")})
	require.Equal(t, 3, aeron.SubscriptionRoundRobinIndexForTest(sub), "index after new image")
}

func TestSubscription_Poll_roundRobinStartIndex(t *testing.T) {
	for _, tc := range []struct {
		name string
		// slots holds the frame payloads per image. A nil entry is a nil image slot.
		slots        [][][]byte
		startIndex   int
		limit        int
		wantPayloads [][]byte
		wantIndex    int
	}{
		{
			name:         "indexAtImageCountFoldsToZero",
			slots:        [][][]byte{{[]byte("img0")}, {[]byte("img1")}},
			startIndex:   2,
			limit:        1,
			wantPayloads: [][]byte{[]byte("img0")},
			wantIndex:    1,
		},
		{
			name:         "wrapVisitsLeadingSlots",
			slots:        [][][]byte{{[]byte("img0")}, {[]byte("img1")}, {[]byte("img2")}},
			startIndex:   2,
			limit:        3,
			wantPayloads: [][]byte{[]byte("img2"), []byte("img0"), []byte("img1")},
			wantIndex:    3,
		},
		{
			name:         "nilSlotBeforeWrap",
			slots:        [][][]byte{{[]byte("img0")}, nil, {[]byte("img2")}},
			startIndex:   1,
			limit:        3,
			wantPayloads: [][]byte{[]byte("img2"), []byte("img0")},
			wantIndex:    2,
		},
		{
			name:         "nilSlotAfterWrap",
			slots:        [][][]byte{nil, {[]byte("img1")}, {[]byte("img2")}},
			startIndex:   1,
			limit:        3,
			wantPayloads: [][]byte{[]byte("img1"), []byte("img2")},
			wantIndex:    2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sub := testSubscription(t)
			addSubscriptionPollSlots(t, sub, tc.slots)
			aeron.SubscriptionSetRoundRobinIndexForTest(sub, tc.startIndex)

			capture := &pollCapture{}
			requireSubscriptionPoll(
				t, sub, capture, tc.limit, len(tc.wantPayloads), tc.wantPayloads,
			)
			require.Equal(t, tc.wantIndex, aeron.SubscriptionRoundRobinIndexForTest(sub))
		})
	}
}

func TestSubscription_Poll_emptyImageRotation(t *testing.T) {
	sub := testSubscription(t)
	addSubscriptionPollImage(
		t,
		sub,
		aeron.TestImageCorrelationID,
		[]byte("img0-a"),
		[]byte("img0-b"),
	)
	emptyImage := aeron.NewImageForTestWithCorrelationID(
		t,
		sub,
		0,
		aeron.TestImageCorrelationID+1,
	)
	aeron.SubscriptionAddImageForTest(sub, emptyImage)

	capture := &pollCapture{}
	requireSubscriptionPollSteps(
		t, sub, capture, 1,
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img0-a")}},
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img0-b")}},
	)

	capture.reset()
	require.Equal(t, 0, sub.Poll(capture.handler, 1))
	require.Empty(t, capture.payloads)
}

func TestSubscription_Poll_handlerPanicContinuesToNextImage(t *testing.T) {
	const wantErr = "poll handler failed"

	sub := testSubscription(t)
	var handled error
	image0 := aeron.NewImageForTestWithErrorHandler(t, sub, 0, func(err error) {
		handled = err
	})
	writePollTestDataFrame(
		aeron.ImageTermBuffer0ForTest(image0),
		0,
		0,
		logbuffer.Unfragmented,
		[]byte("img0"),
	)
	aeron.SubscriptionAddImageForTest(sub, image0)
	addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID+1, []byte("img1"))

	capture := &pollCapture{}
	handler := subscriptionPollOnFirstFragment(t, capture, func(tb testing.TB) {
		panic(errors.New(wantErr))
	})

	require.Equal(t, 2, sub.Poll(handler, 2))
	require.Equal(t, int64(logbuffer.AlignFragmentChunkLength(len("img0"))), image0.Position())
	require.Error(t, handled)
	require.Equal(t, wantErr, handled.Error())
	require.Equal(t, [][]byte{[]byte("img1")}, capture.payloads)
}

func TestSubscription_ControlledPoll_handlerPanicContinuesToNextImage(t *testing.T) {
	const wantErr = "controlled poll handler failed"

	sub := testSubscription(t)
	var handled error
	image0 := aeron.NewImageForTestWithErrorHandler(t, sub, 0, func(err error) {
		handled = err
	})
	writePollTestDataFrame(
		aeron.ImageTermBuffer0ForTest(image0),
		0,
		0,
		logbuffer.Unfragmented,
		[]byte("img0"),
	)
	aeron.SubscriptionAddImageForTest(sub, image0)
	addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID+1, []byte("img1"))

	capture := &controlledPollCapture{}
	handlerCalls := 0
	handler := func(
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	) aeron.ControlledAction {
		handlerCalls++
		if handlerCalls == 1 {
			panic(errors.New(wantErr))
		}
		return capture.handler(buffer, offset, length, header)
	}

	require.Equal(t, 2, sub.ControlledPoll(handler, 2))
	require.Equal(t, int64(logbuffer.AlignFragmentChunkLength(len("img0"))), image0.Position())
	require.Error(t, handled)
	require.Equal(t, wantErr, handled.Error())
	require.Equal(t, [][]byte{[]byte("img1")}, capture.payloads)
}

func TestSubscription_Poll_closeSkipsCounterStore(t *testing.T) {
	sub := testSubscription(t)
	image0 := aeron.NewImageForTestWithCorrelationID(
		t,
		sub,
		0,
		aeron.TestImageCorrelationID,
	)
	aeron.WriteCommittedPaddingFrameForTest(
		aeron.ImageTermBuffer0ForTest(image0),
		0,
		aeron.TestPublicationInitialTermID,
		0,
		pollTestPaddingLength,
	)
	aeron.SubscriptionAddImageForTest(sub, image0)
	addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID+1, []byte("img1"))

	aeron.SetPollAfterPaddingAdvanceForTest(image0, func() {
		aeron.ImageInternalCloseForTest(image0)
	})
	t.Cleanup(func() {
		aeron.SetPollAfterPaddingAdvanceForTest(image0, nil)
	})

	positionBefore := image0.Position()
	capture := &pollCapture{}
	requireSubscriptionPoll(t, sub, capture, 2, 1, [][]byte{[]byte("img1")})
	require.True(t, image0.IsClosed())
	require.Equal(t, positionBefore, image0.Position())
}

func TestSubscription_Poll_snapshotExcludesAddedImage(t *testing.T) {
	sub := testSubscription(t)
	addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID, []byte("img0"))
	addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID+1, []byte("img1"))

	capture := &pollCapture{}
	handler := subscriptionPollOnFirstFragment(t, capture, func(tb testing.TB) {
		addSubscriptionPollImage(tb, sub, aeron.TestImageCorrelationID+2, []byte("new-img"))
		require.Equal(tb, 3, sub.ImageCount())
	})

	require.Equal(t, 2, sub.Poll(handler, 3))
	require.Equal(t, [][]byte{[]byte("img0"), []byte("img1")}, capture.payloads)
}

func TestSubscription_Poll_snapshotRetainsRemovedImage(t *testing.T) {
	sub := testSubscription(t)
	addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID, []byte("img0"))
	addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID+1, []byte("img1"))

	capture := &pollCapture{}
	handler := subscriptionPollOnFirstFragment(t, capture, func(tb testing.TB) {
		require.NotNil(tb, aeron.SubscriptionRemoveImageForTest(sub, aeron.TestImageCorrelationID))
		require.Equal(tb, 1, sub.ImageCount())
	})

	require.Equal(t, 2, sub.Poll(handler, 2))
	require.Equal(t, [][]byte{[]byte("img0"), []byte("img1")}, capture.payloads)
}

func TestSubscription_Poll_skipsNilImage(t *testing.T) {
	sub := testSubscription(t)
	addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID, []byte("img0"))
	addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID+2, []byte("img2"))
	aeron.SubscriptionInjectNilImageAtIndexForHooks(sub, 1)

	capture := &pollCapture{}
	requireSubscriptionPoll(t, sub, capture, 2, 2, [][]byte{[]byte("img0"), []byte("img2")})
}

func TestSubscription_ControlledPoll_closedLeavesRoundRobin(t *testing.T) {
	sub := testSubscription(t)
	addTestImage(t, sub)

	const wantIndex = 3
	aeron.SubscriptionSetRoundRobinIndexForTest(sub, wantIndex)

	require.NoError(t, sub.Close())
	require.Equal(t, 0, sub.ControlledPoll(nil, 1))
	require.Equal(t, wantIndex, aeron.SubscriptionRoundRobinIndexForTest(sub))
}

func TestSubscription_ControlledPoll_emptyImagesReturnsZero(t *testing.T) {
	sub := testSubscription(t)
	aeron.SubscriptionSetRoundRobinIndexForTest(sub, 5)

	require.Equal(t, 0, sub.ControlledPoll(nil, 1))
	require.Equal(t, 1, aeron.SubscriptionRoundRobinIndexForTest(sub))
}

func TestSubscription_ControlledPoll_roundRobin(t *testing.T) {
	sub := testSubscription(t)
	addTwoStandardPollImages(t, sub)

	capture := &controlledPollCapture{}
	requireSubscriptionControlledPollSteps(
		t, sub, capture, 1,
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img0-a")}},
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img1-a")}},
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img0-b")}},
		subscriptionPollStep{want: 1, payloads: [][]byte{[]byte("img1-b")}},
	)
}

func TestSubscription_ControlledPoll_multiImageLimit(t *testing.T) {
	sub := testSubscription(t)
	addTwoStandardPollImages(t, sub)

	capture := &controlledPollCapture{}
	requireSubscriptionControlledPoll(
		t, sub, capture, 3, 3,
		[][]byte{[]byte("img0-a"), []byte("img0-b"), []byte("img1-a")},
	)
}

func TestSubscription_ControlledPoll_abortStopsOneImageBudgetContinues(t *testing.T) {
	sub := testSubscription(t)
	addTwoStandardPollImages(t, sub)

	// First image first fragment aborts (not counted). Remaining budget visits
	// image1 and can consume both of its frames.
	capture := &controlledPollCapture{
		actions: []aeron.ControlledAction{aeron.ControlledAbort},
	}
	requireSubscriptionControlledPoll(
		t, sub, capture, 2, 2,
		[][]byte{[]byte("img0-a"), []byte("img1-a"), []byte("img1-b")},
	)

	// Aborted image0 frame redelivers. Round-robin starts at image1 (empty).
	capture.reset()
	requireSubscriptionControlledPoll(
		t, sub, capture, 2, 2,
		[][]byte{[]byte("img0-a"), []byte("img0-b")},
	)
}

func TestSubscription_ControlledPoll_nilHandlerAndZeroLimit(t *testing.T) {
	capture := &controlledPollCapture{}
	for _, tc := range []struct {
		name    string
		handler aeron.ControlledFragmentHandler
		limit   int
	}{
		{name: "nilHandler", handler: nil, limit: 1},
		{name: "zeroLimit", handler: capture.handler, limit: 0},
		{name: "negativeLimit", handler: capture.handler, limit: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sub := testSubscription(t)
			image0 := addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID, []byte("img0"))
			image1 := addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID+1, []byte("img1"))

			capture.reset()
			require.Equal(t, 0, sub.ControlledPoll(tc.handler, tc.limit), tc.name)
			require.Zero(t, image0.Position(), tc.name)
			require.Zero(t, image1.Position(), tc.name)
			if tc.limit <= 0 {
				require.Empty(t, capture.payloads, tc.name)
			}

			capture.reset()
			require.Equal(t, 1, sub.ControlledPoll(capture.handler, 1))
			require.Equal(t, [][]byte{[]byte("img1")}, capture.payloads)
		})
	}
}

func TestSubscription_ControlledPoll_skipsNilImage(t *testing.T) {
	sub := testSubscription(t)
	addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID, []byte("img0"))
	addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID+2, []byte("img2"))
	aeron.SubscriptionInjectNilImageAtIndexForHooks(sub, 1)

	capture := &controlledPollCapture{}
	requireSubscriptionControlledPoll(
		t, sub, capture, 2, 2,
		[][]byte{[]byte("img0"), []byte("img2")},
	)
}
