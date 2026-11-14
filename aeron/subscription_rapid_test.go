//go:build rapid

package aeron_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/internal/testutil"
)

type subImageOp int

const (
	subImageOpAdd subImageOp = iota
	subImageOpAddDup
	subImageOpRemove
	subImageOpCloseImage
	subImageOpCloseSubscription
)

func drawSubImageOp(rt *rapid.T, step int, hasImages, subClosed bool) subImageOp {
	if !subClosed && rapid.IntRange(0, 19).Draw(rt, fmt.Sprintf("closeRoll%d", step)) == 0 {
		return subImageOpCloseSubscription
	}
	if hasImages {
		op := rapid.IntRange(0, int(subImageOpCloseImage)).Draw(rt, fmt.Sprintf("op%d", step))
		return subImageOp(op)
	}
	return subImageOpAdd
}

func subscriptionHasOpenImage(images map[int64]*aeron.Image) bool {
	for _, image := range images {
		if image != nil && !image.IsClosed() {
			return true
		}
	}
	return false
}

func pickCorrelationID(rt *rapid.T, images map[int64]*aeron.Image, name string) int64 {
	rt.Helper()

	ids := make([]int64, 0, len(images))
	for correlationID := range images {
		ids = append(ids, correlationID)
	}
	idx := rapid.IntRange(0, len(ids)-1).Draw(rt, name)
	return ids[idx]
}

// imageSliceSnapshot is one published image slice. Later adds and removes
// must leave its elements unchanged.
type imageSliceSnapshot struct {
	header []*aeron.Image
	elems  []*aeron.Image
}

// snapshotSubscriptionImages requires len == cap on the published slice, so a
// later append cannot write past its length.
func snapshotSubscriptionImages(t testing.TB, sub *aeron.Subscription) imageSliceSnapshot {
	t.Helper()

	header := aeron.SubscriptionImagesForTest(sub)
	require.Len(t, header, cap(header))

	return imageSliceSnapshot{header: header, elems: slices.Clone(header)}
}

func requireImageSliceUnchanged(t testing.TB, snap imageSliceSnapshot) {
	t.Helper()

	for index := range snap.elems {
		require.Same(t, snap.elems[index], snap.header[index])
	}
}

func firstOpenImageBySession(order []*aeron.Image, sessionID int32) *aeron.Image {
	for _, image := range order {
		if !image.IsClosed() && image.SessionID() == sessionID {
			return image
		}
	}
	return nil
}

func TestSubscription_imageMapInvariants_rapid(t *testing.T) {
	t.Parallel()
	sessionIDs := []int32{11, 22, 33}
	rapid.Check(t, func(rt *rapid.T) {
		t := testutil.IterationTB(t, rt)

		sub := testSubscription(t)

		opCount := rapid.IntRange(1, 12).Draw(rt, "opCount")
		images := make(map[int64]*aeron.Image)
		var order []*aeron.Image
		newImage := func(label string, correlationID, joinPos int64) *aeron.Image {
			sessionID := rapid.SampledFrom(sessionIDs).Draw(rt, label)
			return aeron.NewTestImageForSubscriptionHooksWithSessionID(
				t,
				sub,
				sessionID,
				correlationID,
				joinPos,
			)
		}

		var published []imageSliceSnapshot

		for step := range opCount {
			op := drawSubImageOp(rt, step, len(images) > 0, sub.IsClosed())
			if op == subImageOpAdd || op == subImageOpAddDup || op == subImageOpRemove {
				published = append(published, snapshotSubscriptionImages(t, sub))
			}

			countBefore := sub.ImageCount()

			switch op {
			case subImageOpAdd:
				correlationID := aeron.TestImageCorrelationID + int64(step) + 1
				joinPos := aeron.TestSubscriberPositionAtJoin + int64(step)*64
				image := newImage(fmt.Sprintf("session%d", step), correlationID, joinPos)
				aeron.SubscriptionAddImageForTest(sub, image)
				images[correlationID] = image
				order = append(order, image)

			case subImageOpAddDup:
				correlationID := pickCorrelationID(rt, images, fmt.Sprintf("dupCorr%d", step))
				original := images[correlationID]
				joinPos := aeron.TestSubscriberPositionAtJoin + int64(step)*64 + 1
				dup := newImage(fmt.Sprintf("dupSession%d", step), correlationID, joinPos)
				aeron.SubscriptionAddImageForTest(sub, dup)
				require.Equal(rt, countBefore, sub.ImageCount())
				require.Same(rt, original, images[correlationID])

			case subImageOpRemove:
				correlationID := pickCorrelationID(rt, images, fmt.Sprintf("rmCorr%d", step))
				removed := aeron.SubscriptionRemoveImageForTest(sub, correlationID)
				require.Same(rt, images[correlationID], removed)
				delete(images, correlationID)
				order = slices.DeleteFunc(order, func(image *aeron.Image) bool {
					return image == removed
				})

			case subImageOpCloseImage:
				correlationID := pickCorrelationID(rt, images, fmt.Sprintf("closeCorr%d", step))
				aeron.ImageInternalCloseForTest(images[correlationID])

			case subImageOpCloseSubscription:
				require.NoError(rt, sub.Close())
				require.True(rt, sub.IsClosed())
			}

			require.Equal(rt, len(images), sub.ImageCount())

			for _, snap := range published {
				requireImageSliceUnchanged(t, snap)
			}

			closed := sub.IsClosed()
			wantConnected := !closed && subscriptionHasOpenImage(images)
			require.Equal(rt, wantConnected, sub.IsConnected())

			for _, sessionID := range sessionIDs {
				got, ok := sub.ImageBySessionID(sessionID)
				var want *aeron.Image
				if !closed {
					want = firstOpenImageBySession(order, sessionID)
				}
				if want == nil {
					require.False(rt, ok)
					require.Nil(rt, got)
					continue
				}
				require.True(rt, ok)
				require.Same(rt, want, got)
			}
		}
	})
}

func addSubscriptionPollImageWithRandomFrames(
	rt *rapid.T,
	tb testing.TB,
	sub *aeron.Subscription,
	correlationID int64,
	namePrefix string,
) *aeron.Image {
	tb.Helper()

	image := aeron.NewImageForTestWithCorrelationID(tb, sub, 0, correlationID)
	frameCount := rapid.IntRange(0, 4).Draw(rt, namePrefix+"frameCount")
	writePollTestRandomFrames(
		rt,
		aeron.ImageTermBuffer0ForTest(image),
		namePrefix,
		frameCount,
		24,
		logbuffer.Unfragmented,
	)
	aeron.SubscriptionAddImageForTest(sub, image)
	return image
}

func TestSubscription_Poll_invariants_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		t := testutil.IterationTB(t, rt)

		sub := testSubscription(t)
		imageCount := rapid.IntRange(1, 4).Draw(rt, "imageCount")
		images := make([]*aeron.Image, 0, imageCount)
		for index := range imageCount {
			images = append(images, addSubscriptionPollImageWithRandomFrames(
				rt,
				t,
				sub,
				aeron.TestImageCorrelationID+int64(index),
				fmt.Sprintf("img%d", index),
			))
		}

		capture := &pollCapture{}
		fragmentLimit := rapid.IntRange(1, 8).Draw(rt, "fragmentLimit")
		positionsBefore := make([]int64, len(images))
		for index, image := range images {
			positionsBefore[index] = image.Position()
		}

		fragments := sub.Poll(capture.handler, fragmentLimit)

		require.LessOrEqual(rt, fragments, fragmentLimit)
		require.Len(rt, capture.payloads, fragments)
		for index, image := range images {
			require.GreaterOrEqual(rt, image.Position(), positionsBefore[index])
		}

		rotationCount := rapid.IntRange(2, 4).Draw(rt, "rotationImageCount")
		rotationSub := testSubscription(t)
		rotationPayloads := make([][]byte, rotationCount)
		for index := range rotationCount {
			payload := []byte{byte('a' + index)}
			rotationPayloads[index] = payload
			addSubscriptionPollImage(t, rotationSub, aeron.TestImageCorrelationID+int64(index), payload)
		}

		rotationSteps := make([]subscriptionPollStep, rotationCount)
		for index := range rotationCount {
			rotationSteps[index] = subscriptionPollStep{
				want:     1,
				payloads: [][]byte{rotationPayloads[index]},
			}
		}
		rotationCapture := &pollCapture{}
		requireSubscriptionPollSteps(t, rotationSub, rotationCapture, 1, rotationSteps...)
	})
}

// subscriptionRotationModel is the reference model for round-robin polls. A
// nil queue is a nil image slot.
type subscriptionRotationModel struct {
	queues          [][][]byte
	roundRobinIndex int
}

func (model *subscriptionRotationModel) poll(limit int) [][]byte {
	length := len(model.queues)
	if model.roundRobinIndex >= length {
		model.roundRobinIndex = 0
	}
	startingIndex := model.roundRobinIndex
	model.roundRobinIndex = startingIndex + 1

	var delivered [][]byte
	for offset := range length {
		slot := (startingIndex + offset) % length
		queue := model.queues[slot]
		if queue == nil {
			continue
		}
		take := min(limit-len(delivered), len(queue))
		delivered = append(delivered, queue[:take]...)
		model.queues[slot] = queue[take:]
	}
	return delivered
}

func TestSubscription_Poll_roundRobinModel_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		t := testutil.IterationTB(t, rt)

		sub := testSubscription(t)
		slotCount := rapid.IntRange(0, 4).Draw(rt, "slotCount")
		model := &subscriptionRotationModel{queues: make([][][]byte, slotCount)}
		for slot := range slotCount {
			if rapid.IntRange(0, 3).Draw(rt, fmt.Sprintf("nilRoll%d", slot)) == 0 {
				continue
			}
			frameCount := rapid.IntRange(0, 3).Draw(rt, fmt.Sprintf("frameCount%d", slot))
			queue := make([][]byte, frameCount)
			for frame := range frameCount {
				queue[frame] = fmt.Appendf(nil, "img%d-%d", slot, frame)
			}
			model.queues[slot] = queue
		}
		addSubscriptionPollSlots(t, sub, model.queues)

		model.roundRobinIndex = rapid.IntRange(0, 2*slotCount+1).Draw(rt, "startIndex")
		aeron.SubscriptionSetRoundRobinIndexForTest(sub, model.roundRobinIndex)

		capture := &pollCapture{}
		pollCount := rapid.IntRange(1, 8).Draw(rt, "pollCount")
		for step := range pollCount {
			limit := rapid.IntRange(0, 4).Draw(rt, fmt.Sprintf("limit%d", step))
			want := model.poll(limit)

			capture.reset()
			require.Equal(rt, len(want), sub.Poll(capture.handler, limit), "step %d", step)
			require.Len(rt, capture.payloads, len(want), "step %d", step)
			for index, payload := range want {
				require.Equal(rt, payload, capture.payloads[index], "step %d", step)
			}
			require.Equal(
				rt,
				model.roundRobinIndex,
				aeron.SubscriptionRoundRobinIndexForTest(sub),
				"step %d",
				step,
			)
		}
	})
}
