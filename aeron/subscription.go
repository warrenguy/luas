package aeron

import (
	"sync/atomic"

	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/conductor"
)

// Subscription receives fragments for one channel and stream.
//
// One goroutine at a time polls a Subscription and its images. The poll
// goroutine need not run [Client.DoWork], unless
// [Config.ResourceLingerDuration] is zero. [Subscription.Close] and
// [Subscription.SetUnavailableImageHandler] are safe from any goroutine under
// the same condition.
//
// Call these methods on the poll goroutine:
//   - [FragmentAssembler.FreeSessionBuffer]
//   - [FragmentAssembler.Clear]
//   - [ControlledFragmentAssembler.FreeSessionBuffer]
//   - [ControlledFragmentAssembler.Clear]
//
// An [UnavailableImageHandler] must not free a session buffer the poll
// goroutine still reads. Check [Image.IsClosed] on the poll goroutine.
//
// See [Config.ResourceLingerDuration] and [Config.ClientCloseLingerDuration]
// for calls in flight when a log unmaps.
//
// Connect copies image lifecycle callbacks from [Config].
type Subscription struct {
	registrationID int64
	streamID       int32
	channel        string

	// A lookup can read channelStatusCounterID before the ready event stores it.
	channelStatusCounterID atomic.Int32
	counters               *cnc.CountersReader

	// images is an immutable []*Image. Writers store a new slice or nil.
	images atomic.Pointer[[]*Image]

	closed atomic.Bool

	availableImageHandler   AvailableImageHandler
	unavailableImageHandler atomic.Pointer[UnavailableImageHandler]
	conductor               *conductor.Conductor
	client                  *Client

	// roundRobinIndex is the start index for the next pollImages call.
	// pollImages folds it to 0 when it is at or above the image count.
	roundRobinIndex int

	// receiveDestinationHook, when non-nil, handles AddDestination and
	// RemoveDestination in place of the client/driver path. Tests only.
	receiveDestinationHook receiveDestinationHook
}

// receiveDestinationHook overrides driver destination commands.
//
// Nil in production. Tests set this interface via the test-hook helper only.
type receiveDestinationHook interface {
	AddDestination(channel string) error
	RemoveDestination(channel string) error
}

func newSubscription(
	registrationID int64,
	streamID int32,
	channel string,
	available AvailableImageHandler,
	unavailable UnavailableImageHandler,
) *Subscription {
	subscription := &Subscription{
		registrationID:        registrationID,
		streamID:              streamID,
		channel:               channel,
		availableImageHandler: available,
	}
	subscription.channelStatusCounterID.Store(cnc.NullCounterID)
	subscription.storeUnavailableImageHandler(unavailable)
	return subscription
}

func (subscription *Subscription) applySubscriptionReady(channelStatusCounterID int32) {
	subscription.channelStatusCounterID.Store(channelStatusCounterID)
}

// SetUnavailableImageHandler replaces the unavailable-image callback for this
// subscription.
//
// Behaviour: the next driver OnUnavailableImage for this subscription invokes
// handler. Nil clears the callback. A later call may replace a prior handler.
// A nil receiver is a silent no-op.
//
// Concurrency: see [Subscription]. [UnavailableImageHandler] states where
// the handler runs.
func (subscription *Subscription) SetUnavailableImageHandler(
	handler UnavailableImageHandler,
) {
	if subscription == nil {
		return
	}
	subscription.storeUnavailableImageHandler(handler)
}

func (subscription *Subscription) storeUnavailableImageHandler(
	handler UnavailableImageHandler,
) {
	if handler == nil {
		subscription.unavailableImageHandler.Store(nil)
		return
	}

	subscription.unavailableImageHandler.Store(&handler)
}

func (subscription *Subscription) loadUnavailableImageHandler() UnavailableImageHandler {
	handler := subscription.unavailableImageHandler.Load()
	if handler == nil {
		return nil
	}
	return *handler
}

func (subscription *Subscription) RegistrationID() int64 {
	return subscription.registrationID
}

func (subscription *Subscription) StreamID() int32 {
	return subscription.streamID
}

func (subscription *Subscription) Channel() string {
	return subscription.channel
}

func (subscription *Subscription) ImageCount() int {
	return len(subscription.imagesSnapshot())
}

// ImageAt returns the image at index when the slot holds an open image.
//
// Lifetime: an index holds until the image slice changes. The returned
// Image outlives that change. [Config.ResourceLingerDuration] bounds use of
// its mapped bytes.
func (subscription *Subscription) ImageAt(index int) (*Image, bool) {
	if subscription.closed.Load() {
		return nil, false
	}

	images := subscription.imagesSnapshot()
	if uint(index) >= uint(len(images)) {
		return nil, false
	}
	image := images[index]
	if image == nil || image.IsClosed() {
		return nil, false
	}
	return image, true
}

// ImageBySessionID returns the first open image in slice order whose session
// id equals sessionID.
//
// A closed subscription returns nil, false.
//
// Concurrency: see [Subscription].
func (subscription *Subscription) ImageBySessionID(sessionID int32) (*Image, bool) {
	if subscription.closed.Load() {
		return nil, false
	}
	for _, image := range subscription.imagesSnapshot() {
		if image != nil && !image.IsClosed() && image.SessionID() == sessionID {
			return image, true
		}
	}
	return nil, false
}

// IsConnected reports whether any open image is attached.
//
// Returns false when the subscription is closed, even if images remain in
// the slice.
func (subscription *Subscription) IsConnected() bool {
	if subscription.closed.Load() {
		return false
	}
	for _, image := range subscription.imagesSnapshot() {
		if image != nil && !image.IsClosed() {
			return true
		}
	}
	return false
}

func (subscription *Subscription) IsClosed() bool {
	return subscription.closed.Load()
}

// Poll delivers fragments from attached images in round-robin order. The
// fragmentLimit budget spans the images in rotation order.
//
// Poll reads the image slice at entry. Poll skips an image added during the
// call. Poll may still read an image removed during the call. A reentrant
// Close from a fragment handler does not abort the call.
//
// Returns 0 for a closed subscription.
//
// Concurrency: see [Subscription].
func (subscription *Subscription) Poll(handler FragmentHandler, fragmentLimit int) int {
	return subscription.pollImages(fragmentLimit, func(image *Image, limit int) int {
		return image.Poll(handler, limit)
	})
}

// ControlledPoll delivers fragments from attached images in round-robin
// order. A [ControlledFragmentHandler] controls the position per fragment.
//
// ControlledPoll follows the image slice and budget rules of
// [Subscription.Poll]. [ControlledAbort] and [ControlledBreak] stop only the
// current image's batch. The remaining budget still visits later images.
//
// Returns 0 for a closed subscription.
//
// Concurrency: see [Subscription].
func (subscription *Subscription) ControlledPoll(
	handler ControlledFragmentHandler,
	fragmentLimit int,
) int {
	return subscription.pollImages(fragmentLimit, func(image *Image, limit int) int {
		return image.ControlledPoll(handler, limit)
	})
}

// pollImages walks a snapshot of images in round-robin order.
//
// Closed: returns 0 without advancing roundRobinIndex.
//
// Open: starts at roundRobinIndex, or at 0 when roundRobinIndex is outside
// the image slots. Stores the start plus one in roundRobinIndex. Skips nil
// entries.
func (subscription *Subscription) pollImages(
	fragmentLimit int,
	pollImage func(image *Image, limit int) int,
) int {
	if subscription.closed.Load() {
		return 0
	}

	images := subscription.imagesSnapshot()

	startingIndex := subscription.roundRobinIndex
	if uint(startingIndex) >= uint(len(images)) {
		startingIndex = 0
	}
	subscription.roundRobinIndex = startingIndex + 1

	fragmentsRead := pollImageRange(images[startingIndex:], 0, fragmentLimit, pollImage)
	return pollImageRange(images[:startingIndex], fragmentsRead, fragmentLimit, pollImage)
}

// pollImageRange polls each non-nil image in order until fragmentsRead
// reaches fragmentLimit. Returns the updated fragmentsRead.
func pollImageRange(
	images []*Image,
	fragmentsRead int,
	fragmentLimit int,
	pollImage func(image *Image, limit int) int,
) int {
	for _, image := range images {
		if fragmentsRead >= fragmentLimit {
			break
		}
		if image != nil {
			fragmentsRead += pollImage(image, fragmentLimit-fragmentsRead)
		}
	}
	return fragmentsRead
}

func (subscription *Subscription) imagesSnapshot() []*Image {
	images := subscription.images.Load()
	if images == nil {
		return nil
	}
	return *images
}

func imageIndexIn(images []*Image, correlationID int64) int {
	for index, image := range images {
		if image != nil && image.CorrelationID() == correlationID {
			return index
		}
	}
	return -1
}

func (subscription *Subscription) addImage(image *Image) bool {
	if image == nil {
		return false
	}
	current := subscription.imagesSnapshot()
	if imageIndexIn(current, image.CorrelationID()) >= 0 {
		return false
	}

	next := make([]*Image, len(current)+1)
	copy(next, current)
	next[len(current)] = image
	subscription.images.Store(&next)
	return true
}

// removeImage splices the image out of the slice.
//
// The conductor calls removeImage before Image.internalClose, so in-flight
// Poll snapshots stay valid.
func (subscription *Subscription) removeImage(correlationID int64) *Image {
	current := subscription.imagesSnapshot()
	index := imageIndexIn(current, correlationID)
	if index < 0 {
		return nil
	}

	image := current[index]

	if len(current) == 1 {
		subscription.images.Store(nil)
		return image
	}

	next := make([]*Image, len(current)-1)
	copy(next[:index], current[:index])
	copy(next[index:], current[index+1:])
	subscription.images.Store(&next)
	return image
}

// Close tears down this registration through the bound conductor.
//
// Behaviour: repeated calls return nil.
//
// Concurrency: see [Subscription].
func (subscription *Subscription) Close() error {
	if subscription.closed.Load() {
		return nil
	}
	if subscription.conductor == nil {
		return subscription.markClosed()
	}
	return subscription.conductor.CloseSubscription(subscription)
}

func (subscription *Subscription) markClosed() error {
	subscription.closed.Store(true)
	return nil
}
