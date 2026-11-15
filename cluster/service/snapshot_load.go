package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/archive"
	"github.com/warrenguy/luas/atomicbuffer"
	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/cluster/codecs/clusterwire"
	"github.com/warrenguy/luas/idlestrategy"
)

// snapshotLoadFragmentLimit matches upstream ServiceSnapshotLoader
// FRAGMENT_LIMIT.
const snapshotLoadFragmentLimit = 10

// snapshotReplayControl is the archive client surface a snapshot load
// uses. Poll drains the control-response subscription. The archive pings
// that session once a second and aborts it after five seconds of failed
// pings, so a long replay must keep draining it.
type snapshotReplayControl interface {
	Poll() int
	StartReplay(
		ctx context.Context,
		recordingID int64,
		position int64,
		length int64,
		replayStreamID int32,
		replayChannel []byte,
	) (int64, error)
	StopReplay(ctx context.Context, replaySessionID int64) error
}

var _ snapshotReplayControl = (*archive.Client)(nil)

// snapshotLoad holds the collaborators of one snapshot load.
type snapshotLoad struct {
	control       snapshotReplayControl
	subscriptions SubscriptionAdder
	pump          func() int
	channel       string
	streamID      int32
	timeout       time.Duration
	handler       LoadSnapshotHandler // nil unless the service implements it
}

// run replays recordingID into agent. Every snapshot marker must carry
// logPosition.
//
// Order:
//  1. start the replay from position 0 with length NULL_VALUE
//  2. add the replay subscription on the session-scoped channel
//  3. await the replay image
//  4. poll the loader until the end marker
//  5. call LoadSnapshot when the service implements it
//  6. stop the replay, then close the replay subscription
//
// Returns an error when any step fails. run closes every resource it opens
// before it returns.
func (l snapshotLoad) run(agent *Agent, recordingID, logPosition int64) error {
	ctx := context.Background()
	replaySessionID, err := l.control.StartReplay(
		ctx,
		recordingID,
		0,
		nullValue,
		l.streamID,
		[]byte(l.channel),
	)
	if err != nil {
		return fmt.Errorf("%w: start replay: %w", errSnapshotLoad, err)
	}

	//nolint:gosec // G115: the image carries the low 32 bits of the session id
	channel := sessionScopedChannel(l.channel, int32(uint32(replaySessionID)))
	sub, err := l.subscriptions.AddSubscription(l.streamID, channel)
	sub, err = addedHandle(sub, err, errAdderReturnedNoSubscription)
	if err != nil {
		return errors.Join(
			fmt.Errorf("%w: add replay subscription: %w", errSnapshotLoad, err),
			l.stopReplay(ctx, replaySessionID),
		)
	}

	err = l.load(agent, sub, replaySessionID, logPosition)
	err = errors.Join(err, l.stopReplay(ctx, replaySessionID))
	if closeErr := sub.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf(
			"%w: close replay subscription: %w", errSnapshotLoad, closeErr,
		))
	}
	return err
}

func (l snapshotLoad) stopReplay(ctx context.Context, replaySessionID int64) error {
	if err := l.control.StopReplay(ctx, replaySessionID); err != nil {
		return fmt.Errorf("%w: stop replay: %w", errSnapshotLoad, err)
	}
	return nil
}

// load runs steps 3 to 5 of [snapshotLoad.run] on sub.
func (l snapshotLoad) load(
	agent *Agent,
	sub *aeron.Subscription,
	replaySessionID int64,
	logPosition int64,
) error {
	idle := idlestrategy.NewDefaultBackoff()
	image, err := l.awaitImage(sub, replaySessionID, idle)
	if err != nil {
		return err
	}

	loader := newServiceSnapshotLoader(agent, logPosition)
	deadline := time.Now().Add(l.timeout)
	for {
		// An invoker transport reports an unavailable image only on DoWork.
		l.pump()
		l.control.Poll()
		fragments := image.ControlledPoll(loader.handler, snapshotLoadFragmentLimit)
		switch {
		case loader.err != nil:
			return loader.err
		case loader.done:
			return l.callHandler(image)
		case fragments > 0:
			deadline = time.Now().Add(l.timeout)
		case image.IsClosed() || image.IsEndOfStream():
			return fmt.Errorf("%w: replay ended before the end marker", errSnapshotLoad)
		case !time.Now().Before(deadline):
			return fmt.Errorf(
				"%w: no replay fragment within %s", errSnapshotLoad, l.timeout,
			)
		}
		idle.Idle(fragments)
	}
}

// awaitImage returns the replay image of replaySessionID on sub.
//
// Returns an error when the lookup fails or the image does not appear within
// the load timeout.
func (l snapshotLoad) awaitImage(
	sub *aeron.Subscription,
	replaySessionID int64,
	idle idlestrategy.IdleStrategy,
) (*aeron.Image, error) {
	deadline := time.Now().Add(l.timeout)
	for {
		image, found, err := archive.FindReplayImage(sub, replaySessionID)
		switch {
		case err != nil:
			return nil, fmt.Errorf("%w: %w", errSnapshotLoad, err)
		case found:
			return image, nil
		}
		idle.Idle(l.pump() + l.control.Poll())
		if !time.Now().Before(deadline) {
			return nil, fmt.Errorf(
				"%w: replay image not found within %s", errSnapshotLoad, l.timeout,
			)
		}
	}
}

func (l snapshotLoad) callHandler(image *aeron.Image) error {
	if l.handler == nil {
		return nil
	}
	if err := l.handler.LoadSnapshot(image); err != nil {
		return fmt.Errorf("%w: LoadSnapshot: %w", errSnapshotLoad, err)
	}
	return nil
}

// serviceSnapshotLoader restores a service snapshot into agent.
//
// Behaviour: the loader accepts BEGIN, client session rows, SECTION and
// NULL_VALUE marks, then END. Any other message sequence fails the load.
// Errors format every enum as an integer, so an undefined wire value does
// not panic.
type serviceSnapshotLoader struct {
	agent       *Agent
	logPosition int64
	inSnapshot  bool
	done        bool
	err         error
	timeUnit    clusterwire.ClusterTimeUnit // from BEGIN
	appVersion  int32                       // from BEGIN
	handler     aeron.ControlledFragmentHandler
}

// newServiceSnapshotLoader builds a loader behind a fragment assembler. A
// client session row above the max payload arrives in fragments.
func newServiceSnapshotLoader(agent *Agent, logPosition int64) *serviceSnapshotLoader {
	l := &serviceSnapshotLoader{
		agent:       agent,
		logPosition: logPosition,
	}
	l.handler = aeron.NewControlledFragmentAssembler(l.handleMessage).Handler()
	return l
}

func (l *serviceSnapshotLoader) handleMessage(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	_ *aeron.Header,
) aeron.ControlledAction {
	raw := buffer.Bytes()
	if offset < 0 || length < 0 || offset > len(raw)-length {
		return l.failf("fragment offset %d length %d outside the buffer", offset, length)
	}
	return l.dispatch(raw[offset : offset+length])
}

// dispatch applies one reassembled message.
//
// Returns ControlledBreak at END, ControlledAbort on an error, and
// ControlledContinue otherwise.
func (l *serviceSnapshotLoader) dispatch(frag []byte) aeron.ControlledAction {
	layout, ok := codecs.ClusterMessageLayoutAt(frag, 0, 0)
	if !ok {
		return l.failLayout(frag)
	}
	if layout.TemplateID == clusterwire.SnapshotMarkerSbeTemplateID {
		return l.handleMarker(frag, layout)
	}
	if !l.inSnapshot {
		return l.failf("template %d before the begin marker", layout.TemplateID)
	}
	if layout.TemplateID != clusterwire.ClientSessionSbeTemplateID {
		return l.failf("template %d inside the snapshot", layout.TemplateID)
	}
	fields, ok := codecs.TryDecodeClientSessionAt(frag, 0)
	if !ok {
		return l.failf("client session row does not decode")
	}
	if err := l.agent.restoreClientSession(fields); err != nil {
		return l.failf("client session %d repeated", fields.ClusterSessionID)
	}
	return aeron.ControlledContinue
}

func (l *serviceSnapshotLoader) handleMarker(
	frag []byte,
	layout codecs.ClusterMessageLayout,
) aeron.ControlledAction {
	fields, ok := codecs.SnapshotMarkerFieldsFromLayout(frag, layout)
	switch {
	case !ok:
		return l.failf("snapshot marker does not decode")
	case fields.TypeID != codecs.ServiceSnapshotTypeID:
		return l.failf(
			"snapshot type id %d, want %d", fields.TypeID, codecs.ServiceSnapshotTypeID,
		)
	case fields.LogPosition != l.logPosition:
		return l.failf("marker log position %d, want %d", fields.LogPosition, l.logPosition)
	}

	switch fields.Mark {
	case clusterwire.SnapshotMark_BEGIN:
		if l.inSnapshot {
			return l.failf("begin marker repeated")
		}
		timeUnit, known := resolveTimeUnit(fields.TimeUnit)
		if !known {
			return l.failf("time unit %d unknown", int32(fields.TimeUnit))
		}
		l.timeUnit = timeUnit
		// The container does not configure an app version. The loader does
		// not check the value. The next snapshot take reuses it.
		l.appVersion = fields.AppVersion
		l.inSnapshot = true
		return aeron.ControlledContinue
	case clusterwire.SnapshotMark_END:
		if !l.inSnapshot {
			return l.failf("end marker before the begin marker")
		}
		l.done = true
		l.agent.timeUnit = l.timeUnit
		l.agent.appVersion = l.appVersion
		return aeron.ControlledBreak
	case clusterwire.SnapshotMark_SECTION, clusterwire.SnapshotMark_NULL_VALUE:
		if !l.inSnapshot {
			return l.failf("snapshot mark %d before the begin marker", int32(fields.Mark))
		}
		return aeron.ControlledContinue
	}
	return l.failf("snapshot mark %d unknown", int32(fields.Mark))
}

// failLayout fails a message whose layout does not parse.
func (l *serviceSnapshotLoader) failLayout(frag []byte) aeron.ControlledAction {
	_, templateID, schemaID, _, ok := codecs.DecodeMessageHeader(frag, 0)
	switch {
	case !ok:
		return l.failf("message header short, length %d", len(frag))
	case schemaID != clusterwire.MessageHeaderSbeSchemaID:
		return l.failf("schema id %d, want %d", schemaID, clusterwire.MessageHeaderSbeSchemaID)
	}
	return l.failf("template %d does not decode", templateID)
}

func (l *serviceSnapshotLoader) failf(format string, args ...any) aeron.ControlledAction {
	l.err = fmt.Errorf("%w: %s", errSnapshotLoad, fmt.Sprintf(format, args...))
	return aeron.ControlledAbort
}
