package aeron

import (
	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/conductor"
	"github.com/warrenguy/luas/aeron/logbuffer"
)

type conductorResourceLifecycle struct {
	availableImageHandler   AvailableImageHandler
	unavailableImageHandler UnavailableImageHandler
	client                  *Client
}

// ConductorResourceLifecycle is the nil-handler default for tests and internal fixtures.
var ConductorResourceLifecycle conductor.ResourceLifecycle = conductorResourceLifecycle{}

func (conductorResourceLifecycle) NewPublication(
	event conductor.PublicationBuffersReady,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	channel string,
) (conductor.PublicationHandle, error) {
	publication, err := newPublicationFromReady(
		event,
		logBuffers.MappedLogBuffer(),
		countersReader,
		channel,
	)
	if err != nil {
		return nil, err
	}
	return publication, nil
}

func (conductorResourceLifecycle) NewExclusivePublication(
	event conductor.PublicationBuffersReady,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	channel string,
) (conductor.PublicationHandle, error) {
	publication, err := newExclusivePublicationFromReady(
		event,
		logBuffers.MappedLogBuffer(),
		countersReader,
		channel,
	)
	if err != nil {
		return nil, err
	}
	return publication, nil
}

func (lifecycle conductorResourceLifecycle) NewPreSubscription(
	registrationID int64,
	streamID int32,
	channel string,
) conductor.SubscriptionHandle {
	return newSubscription(
		registrationID,
		streamID,
		channel,
		lifecycle.availableImageHandler,
		lifecycle.unavailableImageHandler,
	)
}

func (conductorResourceLifecycle) ApplySubscriptionReady(
	sub conductor.SubscriptionHandle,
	event conductor.SubscriptionReady,
) error {
	subscription, err := subscriptionFromHandle(sub)
	if err != nil {
		return err
	}
	subscription.applySubscriptionReady(event.ChannelStatusCounterID)
	return nil
}

func (conductorResourceLifecycle) RetireSubscription(
	sub conductor.SubscriptionHandle,
) ([]string, error) {
	subscription, err := subscriptionFromHandle(sub)
	if err != nil {
		return nil, err
	}
	images := subscription.imagesSnapshot()
	subscription.images.Store(nil)

	registryKeys := make([]string, 0, len(images))
	for _, image := range images {
		if image == nil {
			continue
		}
		registryKeys = append(registryKeys, image.LogFileName())
		image.internalClose()
	}
	return registryKeys, subscription.markClosed()
}

func (conductorResourceLifecycle) RetirePublication(
	pub conductor.PublicationHandle,
) (string, error) {
	return retirePublicationHandle(pub)
}

func (lifecycle conductorResourceLifecycle) BindPublication(
	pub conductor.PublicationHandle,
	clientConductor *conductor.Conductor,
) error {
	return bindPublicationHandle(pub, clientConductor, lifecycle.client)
}

func (lifecycle conductorResourceLifecycle) BindSubscription(
	sub conductor.SubscriptionHandle,
	clientConductor *conductor.Conductor,
) error {
	return bindSubscriptionHandle(sub, clientConductor, lifecycle.client)
}

func (conductorResourceLifecycle) MarkPublicationClosed(pub conductor.PublicationHandle) {
	markPublicationHandleClosed(pub)
}

func (conductorResourceLifecycle) MarkSubscriptionClosed(sub conductor.SubscriptionHandle) {
	subscription, err := subscriptionFromHandle(sub)
	if err != nil {
		return
	}
	_ = subscription.markClosed()
}
