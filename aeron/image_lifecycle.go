package aeron

import (
	"errors"
	"fmt"

	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/conductor"
	"github.com/warrenguy/luas/aeron/logbuffer"
)

type conductorImageLifecycle struct{}

// ConductorImageLifecycle wires conductor image events to aeron types.
var ConductorImageLifecycle conductor.ImageLifecycle = conductorImageLifecycle{}

func (conductorImageLifecycle) HasImage(
	sub conductor.SubscriptionHandle,
	correlationID int64,
) bool {
	subscription, err := subscriptionFromHandle(sub)
	if err != nil {
		return false
	}
	return imageIndexIn(subscription.imagesSnapshot(), correlationID) >= 0
}

func (conductorImageLifecycle) NewImage(
	sub conductor.SubscriptionHandle,
	event conductor.AvailableImage,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	errorHandler func(error),
) (conductor.ImageHandle, error) {
	subscription, err := subscriptionFromHandle(sub)
	if err != nil {
		return nil, err
	}
	return newImage(subscription, event, logBuffers, countersReader, errorHandler)
}

func (conductorImageLifecycle) AddImage(
	sub conductor.SubscriptionHandle,
	image conductor.ImageHandle,
) bool {
	subscription, err := subscriptionFromHandle(sub)
	if err != nil {
		return false
	}
	img, ok := imageFromHandle(image)
	if !ok {
		return false
	}
	return subscription.addImage(img)
}

func (conductorImageLifecycle) RemoveImage(
	sub conductor.SubscriptionHandle,
	correlationID int64,
) conductor.ImageHandle {
	subscription, err := subscriptionFromHandle(sub)
	if err != nil {
		return nil
	}
	image := subscription.removeImage(correlationID)
	if image == nil {
		return nil
	}
	return image
}

func (conductorImageLifecycle) CloseImage(image conductor.ImageHandle) {
	img, ok := imageFromHandle(image)
	if !ok {
		return
	}
	img.internalClose()
}

func (conductorImageLifecycle) AvailableImageHandler(
	sub conductor.SubscriptionHandle,
) func(conductor.ImageHandle) {
	return subscriptionImageHandler(sub, func(subscription *Subscription) func(*Image) {
		return subscription.availableImageHandler
	})
}

func (conductorImageLifecycle) UnavailableImageHandler(
	sub conductor.SubscriptionHandle,
) func(conductor.ImageHandle) {
	return subscriptionImageHandler(sub, func(subscription *Subscription) func(*Image) {
		return subscription.loadUnavailableImageHandler()
	})
}

func subscriptionImageHandler(
	sub conductor.SubscriptionHandle,
	handler func(*Subscription) func(*Image),
) func(conductor.ImageHandle) {
	subscription, err := subscriptionFromHandle(sub)
	if err != nil {
		return nil
	}
	return wrapImageHandler(handler(subscription))
}

func typedHandleFrom[T comparable](handle any, kind, typeName string) (T, error) {
	var zero T
	if handle == nil {
		return zero, errors.New("aeron: " + kind + " handle is nil")
	}
	typed, ok := handle.(T)
	if !ok {
		return zero, fmt.Errorf("aeron: %s handle %T is not %s", kind, handle, typeName)
	}
	if typed == zero {
		return zero, errors.New("aeron: " + kind + " handle is nil")
	}
	return typed, nil
}

func subscriptionFromHandle(sub conductor.SubscriptionHandle) (*Subscription, error) {
	return typedHandleFrom[*Subscription](sub, "subscription", "*Subscription")
}

func bindSubscriptionHandle(
	sub conductor.SubscriptionHandle,
	clientConductor *conductor.Conductor,
	client *Client,
) error {
	subscription, err := subscriptionFromHandle(sub)
	if err != nil {
		return err
	}
	subscription.conductor = clientConductor
	subscription.counters = clientConductor.CountersReader()
	subscription.client = client
	return nil
}

func publicationFromHandle(pub conductor.PublicationHandle) (*Publication, error) {
	return typedHandleFrom[*Publication](pub, "publication", "*Publication")
}

func exclusivePublicationFromHandle(
	pub conductor.PublicationHandle,
) (*ExclusivePublication, error) {
	return typedHandleFrom[*ExclusivePublication](
		pub,
		"exclusive publication",
		"*ExclusivePublication",
	)
}

func imageFromHandle(image conductor.ImageHandle) (*Image, bool) {
	if image == nil {
		return nil, false
	}
	img, ok := image.(*Image)
	if !ok || img == nil {
		return nil, false
	}
	return img, true
}

func wrapImageHandler(handler func(*Image)) func(conductor.ImageHandle) {
	if handler == nil {
		return nil
	}
	return func(image conductor.ImageHandle) {
		img, ok := imageFromHandle(image)
		if !ok {
			return
		}
		handler(img)
	}
}
