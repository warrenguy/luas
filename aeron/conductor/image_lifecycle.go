package conductor

import (
	"errors"
	"fmt"

	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/logbuffer"
)

var (
	errImageAttachFailed             = errors.New("conductor: failed to attach image")
	errImageCloseFailedBeforeRelease = errors.New(
		"conductor: image close failed before log buffer release",
	)
)

// SubscriptionHandle and ImageHandle break the conductor and aeron import cycle.
type SubscriptionHandle interface {
	RegistrationID() int64
	StreamID() int32
}

type ImageHandle interface {
	CorrelationID() int64
	LogFileName() string
	IsClosed() bool
}

// ImageLifecycle implements attach/detach in package aeron (ConductorImageLifecycle).
type ImageLifecycle interface {
	HasImage(sub SubscriptionHandle, correlationID int64) bool
	// NewImage: event.LogFileName is the registry acquire key; preserve it on the handle.
	NewImage(
		sub SubscriptionHandle,
		event AvailableImage,
		logBuffers *logbuffer.LogBuffers,
		countersReader *cnc.CountersReader,
		errorHandler func(error),
	) (ImageHandle, error)
	AddImage(sub SubscriptionHandle, image ImageHandle) bool
	RemoveImage(sub SubscriptionHandle, correlationID int64) ImageHandle
	CloseImage(image ImageHandle)
	AvailableImageHandler(sub SubscriptionHandle) func(ImageHandle)
	UnavailableImageHandler(sub SubscriptionHandle) func(ImageHandle)
}

func (conductor *Conductor) subscriptionForImageEvent(
	registrationID int64,
) (SubscriptionHandle, bool) {
	if conductor.imageLifecycle == nil {
		return nil, false
	}
	return conductor.subscriptionByRegID(registrationID)
}

func (conductor *Conductor) handleAvailableImage(event AvailableImage) {
	err := underResourceWriteLockResult(conductor, func() error {
		sub, ok := conductor.subscriptionForImageEvent(event.SubscriptionRegistrationID)
		if !ok {
			return nil
		}

		if conductor.imageLifecycle.HasImage(sub, event.CorrelationID) {
			return nil
		}

		if conductor.config.MediaDriverDir == "" {
			return ErrMediaDriverDirRequired
		}

		resolvedLogFileKey, err := conductor.logBuffersRegistry.resolveLogFileKey(event.LogFileName)
		if err != nil {
			return err
		}

		logBuffers, err := conductor.acquireLogBuffersByKey(resolvedLogFileKey)
		if err != nil {
			return err
		}

		event.LogFileName = resolvedLogFileKey
		image, err := conductor.imageLifecycle.NewImage(
			sub,
			event,
			logBuffers,
			conductor.countersReader,
			conductor.config.SubscriberErrorHandler,
		)
		if err != nil {
			conductor.releaseLogBuffersByKey(resolvedLogFileKey, conductor.config.ResourceLingerNs)
			return err
		}

		if !conductor.imageLifecycle.AddImage(sub, image) {
			conductor.imageLifecycle.CloseImage(image)
			conductor.releaseLogBuffersByKey(resolvedLogFileKey, conductor.config.ResourceLingerNs)
			return errImageAttachFailed
		}

		if handler := conductor.imageLifecycle.AvailableImageHandler(sub); handler != nil {
			conductor.invokeImageHandler(func() {
				handler(image)
			})
		}
		return nil
	})
	conductor.ReportError(err)
}

func (conductor *Conductor) handleUnavailableImage(
	correlationID, subscriptionRegistrationID int64,
) {
	err := underResourceWriteLockResult(conductor, func() error {
		sub, ok := conductor.subscriptionForImageEvent(subscriptionRegistrationID)
		if !ok {
			return nil
		}

		image := conductor.imageLifecycle.RemoveImage(sub, correlationID)
		if image == nil {
			return nil
		}

		conductor.imageLifecycle.CloseImage(image)
		if !image.IsClosed() {
			return errImageCloseFailedBeforeRelease
		}
		registryKey := image.LogFileName()
		conductor.releaseLogBuffersByKey(registryKey, conductor.config.ResourceLingerNs)

		if handler := conductor.imageLifecycle.UnavailableImageHandler(sub); handler != nil {
			conductor.invokeImageHandler(func() {
				handler(image)
			})
		}
		return nil
	})
	conductor.ReportError(err)
}

func (conductor *Conductor) invokeImageHandler(handler func()) {
	// Image handlers suspend the write lock so lookups and resource Close
	// can lock resourceMu. Other goroutines may read or write the resource
	// maps during the suspend window. A handler must not assume
	// exclusive map access.
	//
	// A Client.DoWork call from the handler returns before another service
	// tick. The call does not enter the conductor. inCallback still skips
	// nested image handlers if doWorkLocked runs again. Java ClientConductor
	// still delivers nested available and unavailable handlers.
	if conductor.inCallback {
		return
	}
	conductor.inCallback = true
	savedDepth := conductor.suspendResourceWriteLock()
	entered := conductor.enterHandler()

	// Defer exitHandler first so it runs last. The panic report stays in
	// scope. An ErrorHandler panic still exits the scope.
	defer conductor.exitHandler(entered)
	defer func() {
		conductor.reportHandlerPanic(recover(), "image handler")
		conductor.resumeResourceWriteLock(savedDepth)
		conductor.inCallback = false
	}()
	handler()
}

// reportHandlerPanic sends a recovered user handler panic to ErrorHandler.
// label names the handler in a non-error panic value. Nil recovered is a
// no-op.
func (conductor *Conductor) reportHandlerPanic(recovered any, label string) {
	if recovered == nil {
		return
	}
	err, ok := recovered.(error)
	if !ok {
		err = fmt.Errorf("conductor: %s panic: %v", label, recovered)
	}
	conductor.ReportError(err)
}
