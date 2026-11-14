package aeron

// AvailableImageHandler receives each new Image that joins a Subscription.
//
// The handler runs inside Client.DoWork with clientLock held. The handler may
// call Client.Publication, Client.ExclusivePublication, and
// Client.Subscription.
//
// Concurrency: the image joins the subscription before the handler runs. A
// poller on another goroutine can read fragments from the image before the
// handler returns.
//
// Client calls from the handler follow the [Client] handler rule.
//
// While a handler runs, the conductor does not deliver nested available or
// unavailable image handlers. It still consumes the driver event. Java Aeron
// delivers nested image handlers.
//
// Client.DoWork recovers a panic in the handler, unlike a panic in an
// Image.Poll fragment handler. A non-nil Config.ErrorHandler receives the
// recovered value. With a nil ErrorHandler, the conductor discards the panic
// and continues.
type AvailableImageHandler func(image *Image)

// UnavailableImageHandler is invoked after an Image is removed from its
// Subscription and internalClose has run.
//
// The handler runs on driver OnUnavailableImage events during Client.DoWork.
// Subscription Close via RetireSubscription removes and closes images without
// invoking this callback.
//
// The Image passed to the handler is closed; Poll returns zero. Log buffer
// release is scheduled; the handler must not assume the mmap remains mapped.
//
// The lock and Client call rules match AvailableImageHandler.
//
// Panic handling matches AvailableImageHandler.
type UnavailableImageHandler func(image *Image)
