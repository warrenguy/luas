//go:build example

// ipc-duty-cycle is a long-running IPC pub/sub sample that drives Client.DoWork
// and Subscription.Poll on one invoker goroutine (no resource wait/offer helpers).
//
// Prerequisites: run aeronmd with AERON_DIR set to the same directory this
// process uses (see README).
//
// Flow: register a publication and subscription on one IPC stream, then loop
// DoWork / Offer / Poll until SIGINT/SIGTERM or media-driver loss. Run:
// go run -tags=example ./examples/ipc-duty-cycle/
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/idlestrategy"
)

const (
	streamID      = 1001
	channel       = "aeron:ipc"
	pollLimit     = 10
	idleSleep     = time.Millisecond
	offerInterval = time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Must match the media driver directory; default suits local aeronmd from README.
	aeronDir := cmp.Or(os.Getenv("AERON_DIR"), "/tmp/aeron")

	// UseConductorInvoker keeps DoWork on this goroutine. ErrorHandler runs
	// inside DoWork and must not call back into Client.
	client, err := aeron.Connect(ctx, aeron.Config{
		AeronDir:            aeronDir,
		UseConductorInvoker: true,
		ErrorHandler: func(err error) {
			log.Printf("ipc-duty-cycle: aeron error: %v", err)
		},
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Fatalf("ipc-duty-cycle: connect: %v", err)
	}
	defer closeWithLog(client.Close)

	pub, err := client.AddPublication(streamID, channel)
	if err != nil {
		log.Fatalf("ipc-duty-cycle: add publication: %v", err)
	}
	defer closeWithLog(pub.Close)

	sub, err := client.AddSubscription(streamID, channel)
	if err != nil {
		log.Fatalf("ipc-duty-cycle: add subscription: %v", err)
	}
	defer closeWithLog(sub.Close)

	var received int
	handler := aeron.AdaptFragmentHandlerBytes(func(payload []byte, _ *aeron.Header) {
		received++
		log.Printf("ipc-duty-cycle: received %q (n=%d)", payload, received)
	})

	// Single invoker owns DoWork + Offer + Poll.
	idle := idlestrategy.NewSleeping(idleSleep)
	nextOffer := time.Now()
	var seq int

	log.Printf("ipc-duty-cycle: running (Ctrl-C or driver loss to stop)")

	for ctx.Err() == nil {
		work := client.DoWork()
		if client.IsTerminating() {
			stop()
			break
		}

		// Pace offers (~1/s). Retriable Offer codes (e.g. NotConnected) leave
		// nextOffer alone so the same seq retries after more DoWork.
		now := time.Now()
		if !now.Before(nextOffer) {
			payload := fmt.Appendf(nil, "hello-%d", seq)
			result := pub.Offer(payload)
			if result > 0 {
				log.Printf("ipc-duty-cycle: offered %q at position %d", payload, result)
				seq++
				nextOffer = now.Add(offerInterval)
				work++
			} else if !aeron.IsOfferRetriable(result) {
				log.Printf(
					"ipc-duty-cycle: offer terminal: %s",
					aeron.PositionCodeName(result),
				)
				stop()
				break
			}
		}

		work += sub.Poll(handler, pollLimit)
		idle.Idle(work)
	}

	log.Printf("ipc-duty-cycle: stop (%v); offered=%d received=%d", ctx.Err(), seq, received)
}

func closeWithLog(closeFn func() error) {
	if err := closeFn(); err != nil {
		log.Print(err)
	}
}
