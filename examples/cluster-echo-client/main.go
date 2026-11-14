//go:build example

// cluster-echo-client sends a ping on ingress. It resends the ping every
// 250 ms until the same bytes arrive on egress.
//
// Ingress and egress default to IPC. CLUSTER_INGRESS_CHANNEL,
// CLUSTER_INGRESS_ENDPOINTS, and CLUSTER_EGRESS_CHANNEL override the
// defaults.
//
// Prerequisites: see README. The Go client shares AERON_DIR with the media
// driver. Do not start a separate aeronmd for the IPC default. Run:
// go run -tags=example ./examples/cluster-echo-client/
package main

import (
	"bytes"
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
	"github.com/warrenguy/luas/cluster/client"
)

const (
	defaultAeronDir = "/dev/shm/aeron"
	echoTimeout     = 30 * time.Second
	resendInterval  = 250 * time.Millisecond
	ipcChannel      = "aeron:ipc?term-length=64k"
)

var pingPayload = []byte("luas-cluster-echo-ping")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	aeronDir := cmp.Or(os.Getenv("AERON_DIR"), defaultAeronDir)
	ingressChannel := cmp.Or(os.Getenv("CLUSTER_INGRESS_CHANNEL"), ipcChannel)
	ingressEndpoints := os.Getenv("CLUSTER_INGRESS_ENDPOINTS")
	egressChannel := cmp.Or(os.Getenv("CLUSTER_EGRESS_CHANNEL"), ipcChannel)

	// Connect to the media driver the cluster member uses.
	//
	// ErrorHandler logs asynchronous driver faults not returned by the calls below.
	transport, err := aeron.Connect(ctx, aeron.Config{
		AeronDir: aeronDir,
		ErrorHandler: func(err error) {
			log.Printf("cluster-echo-client: aeron error: %v", err)
		},
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Fatalf("cluster-echo-client: connect: %v", err)
	}
	defer closeWithLog(transport.Close)

	var echoed []byte

	// Open a cluster client session. EgressHandler keeps the first message
	// the cluster returns on egress.
	session, err := client.Connect(ctx, client.Config{
		Aeron:            transport,
		AeronDir:         aeronDir,
		IngressChannel:   ingressChannel,
		IngressEndpoints: ingressEndpoints,
		EgressChannel:    egressChannel,
		EgressHandler: &client.EgressHandlers{
			MessageBytesFunc: func(_, _ int64, payload []byte, _ *aeron.Header) {
				if len(echoed) == 0 {
					echoed = bytes.Clone(payload)
				}
			},
		},
		ClientName:     "cluster-echo-client",
		MessageTimeout: 60 * time.Second,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Fatal(err)
	}
	defer closeWithLog(session.Close)

	echoCtx, cancel := context.WithTimeout(ctx, echoTimeout)
	defer cancel()

	var sentAt time.Time

	// Poll the session until the ping comes back on egress. The service drops
	// a reply while its response publication connects, so resend the ping
	// every resendInterval.
	for len(echoed) == 0 {
		if err := echoCtx.Err(); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Fatalf(
				"cluster-echo-client: echo timeout after %s (sessionState=%v)",
				echoTimeout,
				session.SessionState(),
			)
		}

		if time.Since(sentAt) >= resendInterval {
			sendPing(echoCtx, session)
			sentAt = time.Now()
		}

		session.SpinOnce()
	}

	if !bytes.Equal(echoed, pingPayload) {
		log.Fatalf("cluster-echo-client: echo mismatch: got %q want %q", echoed, pingPayload)
	}
	fmt.Printf("cluster-echo-client: echo ok (%q)\n", echoed)
}

// sendPing offers the ping on cluster ingress. It retries while the stream
// is full or busy. It returns when ctx ends.
func sendPing(ctx context.Context, session *client.AeronCluster) {
	pos, err := session.IngressOfferUntilPositive(ctx, client.IngressOfferConfig{}, func() int64 {
		return session.Offer(pingPayload)
	})
	if err != nil {
		// The poll loop reports a context end.
		if ctx.Err() != nil {
			return
		}

		log.Fatalf("cluster-echo-client: ingress offer failed: %v", err)
	}

	log.Printf("cluster-echo-client: offered ping at position %d", pos)
}

func closeWithLog(closeFn func() error) {
	if err := closeFn(); err != nil {
		log.Print(err)
	}
}
