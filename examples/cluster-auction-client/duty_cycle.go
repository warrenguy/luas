//go:build example

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"time"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/atomicbuffer"
	"github.com/warrenguy/luas/cluster/client"
	"github.com/warrenguy/luas/examples/cluster-auction-client/auctionwire"
)

var auctionLog = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds)

// newEgressHandlers decodes auction egress bids and logs cluster session events.
func newEgressHandlers(clientState *ClientState) *client.EgressHandlers {
	return &client.EgressHandlers{
		MessageFunc: func(
			clusterSessionID int64,
			_ int64,
			buf atomicbuffer.AtomicBuffer,
			offset int,
			length int,
			_ *aeron.Header,
		) {
			bid, ok := auctionwire.DecodeEgress(buf, offset, length)
			if !ok {
				log.Printf(
					"cluster-auction-client: egress decode failed (length %d, need %d)",
					length,
					auctionwire.EgressMessageLength,
				)
				return
			}
			clientState.recordEgress(bid)
			auctionLog.Printf(
				"SessionMessage(%d, %d,%d, %d, %t)",
				clusterSessionID,
				bid.CorrelationID,
				bid.CustomerID,
				bid.Price,
				bid.BidSucceeded,
			)
		},
		SessionEventFunc: func(
			correlationID int64,
			_ int64,
			leadershipTermID int64,
			leaderMemberID int32,
			code client.EventCode,
			detail []byte,
		) {
			auctionLog.Printf(
				"SessionEvent(%d, %d, %d, %s, %s)",
				correlationID,
				leadershipTermID,
				leaderMemberID,
				code,
				detail,
			)
		},
		NewLeaderFunc: func(
			clusterSessionID int64,
			leadershipTermID int64,
			leaderMemberID int32,
			_ []byte,
		) {
			auctionLog.Printf(
				"NewLeader(%d, %d, %d)",
				clusterSessionID,
				leadershipTermID,
				leaderMemberID,
			)
		},
	}
}

func runDutyCycle(
	ctx context.Context,
	session *client.AeronCluster,
	config Config,
	clientState *ClientState,
) error {
	// Loop until all bids are sent. Offer bids on a random schedule and send
	// keepalive between bids. The background conductor drives the transport.
	offerConfig := client.IngressOfferConfig{}
	clientState.nextBidDeadline = time.Now().Add(randomDelayMs(config.BidIntervalMs))
	clientState.bidsLeftToSend = config.NumBids

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		now := time.Now()

		if clientState.bidDue(now) && clientState.bidsLeftToSend > 0 {
			// Bid slightly above the last price reported on egress.
			price := clientState.lastBidSeen + int64(rand.IntN(10))
			correlationID := clientState.nextCorrelation()
			err := sendBid(ctx, session, config, correlationID, price, offerConfig)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("cluster-auction-client: bid offer: %w", err)
			}
			completed := time.Now()
			clientState.nextBidDeadline = completed.Add(randomDelayMs(config.BidIntervalMs))
			clientState.keepAliveDeadline = completed.Add(time.Second)
			clientState.bidsLeftToSend--
			auctionLog.Printf(
				"Sent(%d, %d, %d) bidsRemaining=%d",
				correlationID,
				config.CustomerID,
				price,
				clientState.bidsLeftToSend,
			)
		} else if clientState.keepAliveDue(now) {
			// Send ingress traffic while waiting between bids so the session stays open.
			if clientState.bidsLeftToSend <= 0 {
				break
			}
			_, err := session.SendKeepAliveUntilPositive(ctx, offerConfig)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("cluster-auction-client: keepalive: %w", err)
			}
			clientState.keepAliveDeadline = time.Now().Add(time.Second)
		}

		// Poll egress once. Yield when nothing arrived.
		session.SpinOnce()
	}

	return nil
}

func sendBid(
	ctx context.Context,
	session *client.AeronCluster,
	config Config,
	correlationID int64,
	price int64,
	offerConfig client.IngressOfferConfig,
) error {
	var buf [auctionwire.BidMessageLength]byte
	auctionwire.EncodeBid(buf[:], correlationID, config.CustomerID, price)
	_, err := session.IngressOfferUntilPositive(
		ctx,
		offerConfig,
		func() int64 {
			return session.Offer(buf[:])
		},
	)
	return err
}

func randomDelayMs(maxMs int) time.Duration {
	return time.Duration(rand.IntN(maxMs)) * time.Millisecond
}
