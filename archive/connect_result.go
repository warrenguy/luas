package archive

import (
	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/internal/transportfault"
)

// ConnectResult holds archive control handles and session ids from a
// successful connect handshake. AsyncConnect.Poll transfers ownership of
// connect resources to the result. AsyncConnect.Close documents cleanup on
// success and on failure.
//
// Proxy Try methods and Poller.Poll do not call DoWork. With an invoker-mode
// client, the caller calls Aeron.DoWork between poll and offer steps, or uses
// [Client.ControlOfferUntilPositive] for offer retries.
type ConnectResult struct {
	ControlSessionID int64
	ArchiveID        int64
	ServerVersion    int32

	ControlPublication  ControlPublication
	ControlSubscription *aeron.Subscription

	Proxy  *Proxy
	Poller *ControlResponsePoller

	Aeron           *aeron.Client
	OwnsAeronClient bool
	Config          Config

	// transportFault reads the termination cause of a connect-owned client.
	// It is nil when the caller supplied Config.Aeron.
	transportFault *transportfault.Monitor
}
