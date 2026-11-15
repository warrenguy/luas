package client

import (
	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/internal/transportfault"
)

// ConnectResult holds session handles and ids after a successful connect handshake.
//
// On success, Poll transfers ownership of session resources into ConnectResult.
// NewAeronCluster(result) moves IngressPublication, EgressSubscription, EgressImage,
// and (when OwnsAeronClient) Aeron into the session, nils those transferred fields
// on result, and leaves cleanup to AeronCluster.Close() only - do not Close those
// handles on result after transfer.
//
// MemberIngressMap aliases the session map at NewAeronCluster time (not nilled on
// result). On leader change the session replaces its map with a newly built one;
// result.MemberIngressMap is stale. Use AeronCluster.MemberIngressMap() for the live
// map and do not cache the result field across leader changes. Do not Close or mutate
// map entries via result after transfer.
//
// At Poll success, leader MemberIngressMap publication pointers alias
// ConnectResult.IngressPublication; non-leader publications are closed before
// Poll returns. After NewAeronCluster, use AeronCluster.IngressPublication() and
// AeronCluster.MemberIngressMap() - result.IngressPublication is nilled on transfer
// while result.MemberIngressMap still aliases the live map until the first leader
// change replaces it.
//
// LeadershipTermID and LeaderMemberID are connect-time snapshots. After leader
// change use AeronCluster.LeadershipTermID and AeronCluster.LeaderMemberID for
// live values. When IngressEndpoints is unset, MemberIngressMap uses synthetic
// member id 0; do not use LeaderMemberID alone to look up channel-only ingress.
//
// Discard AsyncConnect without Close on success. On failure, call AsyncConnect.Close.
type ConnectResult struct {
	ClusterSessionID         int64
	LeadershipTermID         int64
	LeaderMemberID           int32
	LeaderHeartbeatTimeoutNs int64

	IngressPublication IngressPublication
	EgressSubscription *aeron.Subscription
	EgressImage        *aeron.Image

	MemberIngressMap MemberIngressMap

	Aeron           *aeron.Client
	OwnsAeronClient bool
	Config          Config

	// transportFault reads the termination cause of a connect-owned client.
	// It is nil when the caller supplied Config.Aeron.
	transportFault *transportfault.Monitor
}
