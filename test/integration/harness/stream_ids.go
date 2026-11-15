//go:build integration

package harness

// Integration test stream IDs for IPC pub/sub and harness poll/offer helpers.
//
// Each constant must map to at most one parallel Shared(t) consumer per package
// binary. Duplicate IDs on one shared aeronmd cause cross-test IPC interference
// under t.Parallel. Add a new constant rather than reusing an existing one.
//
//go:generate go tool -modfile=../../../tools/go.mod constnamemap-gen -source=stream_ids.go -output=stream_ids_registry_test.go -package=harness -build=integration -var=sharedDriverStreamIDs -valuetype=int32 -suffix=StreamID -doc "sharedDriverStreamIDs lists every named stream ID constant in stream_ids.go consumed by parallel Shared(t) tests." -doc "Regenerate via go generate -tags=integration ./test/integration/harness/."
const (
	ChannelStatusCounterStreamID      int32 = 1001
	PubSubRoundtripStreamID           int32 = 1007
	PubSubTermRolloverStreamID        int32 = 1008
	PubSubCloseDrainStreamID          int32 = 1009
	PubSubFragmentedRoundtripStreamID int32 = 1012
	PubSubTwoClientsRoundtripStreamID int32 = 1013
	MPSCMatrixPubSubStreamID          int32 = 1010
	MPSCMatrixSessionIDStreamID       int32 = 1011
	PublicationLimitStreamID          int32 = 1014
	// Isolated from PublicationLimitStreamID for shared-driver cross-test safety.
	PublicationLimitAfterDrainStreamID              int32 = 1015
	SharedPublicationCloseStreamID                  int32 = 1016
	PublicationLocalSocketAddressesStreamID         int32 = 1017
	ClientCloseSkipsLingerWaitStreamID              int32 = 1018
	ExclusivePublicationTwoClientsRoundtripStreamID int32 = 7206
	AeronHelpersSmokeStreamID                       int32 = 8024
	// Isolated from AeronHelpersSmokeStreamID for shared-driver cross-test safety.
	AeronHelpersFragmentedSmokeStreamID     int32 = 8025
	AeronHelpersBackgroundStreamID          int32 = 8029
	AeronHelpersBackgroundExclusiveStreamID int32 = 8031

	ConnectedWaitPublicationTimeoutStreamID  int32 = 7991
	ConnectedWaitSubscriptionTimeoutStreamID int32 = 7994
	RequirePublicationLookupMissStreamID     int32 = 7992
	// Isolated from RequirePublicationLookupMissStreamID for shared-driver cross-test safety.
	RequirePublicationLookupMissTimeoutStreamID int32 = 8026
	WaitForIPCLinkConnectedStreamID             int32 = 7993
	// Intentional pub/sub mismatch for TestWaitForIPCLinkConnected_subscriptionTimeout.
	WaitForIPCLinkConnectedSubTimeoutPubStreamID int32 = 8028
	// Sub-only stream for TestWaitForIPCLinkConnected_subscriptionTimeout (no matching pub).
	WaitForIPCLinkConnectedSubscriptionOnlyTimeoutStreamID int32 = 8030

	WaitUntilConnectedTimeoutStreamID      int32 = 7990
	WaitUntilImageCountZeroActiveStreamID  int32 = 7995
	WaitUntilImageCountOneWantTwoStreamID  int32 = 7999
	WaitUntilImageCountInvalidWantStreamID int32 = 8000
	PollUntilNoActiveImagesTimeoutStreamID int32 = 8012
	// Isolated from PollUntilNoActiveImagesTimeoutStreamID for shared-driver cross-test safety.
	PollUntilNoActiveImagesCtxDeadlineStreamID  int32 = 8027
	PollUntilNoActiveImagesSplitTimeoutStreamID int32 = 8015
	PollSplitClientIPCLinkTimeoutStreamID       int32 = 8014
	// Intentional pub/sub mismatch for TestPollSplitClientIPCLink_timeoutMessage.
	// Isolated from PollUntilNoActiveImagesSplitTimeoutStreamID for shared-driver cross-test safety.
	PollSplitClientIPCLinkSubStreamID int32 = 8023

	PollDeliveredTimeoutStreamID              int32 = 7996
	PollDeliveredPartialDeliveryStreamID      int32 = 8001
	PollDeliveredInvalidWantStreamID          int32 = 7997
	PollDeliveredLiveIPCStreamID              int32 = 7998
	PollDeliveredSplitTimeoutStreamID         int32 = 8016
	PollDeliveredSplitPartialDeliveryStreamID int32 = 8017
	PollDeliveredSplitInvalidWantStreamID     int32 = 8018

	PollReassembledTimeoutStreamID         int32 = 8002
	PollReassembledPartialDeliveryStreamID int32 = 8005
	PollReassembledInvalidWantStreamID     int32 = 8004
	PollReassembledLiveIPCStreamID         int32 = 8003
	PollReassembledNoImageStreamID         int32 = 8006

	OfferWithRetryWitnessBackPressuredStreamID int32 = 8011
	// Isolated from WaitUntilConnectedTimeoutStreamID for shared-driver cross-test safety.
	OfferWithRetryNotConnectedTimeoutStreamID int32 = 8019
	// Isolated from WaitForIPCLinkConnectedStreamID for shared-driver cross-test safety.
	OfferWithRetryNotConnectedThenConnectStreamID int32 = 8020
	// Isolated from ConnectedWaitPublicationTimeoutStreamID for shared-driver cross-test safety.
	OfferWithRetryClosedPublicationStreamID int32 = 8021

	AssertSubscribeMetadataStreamID int32 = 8022

	// Cluster log commit live tests. Isolated for SharedParallel.
	LogCommitAppendNLTStreamID      int32 = 8301
	LogCommitBoundIsolationStreamID int32 = 8302

	// Single-peer AeronPublisher CommitPosition (control stream).
	LogCommitAeronPublisherStreamID int32 = 8304

	// Multi-destination receive merge on live aeronmd (control-mode=manual).
	MultiDestinationSubscriptionStreamID int32 = 8401

	// Multi-destination send on live aeronmd (control-mode=manual).
	MultiDestinationPublicationStreamID int32 = 8402

	// Rejected send destination adds on live aeronmd.
	MultiDestinationPublicationErrorsStreamID int32 = 8403

	// Async send destination commands on live aeronmd.
	MultiDestinationPublicationAsyncStreamID int32 = 8404

	// Module egress add released while the ready mark names it.
	ConsensusEgressCancelMarkedStreamID int32 = 8501

	// Module egress add released after the ready mark moved.
	ConsensusEgressCancelMovedStreamID int32 = 8502

	// Module log for a snapshot take closed while the ready mark names its add.
	ConsensusSnapshotCancelMarkedLogStreamID int32 = 8503

	// Module snapshot add closed while the ready mark names it.
	ConsensusSnapshotCancelMarkedStreamID int32 = 8504

	// Module log for a snapshot take closed after the ready mark moved.
	ConsensusSnapshotCancelMovedLogStreamID int32 = 8505

	// Module snapshot add closed after the ready mark moved.
	ConsensusSnapshotCancelMovedStreamID int32 = 8506

	// Service log for a snapshot take closed while the ready mark names its add.
	ServiceSnapshotCancelMarkedLogStreamID int32 = 8507

	// Service snapshot add closed while the ready mark names it.
	ServiceSnapshotCancelMarkedStreamID int32 = 8508

	// Service log for a snapshot take closed after the ready mark moved.
	ServiceSnapshotCancelMovedLogStreamID int32 = 8509

	// Service snapshot add closed after the ready mark moved.
	ServiceSnapshotCancelMovedStreamID int32 = 8510
)
