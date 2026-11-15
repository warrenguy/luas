//go:build example

// cluster-member-service runs one cluster member and one echo service.
//
// Prerequisites: bin/aeronmd, a co-located Archive, and Java. See README.
//
// Run:
//
//	go run -tags=example ./examples/cluster-member-service/
package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/archive"
	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/cluster/codecs/clusterwire"
	"github.com/warrenguy/luas/cluster/consensus"
	"github.com/warrenguy/luas/cluster/service"
)

const (
	defaultClusterDir = "/tmp/luas-cluster-member"
	defaultAeronDir   = "/tmp/aeron"
	memberID          = 0
	hostname          = "localhost"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("cluster-member-service", flag.ContinueOnError)

	portBase := fs.Int("port-base", cluster.DefaultPortBase, "first port of member 0")
	clusterDir := fs.String(
		"cluster-dir",
		defaultClusterDir,
		"cluster directory; a second run recovers it",
	)

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}

		return 1
	}

	if fs.NArg() != 0 {
		log.Print("cluster-member-service: unexpected arguments")
		return 1
	}

	if *portBase <= 0 {
		log.Print("cluster-member-service: port base must be positive")
		return 1
	}

	members, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
		StartingMemberID: memberID,
		IngressHostnames: []string{hostname},
		ClusterHostnames: []string{hostname},
		PortBase:         *portBase,
	})
	if err != nil {
		log.Printf("cluster-member-service: members: %v", err)
		return 1
	}

	aeronDir := cmp.Or(os.Getenv("AERON_DIR"), defaultAeronDir)
	ingressPort := cluster.CalculatePort(memberID, *portBase, cluster.ClientFacingPortOffset)

	// A signal stops this member only.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Restore default signal handling on the first signal. A second signal
	// then ends a stuck stop.
	context.AfterFunc(ctx, stop)

	svc := &echoService{}

	cfg := cluster.Config{
		MemberID:                      memberID,
		ClusterMembers:                members,
		ClusterDir:                    *clusterDir,
		AeronDir:                      aeronDir,
		ServiceCount:                  1,
		ArchiveControlRequestChannel:  archive.DefaultLocalControlChannel,
		ArchiveControlResponseChannel: archive.DefaultLocalControlChannel,
		ErrorHandler: func(err error) {
			log.Printf("cluster-member-service: %v", err)
		},
	}

	log.Printf(
		"cluster-member-service: member %d ingress %s:%d cluster-dir %s",
		memberID,
		hostname,
		ingressPort,
		*clusterDir,
	)

	module, err := cluster.OpenConsensusModule(ctx, cluster.ConsensusModuleDeps{Config: cfg})
	if err != nil {
		log.Printf("cluster-member-service: open module: %v", err)
		return stopCode(err)
	}

	defer closeWithLog("close module", module.Close)

	container, err := service.Open(service.Config{
		Aeron:             module.Client(),
		Service:           svc,
		CommittedPosition: module.CommitPosition,
		Archive:           &archive.Config{},
		Recovery:          &service.RecoveryConfig{},
		ErrorHandler: func(err error) {
			log.Printf("cluster-member-service: service: %v", err)
		},
	})
	if err != nil {
		log.Printf("cluster-member-service: open service: %v", err)
		return stopCode(err)
	}

	defer closeWithLog("close container", container.Close)

	runner, err := cluster.NewRunner(cluster.RunnerDeps{
		Client:  module.Client(),
		Agent:   module.Agent(),
		Archive: module.ArchiveClient(),
		Idle:    module.IdleStrategy(),
		Clock:   module.NanoClock(),
	})
	if err != nil {
		log.Printf("cluster-member-service: runner: %v", err)
		return stopCode(err)
	}

	log.Print("cluster-member-service: running")

	err = runner.RunWithServices(ctx, container.Agent())

	// Restore default signal handling after a stop without a signal. A signal
	// then ends a stuck Close.
	stop()
	log.Printf("cluster-member-service: stopped: %v", err)

	return stopCode(err)
}

func closeWithLog(label string, closeFn func() error) {
	if err := closeFn(); err != nil {
		log.Printf("cluster-member-service: %s: %v", label, err)
	}
}

func stopCode(err error) int {
	var term *consensus.TerminationError
	if errors.As(err, &term) && term.Expected {
		return 0
	}

	if errors.Is(err, context.Canceled) {
		return 0
	}

	return 1
}

// echoService offers each session message back to its client.
type echoService struct{}

func (*echoService) Start(service.Cluster) error {
	log.Print("cluster-member-service: start")
	return nil
}

func (*echoService) SessionOpen(service.ClientSession, int64) {}

func (*echoService) SessionClose(service.ClientSession, int64, clusterwire.CloseReason) {}

func (*echoService) SessionMessage(session service.ClientSession, _ int64, payload []byte) {
	if session == nil {
		log.Print("cluster-member-service: session message dropped: no session")
		return
	}

	result := session.Offer(payload)

	switch {
	case result == service.MockedOffer:
		// Log replay: the member is not the leader. It does not send a reply.
		return

	case result > 0:
		log.Printf("cluster-member-service: echo %q", payload)
		return
	}

	log.Printf(
		"cluster-member-service: echo offer failed: %s",
		aeron.PositionCodeName(result),
	)
}

func (*echoService) TimerEvent(int64, int64) {}

func (*echoService) RoleChange(role codecs.Role) {
	log.Printf("cluster-member-service: role %s", role)
}

func (*echoService) Terminate(service.Cluster) error {
	log.Print("cluster-member-service: terminate")
	return nil
}
