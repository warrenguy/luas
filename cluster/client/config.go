package client

import (
	"cmp"
	"errors"
	"strconv"
	"time"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/idlestrategy"
)

// Config holds cluster client configuration. Normalize before Validate and
// before connect.
//
// Connect and the session call DoWork on Aeron only in invoker mode. With
// an invoker client, the caller calls DoWork between idle steps, during
// AsyncConnect.Poll and in the session duty cycle.
//
// There is no AgentInvoker field.
type Config struct {
	// IngressChannel is required for cluster connect.
	IngressChannel string

	// EgressChannel is required for cluster connect. The media must be ipc,
	// ipcx, or udp. It must not set control-mode=response.
	EgressChannel string

	// IngressEndpoints is "0=host:port,1=host:port"; empty means unset (Java null).
	IngressEndpoints string

	IngressStreamID  int32
	EgressStreamID   int32
	ResponseStreamID int32

	MessageTimeout time.Duration
	// IngressRetryTimeout bounds IngressOfferUntilPositive and
	// SendKeepAliveUntilPositive. Zero uses DefaultIngressRetryTimeout after
	// Normalize.
	IngressRetryTimeout time.Duration
	NewLeaderTimeout    time.Duration

	AeronDir string
	Aeron    *aeron.Client
	// AeronConfig configures the transport client that connect creates when
	// Aeron is nil. aeron.Connect applies the transport defaults, not
	// Config.Normalize.
	//
	// Behaviour:
	//   - connect passes ErrorHandler through unchanged.
	//   - ErrorHandler runs under the transport client lock. It must not call
	//     Close, PollEgress or AsyncConnect.Poll on the session.
	//   - A nil ErrorHandler drops errors that leave the owned client running.
	//   - A fault that terminates the owned client closes the session on the
	//     next PollEgress. AeronCluster.TransportFault returns the cause.
	//   - Every owned client shares the ConductorIdleStrategy instance. Leave
	//     it nil when the Config opens more than one session.
	AeronConfig aeron.Config

	// IngressExclusive nil means true (Java isIngressExclusive default).
	// Explicit false disables exclusive ingress publications.
	IngressExclusive *bool

	// ClientName is sent in SessionConnectRequest.clientInfo; empty defaults to
	// cluster-client at connect.
	ClientName string

	CredentialsSupplier     CredentialsSupplier
	EgressHandler           EgressHandler
	ControlledEgressHandler ControlledEgressHandler

	// IdleStrategy paces connect, SpinOnce, and ingress retry. Use one instance
	// across the session lifetime; Connect resets it on exit before the duty cycle.
	IdleStrategy idlestrategy.IdleStrategy

	// DirectAssemblers mirrors Java isDirectAssemblers; passed to EgressAdapter reassembly.
	DirectAssemblers bool
}

// Normalize applies cluster client defaults. Zero duration and stream ID fields mean
// apply default. Idempotent.
func (cfg Config) Normalize() Config {
	cfg.IngressStreamID = cmp.Or(cfg.IngressStreamID, DefaultIngressStreamID)
	cfg.EgressStreamID = cmp.Or(cfg.EgressStreamID, DefaultEgressStreamID)
	cfg.ResponseStreamID = cmp.Or(cfg.ResponseStreamID, cfg.EgressStreamID)
	cfg.MessageTimeout = cmp.Or(cfg.MessageTimeout, DefaultMessageTimeout)
	cfg.IngressRetryTimeout = cmp.Or(cfg.IngressRetryTimeout, DefaultIngressRetryTimeout)
	cfg.NewLeaderTimeout = cmp.Or(cfg.NewLeaderTimeout, cfg.MessageTimeout)

	if cfg.CredentialsSupplier == nil {
		cfg.CredentialsSupplier = NullCredentialsSupplier{}
	}

	if cfg.IdleStrategy == nil {
		cfg.IdleStrategy = NewDefaultIdleStrategy()
	}

	if cfg.IngressExclusive == nil {
		v := true
		cfg.IngressExclusive = &v
	}

	cfg.AeronConfig.AeronDir = cmp.Or(cfg.AeronConfig.AeronDir, cfg.AeronDir)

	return cfg
}

// ValidateForConnect checks cluster config after Normalize for async connect.
// Same as Validate except EgressHandler is not required. ControlledEgressHandler
// is not validated by either method. All errors use the cluster client: prefix.
// Call Normalize before ValidateForConnect and connect.
func (cfg Config) ValidateForConnect() error {
	return cfg.validateConfig(false)
}

// Validate checks cluster config after Normalize. Same as ValidateForConnect but
// requires EgressHandler. Call Normalize before Validate and connect.
func (cfg Config) Validate() error {
	return cfg.validateConfig(true)
}

func (cfg Config) validateConfig(requireEgressHandler bool) error {
	if cfg.IngressChannel == "" {
		return errors.New("cluster client: IngressChannel is required")
	}
	if cfg.EgressChannel == "" {
		return errors.New("cluster client: EgressChannel is required")
	}
	err := validateClusterIngressEndpoints(
		cfg.IngressChannel,
		cfg.IngressEndpoints,
	)
	if err != nil {
		return err
	}
	if err := validateClusterEgressChannel(cfg.EgressChannel); err != nil {
		return err
	}
	if len(cfg.ClientName) > MaxClientNameLength {
		return errors.New(
			"cluster client: ClientName length must be <= " +
				strconv.Itoa(MaxClientNameLength),
		)
	}
	if cfg.MessageTimeout <= 0 {
		return errors.New("cluster client: MessageTimeout must be positive")
	}
	if cfg.IngressRetryTimeout <= 0 {
		return errors.New("cluster client: IngressRetryTimeout must be positive")
	}
	if cfg.NewLeaderTimeout < 0 {
		return errors.New("cluster client: NewLeaderTimeout must not be negative")
	}
	if requireEgressHandler && egressHandlerMissing(cfg.EgressHandler) {
		return errors.New("cluster client: EgressHandler is required")
	}
	if cfg.CredentialsSupplier == nil {
		return errors.New("cluster client: CredentialsSupplier is required")
	}
	if cfg.IdleStrategy == nil {
		return errors.New("cluster client: IdleStrategy is required")
	}
	return nil
}

func validateClusterIngressEndpoints(ingressChannel, ingressEndpoints string) error {
	if IngressChannelPassthrough(ingressChannel) && ingressEndpoints != "" {
		return errors.New("cluster client: IngressEndpoints must be empty for ipc/ipcx ingress")
	}
	if ingressEndpoints == "" {
		return nil
	}
	if IngressChannelIsUDP(ingressChannel) &&
		ingressChannelQueryHasEndpointParam(ingressChannel) {
		return errors.New(
			"cluster client: IngressChannel must not use ?endpoint= when IngressEndpoints is configured",
		)
	}
	return validateIngressChannelForMemberEndpoints(ingressChannel)
}

func egressHandlerMissing(handler EgressHandler) bool {
	if handler == nil {
		return true
	}
	handlers, isHandlers := handler.(*EgressHandlers)
	return isHandlers && handlers == nil
}

// OwnsAeronClient reports whether connect should close the transport client.
// false when cfg.Aeron is set; true when connect creates an owned client.
func (cfg Config) OwnsAeronClient() bool {
	return cfg.Aeron == nil
}
