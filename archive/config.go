package archive

import (
	"cmp"
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/idlestrategy"
	"github.com/warrenguy/luas/internal/transportfault"
)

// Config holds Aeron Archive control client configuration.
//
// Normalize before ValidateForConnect and before connect.
//
// Connect and the session call DoWork on Aeron only in invoker mode. With
// an invoker client, the caller calls DoWork between idle steps, during
// AsyncConnect.Poll and after connect.
//
// There is no AgentInvoker field.
type Config struct {
	// ControlRequestChannel is the archive control request URI.
	//
	// When LocalControlChannel is set, AsyncConnect prefers local IPC first (Java parity).
	ControlRequestChannel string

	// ControlResponseChannel is the subscription channel for control responses.
	//
	// Empty defaults to DefaultControlResponseChannel after Normalize.
	//
	// control-mode=response takes effect only with WithRemoteControl. Connect
	// then adds response-correlation-id to the request channel. Both channels
	// must share one media. ipc and ipcx count as one media.
	ControlResponseChannel string

	// LocalControlChannel is the co-located archive IPC control URI.
	//
	// Non-empty causes connect to use this as the request channel (IPC-first).
	//
	// Normalize defaults this to DefaultLocalControlChannel unless WithRemoteControl()
	// was called first.
	LocalControlChannel string

	ControlRequestStreamID int32

	// ControlResponseStreamID is the control response stream id. Zero uses
	// DefaultControlResponseStreamID after Normalize. IPC-first connect uses it
	// too.
	//
	// On ipc outside response mode, it must differ from the request stream id.
	ControlResponseStreamID int32

	LocalControlStreamID int32

	ConnectTimeout time.Duration
	// ControlResponseTimeout bounds control RPC response awaits. Zero uses
	// DefaultControlResponseTimeout after Normalize. Java parity: messageTimeoutNs.
	ControlResponseTimeout time.Duration
	// ControlOfferRetryTimeout is the outer bound on control offer retries,
	// including [Client.ControlOfferUntilPositive]. MessageRetryAttempts ends
	// most retries first. Zero uses DefaultControlOfferRetryTimeout after
	// Normalize.
	ControlOfferRetryTimeout time.Duration
	// MessageRetryAttempts caps the offers in one control offer retry, including
	// [Client.ControlOfferUntilPositive]. Zero uses DefaultMessageRetryAttempts.
	// ControlOfferRetryTimeout also ends the retry. Java parity: messageRetryAttempts.
	MessageRetryAttempts int

	AeronDir string
	Aeron    *aeron.Client

	// AeronConfig configures the transport client that connect creates when
	// Aeron is nil.
	//
	// Behaviour:
	//   - A nil ErrorHandler drops errors that leave the owned client running.
	//   - A fault that terminates the owned client closes the session on the
	//     next poll or blocking call. Client.TransportFault returns the cause.
	//   - Every owned client shares the ConductorIdleStrategy instance. Leave
	//     it nil when the Config opens more than one session.
	AeronConfig aeron.Config

	CredentialsSupplier CredentialsSupplier

	// RecordingSignalHandler receives recording signals on Poll and on blocking
	// control awaits that poll through Client.Poll. *AwaitSignal routes proof
	// signals to the per-call tracker, not this handler. Nil drops signals until
	// Client.SetRecordingSignalHandler installs a handler. An open
	// AsyncStartRecording or AsyncExtendRecording still receives its START or
	// EXTEND signal.
	RecordingSignalHandler RecordingSignalHandler

	// ErrorResponseHandler receives each unsolicited archive ERROR. Nil drops
	// each one. See [Client.SetErrorResponseHandler].
	ErrorResponseHandler func(error)

	// ClientName is sent in AuthConnectRequest.clientInfo; empty defaults to
	// archive-client at connect.
	ClientName string

	IdleStrategy idlestrategy.IdleStrategy

	remoteControlExplicit bool

	effectiveControlRequestChannel   string
	effectiveControlResponseChannel  string
	effectiveControlRequestStreamID  int32
	effectiveControlResponseStreamID int32
}

// WithRemoteControl opts out of the IPC-first LocalControlChannel default applied by
// Normalize. Remote archive connect uses ControlRequestChannel and
// ControlResponseChannel after Normalize.
func (cfg Config) WithRemoteControl() Config {
	cfg.remoteControlExplicit = true
	cfg.LocalControlChannel = ""
	return cfg
}

// Normalize applies archive defaults. Zero duration and stream ID fields mean
// apply default. Idempotent.
func (cfg Config) Normalize() Config {
	if !cfg.remoteControlExplicit && cfg.LocalControlChannel == "" {
		cfg.LocalControlChannel = DefaultLocalControlChannel
	}

	cfg.ControlRequestChannel = cmp.Or(cfg.ControlRequestChannel, DefaultControlRequestChannel)
	cfg.ControlResponseChannel = cmp.Or(cfg.ControlResponseChannel, DefaultControlResponseChannel)
	cfg.LocalControlStreamID = cmp.Or(cfg.LocalControlStreamID, DefaultLocalControlStreamID)
	cfg.ControlRequestStreamID = cmp.Or(cfg.ControlRequestStreamID, DefaultControlRequestStreamID)
	cfg.ControlResponseStreamID = cmp.Or(cfg.ControlResponseStreamID, DefaultControlResponseStreamID)
	cfg.ConnectTimeout = cmp.Or(cfg.ConnectTimeout, DefaultConnectTimeout)
	cfg.ControlResponseTimeout = cmp.Or(cfg.ControlResponseTimeout, DefaultControlResponseTimeout)
	cfg.ControlOfferRetryTimeout = cmp.Or(
		cfg.ControlOfferRetryTimeout,
		DefaultControlOfferRetryTimeout,
	)
	cfg.MessageRetryAttempts = cmp.Or(cfg.MessageRetryAttempts, DefaultMessageRetryAttempts)
	cfg.ClientName = cmp.Or(cfg.ClientName, defaultClientName)

	if cfg.CredentialsSupplier == nil {
		cfg.CredentialsSupplier = NullCredentialsSupplier{}
	}

	if cfg.IdleStrategy == nil {
		cfg.IdleStrategy = NewDefaultIdleStrategy()
	}

	cfg.AeronConfig.AeronDir = cmp.Or(cfg.AeronConfig.AeronDir, cfg.AeronDir)

	return cfg
}

// ValidateForConnect checks archive config for async connect. Applies Normalize and
// effective channel selection on a stack copy without mutating the receiver. All
// errors use the archive: prefix.
func (cfg Config) ValidateForConnect() error {
	cfg = applyEffectiveChannels(cfg.Normalize())

	requestChannel := cfg.effectiveControlRequestChannel
	responseChannel := cfg.effectiveControlResponseChannel
	if err := validateArchiveIngressChannel(requestChannel, "request"); err != nil {
		return err
	}
	if requestChannel != responseChannel {
		if err := validateArchiveIngressChannel(responseChannel, "response"); err != nil {
			return err
		}
	}
	if err := validateResponseMode(requestChannel, responseChannel); err != nil {
		return err
	}
	if err := validateControlStreamIDs(
		requestChannel,
		responseChannel,
		cfg.effectiveControlRequestStreamID,
		cfg.effectiveControlResponseStreamID,
	); err != nil {
		return err
	}

	if cfg.ConnectTimeout <= 0 {
		return errors.New("archive: ConnectTimeout must be positive")
	}
	if cfg.ControlResponseTimeout <= 0 {
		return errors.New("archive: ControlResponseTimeout must be positive")
	}
	if cfg.ControlOfferRetryTimeout <= 0 {
		return errors.New("archive: ControlOfferRetryTimeout must be positive")
	}
	if cfg.MessageRetryAttempts <= 0 {
		return errors.New("archive: MessageRetryAttempts must be positive")
	}
	if len(cfg.ClientName) > MaxClientNameLength {
		return errors.New(
			"archive: ClientName length must be <= " +
				strconv.Itoa(MaxClientNameLength),
		)
	}
	if cfg.Aeron == nil && archiveAeronDir(cfg) == "" {
		return errors.New("archive: AeronDir is required when Aeron is not set")
	}
	return nil
}

// EffectiveControlRequestChannel returns the request URI after IPC-first selection.
//
// After connect, Client.Config reports the URI with session-id added. After a
// control-mode=response connect, it adds response-correlation-id instead.
func (cfg Config) EffectiveControlRequestChannel() string {
	return cfg.effectiveControlRequestChannel
}

// EffectiveControlResponseChannel returns the response subscription URI after
// IPC-first selection.
//
// Outside response mode, Client.Config reports the URI with session-id added.
func (cfg Config) EffectiveControlResponseChannel() string {
	return cfg.effectiveControlResponseChannel
}

// EffectiveControlRequestStreamID returns the request stream id after IPC-first
// selection.
func (cfg Config) EffectiveControlRequestStreamID() int32 {
	return cfg.effectiveControlRequestStreamID
}

// EffectiveControlResponseStreamID returns the response stream id after IPC-first
// selection.
func (cfg Config) EffectiveControlResponseStreamID() int32 {
	return cfg.effectiveControlResponseStreamID
}

// OwnsAeronClient reports whether connect should close the transport client.
// false when cfg.Aeron is set; true when connect creates an owned client.
func (cfg Config) OwnsAeronClient() bool {
	return cfg.Aeron == nil
}

func applyEffectiveChannels(cfg Config) Config {
	cfg.effectiveControlResponseStreamID = cfg.ControlResponseStreamID
	if cfg.LocalControlChannel != "" {
		requestStreamID := cfg.LocalControlStreamID
		if requestStreamID == 0 {
			requestStreamID = cfg.ControlRequestStreamID
		}
		cfg.effectiveControlRequestChannel = cfg.LocalControlChannel
		cfg.effectiveControlResponseChannel = cfg.LocalControlChannel
		cfg.effectiveControlRequestStreamID = requestStreamID
		return cfg
	}

	responseChannel := cfg.ControlResponseChannel
	if responseChannel == "" {
		responseChannel = cfg.ControlRequestChannel
	}
	cfg.effectiveControlRequestChannel = cfg.ControlRequestChannel
	cfg.effectiveControlResponseChannel = responseChannel
	cfg.effectiveControlRequestStreamID = cfg.ControlRequestStreamID
	return cfg
}

func archiveAeronDir(cfg Config) string {
	if cfg.AeronDir != "" {
		return cfg.AeronDir
	}
	return cfg.AeronConfig.AeronDir
}

// concludeConfig finalizes channel defaults and resolves the transport client.
//
// ctx bounds owned transport attach via aeron.Connect only. Shared Aeron and
// test factories ignore ctx. Do not store ctx on Config.
//
// Behaviour:
//   - cfg.Aeron set: returns that client with a nil monitor.
//   - cfg.Aeron nil: returns the owned client and a monitor for it.
func concludeConfig(
	ctx context.Context,
	cfg Config,
) (
	Config,
	*aeron.Client,
	bool,
	*transportfault.Monitor,
	error,
) {
	cfg = applyEffectiveChannels(cfg)
	responseChannel, err := applyUDPRejoinFalse(cfg.effectiveControlResponseChannel)
	if err != nil {
		return Config{}, nil, false, nil, err
	}
	cfg.effectiveControlResponseChannel = responseChannel

	if cfg.Aeron != nil {
		return cfg, cfg.Aeron, false, nil, nil
	}
	aeronCfg := cfg.AeronConfig
	if dir := archiveAeronDir(cfg); dir != "" {
		aeronCfg.AeronDir = dir
	}
	var client *aeron.Client
	if factory, ok := ownedConnectClientFactoryForConfig(cfg); ok {
		factoryCfg := cfg
		factoryCfg.AeronConfig = aeronCfg
		client, err = factory(factoryCfg)
	} else {
		client, err = aeron.Connect(ctx, aeronCfg)
	}
	if err != nil {
		return Config{}, nil, false, nil, err
	}
	cfg.Aeron = client
	return cfg, client, true, &transportfault.Monitor{}, nil
}
