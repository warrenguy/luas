package aeron

import (
	"strconv"

	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/atomicbuffer"
)

// Publication return codes -1 to -5 mirror Aeron Java Publication.
// UnsupportedMessageLength matches the C client AERON_PUBLICATION_ERROR.
//
//   - Values > 0: new stream position after a successful Offer or TryClaim.
//   - NotConnected (-1): no subscriber; retry with idle strategy.
//   - BackPressured (-2): at or past publication limit; retry when subscribers advance.
//   - AdminAction (-3): rotation or metadata catch-up in flight; retry immediately.
//   - Closed (-4): publication closed, or an auxiliary read failed (counter/metadata).
//     PositionLimit returns Closed on close; publicationCounterValueError can force
//     Closed on cold accessors (test hook).
//     AvailableWindow propagates negative codes from PositionLimit and Position.
//     Position maps internal read failures to Closed because it returns int64 only.
//   - MaxPositionExceeded (-5): stream position exhausted. Close the publication and
//     add a new one.
//   - UnsupportedMessageLength (-6): Offer length exceeds both MaxPayloadLength and
//     MaxMessageLength. Do not retry. Java throws IllegalArgumentException here.
//     TryClaim returns a Go error instead.
//
// Offer and TryClaim read the bound publication-limit CNC slot directly after bind.
// PositionLimit reads the same bound slot without per-call CounterValue validation.
// ChannelStatus calls CountersReader.CounterValue on each read.
//
// publicationCounterValueError test hook can fail those cold accessors; hot publish
// ignores it.
const (
	NotConnected             int64 = -1
	BackPressured            int64 = -2
	AdminAction              int64 = -3
	Closed                   int64 = -4
	MaxPositionExceeded      int64 = -5
	UnsupportedMessageLength int64 = -6
)

var publicationPositionCodeNames = map[int64]string{
	NotConnected:             "NotConnected",
	BackPressured:            "BackPressured",
	AdminAction:              "AdminAction",
	Closed:                   "Closed",
	MaxPositionExceeded:      "MaxPositionExceeded",
	UnsupportedMessageLength: "UnsupportedMessageLength",
}

// PositionCodeName returns a human-readable name for a publication Offer or TryClaim result.
//
//   - Negative values map to the exported constant names in this package.
//   - Positive values are successful stream positions and are formatted as decimal strings.
//   - Zero is formatted as "0".
//   - Unknown negative values are formatted as "Unknown(n)".
func PositionCodeName(position int64) string {
	if position >= 0 {
		return strconv.FormatInt(position, 10)
	}
	if name, ok := publicationPositionCodeNames[position]; ok {
		return name
	}
	return "Unknown(" + strconv.FormatInt(position, 10) + ")"
}

// IsOfferRetriable reports whether position should be retried in a transport-level
// offer retry loop.
//
// Retries BackPressured, AdminAction, and NotConnected (waiting for first subscriber).
//
// Terminal (do not retry): Closed, MaxPositionExceeded, UnsupportedMessageLength, 0, and
// unknown negatives. Returns false for position > 0 (success).
//
// Do not use in ingress or archive control retry loops; use IsIngressRetriable instead.
func IsOfferRetriable(position int64) bool {
	return IsIngressRetriable(position) || position == NotConnected
}

// IsIngressRetriable reports whether position should be retried in an ingress or archive
// control offer retry loop that calls PollEgress or a pollFn between attempts.
//
// Retries only BackPressured and AdminAction. NotConnected is terminal (session recovery
// may surface NotConnected after a retriable code).
//
// Closed, MaxPositionExceeded, UnsupportedMessageLength, 0, and unknown negatives are
// terminal. Returns false for position > 0.
func IsIngressRetriable(position int64) bool {
	return position == BackPressured || position == AdminAction
}

func backPressureStatusForLog(
	meta atomicbuffer.AtomicBuffer,
	maxPossiblePosition int64,
	position int64,
	requiredSpan int64,
) int64 {
	if position+requiredSpan >= maxPossiblePosition {
		return MaxPositionExceeded
	}
	if logbuffer.IsConnectedVolatile(meta) != 0 {
		return BackPressured
	}
	return NotConnected
}

func backPressureStatus(
	pub *Publication,
	position int64,
	messageLength int64,
) int64 {
	requiredSpan := int64(logbuffer.AlignFragmentChunkLength(int(messageLength)))
	return backPressureStatusForLog(pub.meta, pub.maxPossiblePosition, position, requiredSpan)
}
