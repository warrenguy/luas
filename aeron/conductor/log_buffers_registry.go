package conductor

import (
	"math"
	"slices"
	"sync/atomic"
	"time"

	"github.com/warrenguy/luas/aeron/logbuffer"
)

func timeNowNs() int64 {
	return time.Now().UnixNano()
}

func releaseNowNs(lingerNs int64) int64 {
	if lingerNs <= 0 {
		return 0
	}
	return timeNowNs()
}

func validateLingerNs(lingerNs int64) {
	if lingerNs < 0 {
		panic("conductor: negative linger duration")
	}
}

type logBufferEntry struct {
	buffers  *logbuffer.LogBuffers
	refCount int32
}

type lingeringLogBuffer struct {
	key        string
	buffers    *logbuffer.LogBuffers
	deadlineNs int64
}

// LogBuffersRegistry is the sole conductor log-buffer map.
//
// Live resources on one log file share one mapping. At zero refCount, the
// mapping leaves the live map. A positive linger moves it to the lingering
// list. Acquire never revives a lingering mapping. It maps the file again, so
// a release never cuts short another mapping's linger.
//
// Not thread-safe; mutation must run under Conductor.resourceMu write lock during
// mutation scopes or close drain.
type LogBuffersRegistry struct {
	entries                map[string]*logBufferEntry
	lingering              []lingeringLogBuffer
	mediaDriverDir         string
	resolvedMediaDriverDir string
	// lingerScheduled mirrors len(lingering). HasScheduledLinger reads it
	// without resourceMu.
	lingerScheduled atomic.Int64
	// unmapper unmaps removed entries off the caller goroutine. Nil unmaps
	// on the caller goroutine.
	unmapper *logBufferUnmapper
}

func NewLogBuffersRegistry(mediaDriverDir string) *LogBuffersRegistry {
	registry := &LogBuffersRegistry{
		entries:        make(map[string]*logBufferEntry),
		mediaDriverDir: mediaDriverDir,
	}
	if mediaDriverDir != "" {
		if resolved, err := resolveLogFileMediaDriverDir(mediaDriverDir); err == nil {
			registry.resolvedMediaDriverDir = resolved
		}
	}
	return registry
}

// newUnmappingLogBuffersRegistry returns a registry that unmaps removed
// entries on a separate goroutine.
func newUnmappingLogBuffersRegistry(mediaDriverDir string) *LogBuffersRegistry {
	registry := NewLogBuffersRegistry(mediaDriverDir)
	registry.unmapper = &logBufferUnmapper{}
	return registry
}

func (registry *LogBuffersRegistry) Acquire(logFileName string) (*logbuffer.LogBuffers, error) {
	key, err := registry.resolveLogFileKey(logFileName)
	if err != nil {
		return nil, err
	}
	return registry.acquireByKey(key)
}

func (registry *LogBuffersRegistry) resolveLogFileKey(logFileName string) (string, error) {
	if registry.resolvedMediaDriverDir != "" {
		return resolveLogFileKeyWithResolvedMediaDriverDir(
			logFileName,
			registry.resolvedMediaDriverDir,
		)
	}
	return resolveLogFileKey(logFileName, registry.mediaDriverDir)
}

func (registry *LogBuffersRegistry) acquireByKey(key string) (*logbuffer.LogBuffers, error) {
	if key == "" {
		return nil, ErrLogFileNameEmpty
	}

	entry := registry.entries[key]
	if entry != nil {
		entry.refCount++
		return entry.buffers, nil
	}

	mapped, err := logbuffer.MapLogFile(key)
	if err != nil {
		return nil, ErrLogBufferMapFailed
	}

	buffers, err := logbuffer.NewLogBuffers(mapped)
	if err != nil {
		_ = mapped.Close()
		return nil, err
	}

	registry.unmapper.start()
	registry.entries[key] = &logBufferEntry{
		buffers:  buffers,
		refCount: 1,
	}
	return buffers, nil
}

// Release decrements the refcount for logFileName. At zero refcount, the
// mapping leaves the live map:
//   - zero lingerNs: the unmapper unmaps the log.
//   - positive lingerNs: SweepLingering unmaps the log after the deadline.
//
// Release ignores a path without a live mapping, or one it cannot resolve.
func (registry *LogBuffersRegistry) Release(logFileName string, lingerNs int64) {
	registry.releaseAt(logFileName, releaseNowNs(lingerNs), lingerNs)
}

func (registry *LogBuffersRegistry) releaseAt(
	logFileName string,
	nowNs int64,
	lingerNs int64,
) {
	if registry == nil || logFileName == "" {
		return
	}

	// Unknown or unresolvable path is an intentional no-op.
	key, err := registry.resolveLogFileKey(logFileName)
	if err != nil {
		return
	}
	registry.releaseResolvedKeyAt(key, nowNs, lingerNs)
}

// releaseResolvedKey: lifecycle must use the acquire-time resolved key, not
// Release, because re-resolve can fail after acquire (e.g. EvalSymlinks).
func (registry *LogBuffersRegistry) releaseResolvedKey(key string, lingerNs int64) {
	registry.releaseResolvedKeyAt(key, releaseNowNs(lingerNs), lingerNs)
}

func (registry *LogBuffersRegistry) releaseResolvedKeyAt(
	key string,
	nowNs int64,
	lingerNs int64,
) {
	if registry == nil || key == "" {
		return
	}

	entry := registry.entries[key]
	if entry == nil {
		return
	}

	validateLingerNs(lingerNs)

	if entry.refCount <= 0 {
		panic("conductor: log buffer release underflow")
	}

	entry.refCount--
	// lingerNs applies only when refCount hits zero; intermediate releases ignore it.
	if entry.refCount > 0 {
		return
	}

	delete(registry.entries, key)

	if lingerNs == 0 {
		registry.unmap(entry.buffers)
		return
	}

	registry.lingering = append(registry.lingering, lingeringLogBuffer{
		key:        key,
		buffers:    entry.buffers,
		deadlineNs: saturatedLingerDeadlineNs(nowNs, lingerNs),
	})
	registry.storeLingerScheduled()
}

func saturatedLingerDeadlineNs(nowNs, lingerNs int64) int64 {
	if nowNs > math.MaxInt64-lingerNs {
		return math.MaxInt64
	}
	return nowNs + lingerNs
}

func (registry *LogBuffersRegistry) unmap(buffers *logbuffer.LogBuffers) {
	if mapped := buffers.MappedLogBuffer(); mapped != nil {
		registry.unmapper.unmap(mapped)
	}
}

// stopUnmapper stops the unmap goroutine after it drains the queue.
func (registry *LogBuffersRegistry) stopUnmapper() {
	registry.unmapper.stop()
}

// SweepLingering unmaps each lingering mapping whose deadline has passed.
// Returns the count.
func (registry *LogBuffersRegistry) SweepLingering(nowNs int64) int {
	if registry == nil {
		return 0
	}

	kept := registry.lingering[:0]
	workCount := 0

	for _, lingering := range registry.lingering {
		if nowNs < lingering.deadlineNs {
			kept = append(kept, lingering)
			continue
		}

		registry.unmap(lingering.buffers)
		workCount++
	}

	clear(registry.lingering[len(kept):])
	registry.lingering = kept
	registry.storeLingerScheduled()
	return workCount
}

// lingerDeadlineNs returns the latest deadline among the lingering mappings
// of logFileName, or 0.
func (registry *LogBuffersRegistry) lingerDeadlineNs(logFileName string) int64 {
	key, ok := registry.resolveKnownKey(logFileName)
	if !ok {
		return 0
	}

	var deadlineNs int64

	for _, lingering := range registry.lingering {
		if lingering.key == key {
			deadlineNs = max(deadlineNs, lingering.deadlineNs)
		}
	}
	return deadlineNs
}

func (registry *LogBuffersRegistry) resolveKnownKey(logFileName string) (string, bool) {
	if registry == nil {
		return "", false
	}

	key, err := registry.resolveLogFileKey(logFileName)
	return key, err == nil
}

func (registry *LogBuffersRegistry) lookupEntry(logFileName string) *logBufferEntry {
	key, ok := registry.resolveKnownKey(logFileName)
	if !ok {
		return nil
	}
	return registry.entries[key]
}

func (registry *LogBuffersRegistry) refCount(logFileName string) int32 {
	entry := registry.lookupEntry(logFileName)
	if entry == nil {
		return 0
	}
	return entry.refCount
}

// contains reports whether logFileName holds a live or lingering mapping.
func (registry *LogBuffersRegistry) contains(logFileName string) bool {
	key, ok := registry.resolveKnownKey(logFileName)
	if !ok {
		return false
	}

	if registry.entries[key] != nil {
		return true
	}
	return slices.ContainsFunc(registry.lingering, func(lingering lingeringLogBuffer) bool {
		return lingering.key == key
	})
}

func (registry *LogBuffersRegistry) storeLingerScheduled() {
	registry.lingerScheduled.Store(int64(len(registry.lingering)))
}

// HasScheduledLinger reports whether any entry is awaiting linger sweep.
func (registry *LogBuffersRegistry) HasScheduledLinger() bool {
	return registry != nil && registry.lingerScheduled.Load() > 0
}

func (registry *LogBuffersRegistry) lingerScheduledCount() int {
	if registry == nil {
		return 0
	}
	return int(registry.lingerScheduled.Load())
}

func (registry *LogBuffersRegistry) countScheduledLingers() int {
	if registry == nil {
		return 0
	}
	return len(registry.lingering)
}
