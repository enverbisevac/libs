package inmem

import (
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/enverbisevac/libs/stream"
)

// entry is a single message stored in a streamState's append-only log.
// seq is the monotonic ordering primitive; ID is its string form, exposed
// to handlers as Message.ID.
type entry struct {
	seq       int64
	id        string
	payload   []byte
	headers   map[string]string
	createdAt time.Time
}

// streamState holds all state for a single named stream: the append-only
// log and a map of consumer groups subscribed to it. Mutations to log,
// nextSeq, and groups are guarded by mu. notifyCh is closed-and-replaced
// under mu to wake workers waiting for new appends without the lost-wakeup
// race that sync.Cond + goroutine-spawn exhibited.
type streamState struct {
	name string

	mu       sync.Mutex
	notifyCh chan struct{}
	log      []*entry
	nextSeq  int64
	groups   map[string]*groupState
}

func newStreamState(name string) *streamState {
	return &streamState{
		name:     name,
		log:      make([]*entry, 0, 64),
		groups:   make(map[string]*groupState),
		notifyCh: make(chan struct{}),
	}
}

// groupState holds the durable cursor and pending list for a single
// (stream, group) pair. Mutations to lastSeq and pending are guarded by
// the streamState's mu — there is one lock per stream covering all
// groups, which serializes claim/ack against append.
type groupState struct {
	name    string
	lastSeq int64

	// pending maps seq -> in-flight claim metadata. A message is in
	// pending if it has been claimed by a worker but not yet acked.
	pending map[int64]*pendingEntry
}

func newGroupState(name string, startSeq int64) *groupState {
	return &groupState{
		name:    name,
		lastSeq: startSeq,
		pending: make(map[int64]*pendingEntry),
	}
}

// pendingEntry tracks one claimed-but-not-acked message.
type pendingEntry struct {
	entry         *entry
	consumer      string // consumer name (worker id) holding the claim
	claimDeadline time.Time
	attempts      int // 1 on first claim, 2 on first reclaim, etc.
}

// numEntries returns the count of entries currently in the log. Internal
// helper used by Service.LogLen; not part of the public contract.
func (s *streamState) numEntries() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.log)
}

// appendEntry adds a new entry under s.mu and wakes one waiting worker.
// Returns the assigned seq.
func (s *streamState) appendEntry(payload []byte, headers map[string]string, now func() time.Time) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSeq++
	e := &entry{
		seq:       s.nextSeq,
		id:        formatSeqID(s.nextSeq),
		payload:   append([]byte(nil), payload...),
		headers:   cloneHeaders(headers),
		createdAt: now(),
	}
	s.log = append(s.log, e)
	close(s.notifyCh)
	s.notifyCh = make(chan struct{})
	return s.nextSeq
}

// formatSeqID renders a numeric seq as a decimal string for Message.ID.
func formatSeqID(seq int64) string {
	return strconv.FormatInt(seq, 10)
}

// cloneHeaders snapshots headers at publish time so caller mutations don't
// leak into delivered messages. nil/empty input returns nil.
func cloneHeaders(h map[string]string) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	maps.Copy(out, h)
	return out
}

// applyTrim removes entries from the head of the log according to the
// policy (maxLen, maxAge). Zero values mean "no trim". When both bounds
// are set, the length cap is applied first, then the age cap. Caller must
// NOT hold s.mu; this method takes it internally.
func (s *streamState) applyTrim(maxLen int64, maxAge time.Duration, now time.Time) {
	if maxLen <= 0 && maxAge <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Length-based trim.
	if maxLen > 0 && int64(len(s.log)) > maxLen {
		drop := int64(len(s.log)) - maxLen
		s.log = s.log[drop:]
	}
	// Age-based trim.
	if maxAge > 0 {
		cutoff := now.Add(-maxAge)
		i := 0
		for i < len(s.log) && s.log[i].createdAt.Before(cutoff) {
			i++
		}
		if i > 0 {
			s.log = s.log[i:]
		}
	}
}

// orCreateGroupLocked returns the groupState for name, creating it with the
// given startSeq if it does not yet exist. ON CONFLICT DO NOTHING semantics
// — startSeq is ignored if the group already exists. Caller must hold s.mu.
func (s *streamState) orCreateGroupLocked(name string, startSeq int64) *groupState {
	g, ok := s.groups[name]
	if !ok {
		g = newGroupState(name, startSeq)
		s.groups[name] = g
	}
	return g
}

// resolveStartSeqLocked returns the seq value a brand-new group should start
// at, given the SubscribeConfig. Caller must hold s.mu.
func (s *streamState) resolveStartSeqLocked(cfg stream.SubscribeConfig) int64 {
	switch {
	case cfg.StartFromID != "":
		// Parse decimal id; on parse error, fall back to Latest.
		if seq, err := strconv.ParseInt(cfg.StartFromID, 10, 64); err == nil {
			return seq
		}
		return s.nextSeq
	case cfg.StartFrom == stream.StartEarliest:
		return 0
	default: // StartLatest
		return s.nextSeq
	}
}

// nextEligibleLocked returns the slice of entries with seq > afterSeq, up to
// maxN entries. Caller must hold s.mu.
func (s *streamState) nextEligibleLocked(afterSeq int64, maxN int) []*entry {
	if len(s.log) == 0 {
		return nil
	}
	out := make([]*entry, 0, maxN)
	for _, e := range s.log {
		if e.seq <= afterSeq {
			continue
		}
		out = append(out, e)
		if len(out) >= maxN {
			break
		}
	}
	return out
}
