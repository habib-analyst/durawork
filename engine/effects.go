package engine

import (
	"encoding/json"
	"path/filepath"
	"time"
)

// Effect is one externally visible side effect, recorded in the effects
// journal. The journal models an idempotent downstream system (a database
// with a unique constraint on Key, an API that honours idempotency keys):
// an effect with a given Key can be recorded at most once, no matter how
// many times it is attempted.
type Effect struct {
	Key      string          `json:"key"`
	Kind     string          `json:"kind"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	AtUnixMs int64           `json:"at_unix_ms"`
}

// Effects is an append-only journal of side effects with key-based dedup.
type Effects struct {
	path string
}

// NewEffects opens (creating if needed) the journal at <dir>/effects.jsonl.
func NewEffects(dir string) *Effects {
	return &Effects{path: filepath.Join(dir, "effects.jsonl")}
}

// Do records the effect unless its key is already present, in which case it
// returns the original record and alreadyDone=true. This is the mechanism
// that turns "activity executed twice after a crash" into "effect happened
// exactly once": the retried activity presents the same idempotency key.
func (e *Effects) Do(key, kind string, payload any) (rec Effect, alreadyDone bool, err error) {
	existing, err := e.All()
	if err != nil {
		return Effect{}, false, err
	}
	for _, r := range existing {
		if r.Key == key {
			return r, true, nil
		}
	}
	var raw json.RawMessage
	if payload != nil {
		raw, err = json.Marshal(payload)
		if err != nil {
			return Effect{}, false, err
		}
	}
	rec = Effect{Key: key, Kind: kind, Payload: raw, AtUnixMs: time.Now().UnixMilli()}
	if err := appendLine(e.path, rec); err != nil {
		return Effect{}, false, err
	}
	return rec, false, nil
}

// All returns every recorded effect in order.
func (e *Effects) All() ([]Effect, error) {
	return readLines[Effect](e.path)
}
