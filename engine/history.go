package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// EventType enumerates every record that can appear in a workflow history.
type EventType string

const (
	EvWorkflowStarted      EventType = "WorkflowStarted"
	EvActivityScheduled    EventType = "ActivityScheduled"
	EvActivityAttemptStart EventType = "ActivityAttemptStarted"
	EvActivityAttemptFail  EventType = "ActivityAttemptFailed"
	EvActivityCompleted    EventType = "ActivityCompleted"
	EvActivityFailed       EventType = "ActivityFailed" // retries exhausted
	EvTimerStarted         EventType = "TimerStarted"
	EvTimerFired           EventType = "TimerFired"
	EvSignalWaitStarted    EventType = "SignalWaitStarted"
	EvSignalClaimed        EventType = "SignalClaimed"
	EvSignalTimedOut       EventType = "SignalTimedOut"
	EvWorkflowCompleted    EventType = "WorkflowCompleted"
	EvWorkflowFailed       EventType = "WorkflowFailed"
	EvWorkflowParked       EventType = "WorkflowParked" // non-determinism detected
)

// Event is one line in a workflow's append-only history file.
type Event struct {
	Seq            int             `json:"seq"`
	Type           EventType       `json:"type"`
	Cmd            int             `json:"cmd"` // command index within the workflow; -1 = workflow-level
	Name           string          `json:"name,omitempty"`
	Attempt        int             `json:"attempt,omitempty"`
	Input          json.RawMessage `json:"input,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
	Error          string          `json:"error,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	DurationMs     int64           `json:"duration_ms,omitempty"`
	FireAtUnixMs   int64           `json:"fire_at_unix_ms,omitempty"`
	DeadlineUnixMs int64           `json:"deadline_unix_ms,omitempty"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	SignalSeq      int             `json:"signal_seq,omitempty"`
	Reason         string          `json:"reason,omitempty"`
	AtUnixMs       int64           `json:"at_unix_ms"`
}

// Signal is one entry in a workflow's durable signal inbox.
type Signal struct {
	Seq      int             `json:"seq"`
	Name     string          `json:"name"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	AtUnixMs int64           `json:"at_unix_ms"`
}

// Store is a directory of per-workflow history files.
//
// Layout: <dir>/<workflowID>/history.jsonl, signals.jsonl, lock.
// Every append is a single line write followed by fsync, so a kill -9 can
// lose at most the append in flight; readers tolerate one torn final line.
type Store struct {
	dir string
	mu  sync.Mutex
}

func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

func (s *Store) wfDir(id string) string { return filepath.Join(s.dir, id) }

func (s *Store) lock(id string) (*os.File, error) {
	wd := s.wfDir(id)
	if err := os.MkdirAll(wd, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(wd, "lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func unlock(f *os.File) {
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	f.Close()
}

// appendLine appends one JSON line to a file and fsyncs it. When the file is
// created, the containing directory is fsynced too so the name is durable.
func appendLine(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, statErr := os.Stat(path)
	created := os.IsNotExist(statErr)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if created {
		d, err := os.Open(filepath.Dir(path))
		if err != nil {
			return err
		}
		defer d.Close()
		return d.Sync()
	}
	return nil
}

// readLines reads a JSONL file, tolerating a single torn final line (the
// signature of a kill -9 mid-append). A corrupt line anywhere else is an
// error, because appends before it were fsynced and must be intact.
func readLines[T any](path string) ([]T, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []T
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var v T
		if err := json.Unmarshal(line, &v); err != nil {
			// Torn write can only be the last line: if anything follows,
			// this is real corruption.
			if !sc.Scan() {
				break
			}
			return nil, fmt.Errorf("%s line %d: corrupt non-final line: %w", path, lineNo, err)
		}
		out = append(out, v)
	}
	return out, sc.Err()
}

// Append records one event in the workflow's history, assigning its Seq.
func (s *Store) Append(id string, ev Event) (Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lf, err := s.lock(id)
	if err != nil {
		return Event{}, err
	}
	defer unlock(lf)
	existing, err := readLines[Event](filepath.Join(s.wfDir(id), "history.jsonl"))
	if err != nil {
		return Event{}, err
	}
	ev.Seq = len(existing) + 1
	if ev.AtUnixMs == 0 {
		ev.AtUnixMs = time.Now().UnixMilli()
	}
	if err := appendLine(filepath.Join(s.wfDir(id), "history.jsonl"), ev); err != nil {
		return Event{}, err
	}
	return ev, nil
}

// Load returns the workflow's full history.
func (s *Store) Load(id string) ([]Event, error) {
	return readLines[Event](filepath.Join(s.wfDir(id), "history.jsonl"))
}

// SendSignal durably records a signal in the workflow's inbox. It is safe to
// call from a separate process while the workflow is running or dead.
func (s *Store) SendSignal(id, name string, payload json.RawMessage) (Signal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lf, err := s.lock(id)
	if err != nil {
		return Signal{}, err
	}
	defer unlock(lf)
	existing, err := readLines[Signal](filepath.Join(s.wfDir(id), "signals.jsonl"))
	if err != nil {
		return Signal{}, err
	}
	sig := Signal{Seq: len(existing) + 1, Name: name, Payload: payload, AtUnixMs: time.Now().UnixMilli()}
	if err := appendLine(filepath.Join(s.wfDir(id), "signals.jsonl"), sig); err != nil {
		return Signal{}, err
	}
	return sig, nil
}

// LoadSignals returns every signal delivered to the workflow so far.
func (s *Store) LoadSignals(id string) ([]Signal, error) {
	return readLines[Signal](filepath.Join(s.wfDir(id), "signals.jsonl"))
}
