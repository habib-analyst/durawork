package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// ActivityFunc performs one unit of non-deterministic work. Activities are
// the only place side effects may happen, and they must honour the
// idempotency contract: an activity may be executed more than once for the
// same command (crash between effect and completion record), always with the
// same Meta.IdempotencyKey, and must use that key (e.g. via Effects.Do) so
// the externally visible effect still happens exactly once.
type ActivityFunc func(ctx context.Context, input json.RawMessage, meta ActivityMeta) (json.RawMessage, error)

// ActivityMeta describes the invocation an activity receives.
type ActivityMeta struct {
	WorkflowID     string
	ActivityName   string
	IdempotencyKey string
	Attempt        int
	Effects        *Effects
}

// ActivityOptions controls retry behaviour for one activity command.
type ActivityOptions struct {
	MaxAttempts       int           // default 3
	InitialBackoff    time.Duration // default 100ms
	BackoffMultiplier float64       // default 2
	MaxBackoff        time.Duration // default 5s
}

func (o ActivityOptions) withDefaults() ActivityOptions {
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 3
	}
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = 100 * time.Millisecond
	}
	if o.BackoffMultiplier <= 0 {
		o.BackoffMultiplier = 2
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 5 * time.Second
	}
	return o
}

func (o ActivityOptions) backoff(afterAttempt int) time.Duration {
	d := float64(o.InitialBackoff)
	for i := 1; i < afterAttempt; i++ {
		d *= o.BackoffMultiplier
	}
	if d > float64(o.MaxBackoff) {
		d = float64(o.MaxBackoff)
	}
	return time.Duration(d)
}

// WorkflowFunc is a workflow definition: ordinary Go code that calls the
// primitives on Context. It must be deterministic — see NonDeterminismError.
type WorkflowFunc func(ctx *Context) (any, error)

// NonDeterminismError is returned (and recorded, parking the workflow) when
// a replay pass issues a command sequence that diverges from the recorded
// history: the workflow code no longer agrees with its own past. Parking is
// deliberate: continuing would corrupt the meaning of recorded results.
type NonDeterminismError struct {
	Cmd      int
	Expected string
	Got      string
}

func (e *NonDeterminismError) Error() string {
	return fmt.Sprintf("non-determinism at command %d: history recorded %s, replay issued %s", e.Cmd, e.Expected, e.Got)
}

// Status is the state of a workflow after a Run call.
type Status string

const (
	StatusCompleted Status = "completed"
	StatusWaiting   Status = "waiting"
	StatusParked    Status = "parked"
	StatusFailed    Status = "failed"
)

// RunResult describes the outcome of driving a workflow as far as possible.
type RunResult struct {
	Status          Status
	Output          json.RawMessage
	WaitReason      string // "timer" | "signal"
	WaitName        string
	WaitUntilUnixMs int64
	Err             error
}

// Executor drives one workflow instance against a Store.
type Executor struct {
	store       *Store
	wfID        string
	input       json.RawMessage
	activities  map[string]ActivityFunc
	effects     *Effects
	hook        func(label string)
	onWait      func(RunResult)
	now         func() time.Time
	maxAutoWait time.Duration
}

// NewExecutor creates an executor for one workflow instance. input is the
// workflow's start input, recorded in WorkflowStarted on first run.
func NewExecutor(store *Store, wfID string, input json.RawMessage, effects *Effects) *Executor {
	return &Executor{
		store:       store,
		wfID:        wfID,
		input:       input,
		activities:  map[string]ActivityFunc{},
		effects:     effects,
		hook:        func(string) {},
		onWait:      func(RunResult) {},
		now:         time.Now,
		maxAutoWait: 10 * time.Minute,
	}
}

func (ex *Executor) RegisterActivity(name string, fn ActivityFunc) {
	ex.activities[name] = fn
}

// SetHook installs a synchronous callback invoked at labelled execution
// points ("pre-activity-1", "mid-activity-1", "post-timer-4", ...). The crash
// harness uses it to stop the process at exact step boundaries.
func (ex *Executor) SetHook(h func(label string)) { ex.hook = h }

// SetOnWait installs a callback invoked whenever a pass blocks.
func (ex *Executor) SetOnWait(h func(RunResult)) { ex.onWait = h }

// SetClock overrides the wall clock (tests).
func (ex *Executor) SetClock(f func() time.Time) { ex.now = f }

// Context is the workflow's handle on the engine.
type Context struct {
	st         *passState
	WorkflowID string
	Input      json.RawMessage
}

// ---------------------------------------------------------------------------
// Command bookkeeping
// ---------------------------------------------------------------------------

type command struct {
	idx        int
	kind       string // "activity" | "timer" | "signal"
	name       string
	durationMs int64

	actScheduled *Event
	actCompleted *Event
	actFailed    *Event
	idemKey      string
	attempts     int

	timerStarted *Event
	timerFired   bool

	waitStarted    *Event
	signalClaimed  *Event
	signalTimedOut bool
}

func (c *command) describe() string {
	switch c.kind {
	case "activity":
		return fmt.Sprintf("activity %q", c.name)
	case "timer":
		return fmt.Sprintf("timer %dms", c.durationMs)
	case "signal":
		return fmt.Sprintf("signal wait %q timeout %dms", c.name, c.durationMs)
	}
	return "unknown command"
}

func buildCommands(events []Event) []command {
	var cmds []command
	ensure := func(idx int) *command {
		for len(cmds) <= idx {
			cmds = append(cmds, command{idx: len(cmds)})
		}
		return &cmds[idx]
	}
	for i := range events {
		ev := events[i]
		if ev.Cmd < 0 {
			continue
		}
		c := ensure(ev.Cmd)
		switch ev.Type {
		case EvActivityScheduled:
			c.kind, c.name, c.idemKey = "activity", ev.Name, ev.IdempotencyKey
			cp := ev
			c.actScheduled = &cp
		case EvActivityAttemptStart:
			c.attempts++
		case EvActivityCompleted:
			cp := ev
			c.actCompleted = &cp
		case EvActivityFailed:
			cp := ev
			c.actFailed = &cp
		case EvTimerStarted:
			c.kind, c.durationMs = "timer", ev.DurationMs
			cp := ev
			c.timerStarted = &cp
		case EvTimerFired:
			c.timerFired = true
		case EvSignalWaitStarted:
			c.kind, c.name, c.durationMs = "signal", ev.Name, ev.DurationMs
			cp := ev
			c.waitStarted = &cp
		case EvSignalClaimed:
			cp := ev
			c.signalClaimed = &cp
		case EvSignalTimedOut:
			c.signalTimedOut = true
		}
	}
	return cmds
}

func claimedSignalSeqs(events []Event) map[int]bool {
	out := map[int]bool{}
	for _, ev := range events {
		if ev.Type == EvSignalClaimed {
			out[ev.SignalSeq] = true
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Pass machinery. A pass runs the workflow function from the top; primitives
// return recorded outcomes for commands already in history and execute live
// past the frontier. Blocking (timer not due, signal not arrived) unwinds via
// a panic the executor recovers — the same technique Temporal's Go SDK uses
// internally — so workflow code stays straight-line.
// ---------------------------------------------------------------------------

type blockedPanic struct {
	reason   string
	name     string
	untilMs  int64
	isSignal bool
}

type fatalPanic struct{ err error }

type passState struct {
	ex           *Executor
	events       []Event
	commands     []command
	histCmdCount int
	nextCmd      int
}

func (st *passState) append(ev Event) Event {
	ev.Cmd = evCmdOr(ev)
	out, err := st.ex.store.Append(st.ex.wfID, ev)
	if err != nil {
		panic(fatalPanic{err: fmt.Errorf("history append failed: %w", err)})
	}
	st.events = append(st.events, out)
	return out
}

func evCmdOr(ev Event) int { return ev.Cmd }

// beginCommand consumes the next command index, verifying determinism
// against history when the command already exists there.
func (st *passState) beginCommand(kind, name string, durationMs int64) int {
	idx := st.nextCmd
	st.nextCmd++
	if idx < len(st.commands) {
		rec := &st.commands[idx]
		got := (&command{kind: kind, name: name, durationMs: durationMs}).describe()
		if rec.kind != kind || rec.name != name || (kind != "activity" && rec.durationMs != durationMs) {
			panic(fatalPanic{err: &NonDeterminismError{Cmd: idx, Expected: rec.describe(), Got: got}})
		}
		return idx
	}
	st.commands = append(st.commands, command{idx: idx, kind: kind, name: name, durationMs: durationMs})
	return idx
}

func marshal(v any) json.RawMessage {
	if v == nil {
		return nil
	}
	if raw, ok := v.(json.RawMessage); ok {
		return raw
	}
	data, err := json.Marshal(v)
	if err != nil {
		panic(fatalPanic{err: err})
	}
	return data
}

// ---------------------------------------------------------------------------
// Primitives
// ---------------------------------------------------------------------------

// ExecuteActivity runs an activity, or returns its recorded result when the
// command already completed in a previous life of the workflow.
func (c *Context) ExecuteActivity(name string, input any, opts ActivityOptions) (json.RawMessage, error) {
	st := c.st
	opts = opts.withDefaults()
	idx := st.beginCommand("activity", name, 0)
	rec := &st.commands[idx]
	if rec.actCompleted != nil {
		return rec.actCompleted.Result, nil
	}
	if rec.actFailed != nil {
		return nil, fmt.Errorf("activity %q failed after %d attempts: %s", name, rec.attempts, rec.actFailed.Error)
	}
	fn, ok := st.ex.activities[name]
	if !ok {
		return nil, fmt.Errorf("activity %q is not registered", name)
	}
	inRaw := marshal(input)
	key := fmt.Sprintf("%s/%d/%s", st.ex.wfID, idx, name)
	if rec.actScheduled == nil {
		ev := st.append(Event{Type: EvActivityScheduled, Cmd: idx, Name: name, Input: inRaw, IdempotencyKey: key})
		rec.actScheduled = &ev
		rec.idemKey = key
	}
	st.ex.hook(fmt.Sprintf("pre-activity-%d", idx))
	var lastErr error
	for attempt := rec.attempts + 1; attempt <= opts.MaxAttempts; attempt++ {
		st.append(Event{Type: EvActivityAttemptStart, Cmd: idx, Name: name, Attempt: attempt})
		res, aerr := fn(context.Background(), inRaw, ActivityMeta{
			WorkflowID:     st.ex.wfID,
			ActivityName:   name,
			IdempotencyKey: rec.idemKey,
			Attempt:        attempt,
			Effects:        st.ex.effects,
		})
		if aerr == nil {
			// The effect has happened; the completion record has not. This
			// is the hardest crash point in durable execution, and the one
			// the idempotency key exists for.
			st.ex.hook(fmt.Sprintf("mid-activity-%d", idx))
			ev := st.append(Event{Type: EvActivityCompleted, Cmd: idx, Name: name, Attempt: attempt, Result: res})
			rec.actCompleted = &ev
			st.ex.hook(fmt.Sprintf("post-activity-%d", idx))
			return res, nil
		}
		lastErr = aerr
		st.append(Event{Type: EvActivityAttemptFail, Cmd: idx, Name: name, Attempt: attempt, Error: aerr.Error()})
		if attempt < opts.MaxAttempts {
			time.Sleep(opts.backoff(attempt))
		}
	}
	errText := ""
	if lastErr != nil {
		errText = lastErr.Error()
	}
	ev := st.append(Event{Type: EvActivityFailed, Cmd: idx, Name: name, Error: errText})
	rec.actFailed = &ev
	return nil, fmt.Errorf("activity %q exhausted %d attempts: %s", name, opts.MaxAttempts, errText)
}

// Sleep blocks the workflow for d, durably: the timer survives process
// death and fires on recovery if it came due while the process was down.
func (c *Context) Sleep(d time.Duration) error {
	st := c.st
	idx := st.beginCommand("timer", "", d.Milliseconds())
	rec := &st.commands[idx]
	if rec.timerFired {
		return nil
	}
	nowMs := st.ex.now().UnixMilli()
	if rec.timerStarted == nil {
		ev := st.append(Event{Type: EvTimerStarted, Cmd: idx, DurationMs: d.Milliseconds(), FireAtUnixMs: nowMs + d.Milliseconds()})
		rec.timerStarted = &ev
	}
	fireAt := rec.timerStarted.FireAtUnixMs
	if nowMs >= fireAt {
		st.append(Event{Type: EvTimerFired, Cmd: idx})
		rec.timerFired = true
		st.ex.hook(fmt.Sprintf("post-timer-%d", idx))
		return nil
	}
	st.ex.hook(fmt.Sprintf("pre-timer-%d", idx))
	panic(blockedPanic{reason: "timer", untilMs: fireAt})
}

// SignalResult is the outcome of WaitSignal: a delivered payload, or a timeout.
type SignalResult struct {
	Payload  json.RawMessage
	TimedOut bool
}

// WaitSignal blocks until a signal with the given name is delivered to the
// workflow's durable inbox, or the timeout (also durable, raced against the
// signal) elapses.
func (c *Context) WaitSignal(name string, timeout time.Duration) (SignalResult, error) {
	st := c.st
	idx := st.beginCommand("signal", name, timeout.Milliseconds())
	rec := &st.commands[idx]
	if rec.signalClaimed != nil {
		return SignalResult{Payload: rec.signalClaimed.Payload}, nil
	}
	if rec.signalTimedOut {
		return SignalResult{TimedOut: true}, nil
	}
	nowMs := st.ex.now().UnixMilli()
	if rec.waitStarted == nil {
		ev := st.append(Event{Type: EvSignalWaitStarted, Cmd: idx, Name: name, DurationMs: timeout.Milliseconds(), DeadlineUnixMs: nowMs + timeout.Milliseconds()})
		rec.waitStarted = &ev
	}
	// Claim the oldest unclaimed matching signal, if one has arrived.
	sigs, err := st.ex.store.LoadSignals(st.ex.wfID)
	if err != nil {
		return SignalResult{}, err
	}
	claimed := claimedSignalSeqs(st.events)
	for _, sig := range sigs {
		if sig.Name == name && !claimed[sig.Seq] {
			ev := st.append(Event{Type: EvSignalClaimed, Cmd: idx, Name: name, Payload: sig.Payload, SignalSeq: sig.Seq})
			rec.signalClaimed = &ev
			st.ex.hook(fmt.Sprintf("post-signal-%d", idx))
			return SignalResult{Payload: sig.Payload}, nil
		}
	}
	if nowMs >= rec.waitStarted.DeadlineUnixMs {
		st.append(Event{Type: EvSignalTimedOut, Cmd: idx, Name: name})
		rec.signalTimedOut = true
		st.ex.hook(fmt.Sprintf("post-signal-%d", idx))
		return SignalResult{TimedOut: true}, nil
	}
	st.ex.hook(fmt.Sprintf("pre-signal-%d", idx))
	panic(blockedPanic{reason: "signal", name: name, untilMs: rec.waitStarted.DeadlineUnixMs, isSignal: true})
}

// ---------------------------------------------------------------------------
// Run loop
// ---------------------------------------------------------------------------

// Run drives the workflow as far as it can go: replaying, executing live
// past the frontier, auto-waiting on timers (and on signal deadlines, while
// polling the inbox) until the workflow completes, fails, parks, or blocks
// on a signal with no near deadline.
func (ex *Executor) Run(wf WorkflowFunc) (result RunResult) {
	for pass := 0; pass < 100000; pass++ {
		events, err := ex.store.Load(ex.wfID)
		if err != nil {
			return RunResult{Status: StatusFailed, Err: err}
		}
		if r, done := terminalState(events); done {
			return r
		}
		started := false
		for _, ev := range events {
			if ev.Type == EvWorkflowStarted {
				started = true
			}
		}
		if !started {
			if _, err := ex.store.Append(ex.wfID, Event{Type: EvWorkflowStarted, Cmd: -1, Input: ex.input}); err != nil {
				return RunResult{Status: StatusFailed, Err: err}
			}
			ex.hook("wf-start")
			events, err = ex.store.Load(ex.wfID)
			if err != nil {
				return RunResult{Status: StatusFailed, Err: err}
			}
		}
		res, blocked := ex.runPass(wf, events)
		if blocked == nil {
			return res
		}
		wait := RunResult{
			Status:          StatusWaiting,
			WaitReason:      blocked.reason,
			WaitName:        blocked.name,
			WaitUntilUnixMs: blocked.untilMs,
		}
		ex.onWait(wait)
		waitDur := time.Duration(blocked.untilMs-ex.now().UnixMilli()) * time.Millisecond
		if waitDur > ex.maxAutoWait {
			return wait
		}
		if waitDur < 0 {
			waitDur = 0
		}
		if blocked.isSignal {
			// Poll the inbox so a signal arriving during the wait resumes
			// the workflow promptly instead of at the deadline.
			deadline := time.Now().Add(waitDur)
			for {
				sigs, serr := ex.store.LoadSignals(ex.wfID)
				if serr == nil {
					evs, lerr := ex.store.Load(ex.wfID)
					if lerr == nil {
						claimed := claimedSignalSeqs(evs)
						for _, sig := range sigs {
							if sig.Name == blocked.name && !claimed[sig.Seq] {
								goto nextPass
							}
						}
					}
				}
				if time.Now().After(deadline) {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
		} else {
			time.Sleep(waitDur + 5*time.Millisecond)
		}
	nextPass:
	}
	return RunResult{Status: StatusFailed, Err: errors.New("run loop exceeded pass limit")}
}

func terminalState(events []Event) (RunResult, bool) {
	for _, ev := range events {
		switch ev.Type {
		case EvWorkflowCompleted:
			return RunResult{Status: StatusCompleted, Output: ev.Result}, true
		case EvWorkflowParked:
			return RunResult{Status: StatusParked, Err: errors.New(ev.Reason)}, true
		case EvWorkflowFailed:
			return RunResult{Status: StatusFailed, Err: errors.New(ev.Error)}, true
		}
	}
	return RunResult{}, false
}

func (ex *Executor) runPass(wf WorkflowFunc, events []Event) (result RunResult, blocked *blockedPanic) {
	st := &passState{
		ex:           ex,
		events:       events,
		commands:     buildCommands(events),
		histCmdCount: 0,
	}
	st.histCmdCount = len(st.commands)
	ctx := &Context{st: st, WorkflowID: ex.wfID, Input: ex.input}
	defer func() {
		if r := recover(); r != nil {
			switch v := r.(type) {
			case blockedPanic:
				bp := v
				blocked = &bp
				result = RunResult{Status: StatusWaiting}
			case fatalPanic:
				var ndErr *NonDeterminismError
				if errors.As(v.err, &ndErr) {
					_, _ = ex.store.Append(ex.wfID, Event{Type: EvWorkflowParked, Cmd: -1, Reason: ndErr.Error()})
					result = RunResult{Status: StatusParked, Err: ndErr}
				} else {
					result = RunResult{Status: StatusFailed, Err: v.err}
				}
			default:
				panic(r)
			}
		}
	}()
	out, err := wf(ctx)
	if err != nil {
		_, _ = ex.store.Append(ex.wfID, Event{Type: EvWorkflowFailed, Cmd: -1, Error: err.Error()})
		return RunResult{Status: StatusFailed, Err: err}, nil
	}
	if st.nextCmd < st.histCmdCount {
		rec := st.commands[st.nextCmd]
		ndErr := &NonDeterminismError{Cmd: st.nextCmd, Expected: rec.describe(), Got: "workflow returned (ended early)"}
		_, _ = ex.store.Append(ex.wfID, Event{Type: EvWorkflowParked, Cmd: -1, Reason: ndErr.Error()})
		return RunResult{Status: StatusParked, Err: ndErr}, nil
	}
	outRaw := marshal(out)
	ex.hook("pre-complete")
	_, _ = ex.store.Append(ex.wfID, Event{Type: EvWorkflowCompleted, Cmd: -1, Result: outRaw})
	return RunResult{Status: StatusCompleted, Output: outRaw}, nil
}
