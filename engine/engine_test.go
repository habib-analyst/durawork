package engine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp(".", ".test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func newTestExecutor(t *testing.T, dir, id string) (*Store, *Executor) {
	t.Helper()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return store, NewExecutor(store, id, json.RawMessage(`{}`), NewEffects(dir))
}

func countingActivity(counter *int32, result string) ActivityFunc {
	return func(_ context.Context, _ json.RawMessage, _ ActivityMeta) (json.RawMessage, error) {
		atomic.AddInt32(counter, 1)
		return json.RawMessage(result), nil
	}
}

func TestReplayReturnsRecordedResults(t *testing.T) {
	dir := testDir(t)
	var calls int32
	wf := func(ctx *Context) (any, error) {
		res, err := ctx.ExecuteActivity("work", nil, ActivityOptions{})
		if err != nil {
			return nil, err
		}
		return map[string]any{"got": string(res)}, nil
	}
	_, ex1 := newTestExecutor(t, dir, "wf")
	ex1.RegisterActivity("work", countingActivity(&calls, `"done"`))
	if r := ex1.Run(wf); r.Status != StatusCompleted {
		t.Fatalf("first run: %v (%v)", r.Status, r.Err)
	}
	// A brand-new executor over the same store must replay, not re-execute.
	_, ex2 := newTestExecutor(t, dir, "wf")
	ex2.RegisterActivity("work", countingActivity(&calls, `"done"`))
	r := ex2.Run(wf)
	if r.Status != StatusCompleted {
		t.Fatalf("second run: %v (%v)", r.Status, r.Err)
	}
	if calls != 1 {
		t.Fatalf("activity executed %d times, want 1 (replay must return the recorded result)", calls)
	}
}

func TestRetryBackoffAccounting(t *testing.T) {
	dir := testDir(t)
	var calls int32
	flaky := func(_ context.Context, _ json.RawMessage, _ ActivityMeta) (json.RawMessage, error) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return nil, errors.New("transient boom")
		}
		return json.RawMessage(`"ok"`), nil
	}
	store, ex := newTestExecutor(t, dir, "wf")
	ex.RegisterActivity("flaky", flaky)
	wf := func(ctx *Context) (any, error) {
		res, err := ctx.ExecuteActivity("flaky", nil, ActivityOptions{MaxAttempts: 3, InitialBackoff: time.Millisecond})
		if err != nil {
			return nil, err
		}
		return string(res), nil
	}
	if r := ex.Run(wf); r.Status != StatusCompleted {
		t.Fatalf("run: %v (%v)", r.Status, r.Err)
	}
	if calls != 3 {
		t.Fatalf("attempts = %d, want 3", calls)
	}
	events, err := store.Load("wf")
	if err != nil {
		t.Fatal(err)
	}
	var fails, completed int
	for _, ev := range events {
		switch ev.Type {
		case EvActivityAttemptFail:
			fails++
		case EvActivityCompleted:
			completed++
			if ev.Attempt != 3 {
				t.Fatalf("completed on attempt %d, want 3", ev.Attempt)
			}
		}
	}
	if fails != 2 || completed != 1 {
		t.Fatalf("history: %d attempt failures, %d completions; want 2 and 1", fails, completed)
	}
}

func TestRetryExhaustionFailsWorkflow(t *testing.T) {
	dir := testDir(t)
	var calls int32
	always := func(_ context.Context, _ json.RawMessage, _ ActivityMeta) (json.RawMessage, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("permanent boom")
	}
	_, ex := newTestExecutor(t, dir, "wf")
	ex.RegisterActivity("always", always)
	wf := func(ctx *Context) (any, error) {
		_, err := ctx.ExecuteActivity("always", nil, ActivityOptions{MaxAttempts: 2, InitialBackoff: time.Millisecond})
		return nil, err
	}
	r := ex.Run(wf)
	if r.Status != StatusFailed {
		t.Fatalf("status = %v, want failed", r.Status)
	}
	if calls != 2 {
		t.Fatalf("attempts = %d, want 2", calls)
	}
}

func TestTimerSurvivesRestart(t *testing.T) {
	dir := testDir(t)
	var calls int32
	wf := func(ctx *Context) (any, error) {
		if err := ctx.Sleep(250 * time.Millisecond); err != nil {
			return nil, err
		}
		return ctx.ExecuteActivity("after", nil, ActivityOptions{})
	}
	_, ex1 := newTestExecutor(t, dir, "wf")
	ex1.maxAutoWait = 30 * time.Millisecond // give up waiting quickly: simulates process exit
	ex1.RegisterActivity("after", countingActivity(&calls, `"ran"`))
	r := ex1.Run(wf)
	if r.Status != StatusWaiting || r.WaitReason != "timer" {
		t.Fatalf("first run: status=%v reason=%v, want waiting/timer", r.Status, r.WaitReason)
	}
	if calls != 0 {
		t.Fatalf("activity ran before the timer fired")
	}
	// "Restart" after the deadline: the overdue timer must fire on recovery.
	time.Sleep(300 * time.Millisecond)
	_, ex2 := newTestExecutor(t, dir, "wf")
	ex2.RegisterActivity("after", countingActivity(&calls, `"ran"`))
	r = ex2.Run(wf)
	if r.Status != StatusCompleted {
		t.Fatalf("second run: %v (%v)", r.Status, r.Err)
	}
	if calls != 1 {
		t.Fatalf("activity executed %d times, want 1", calls)
	}
}

func TestSignalDeliveryAndTimeout(t *testing.T) {
	dir := testDir(t)
	wf := func(ctx *Context) (any, error) {
		sig, err := ctx.WaitSignal("approval", 200*time.Millisecond)
		if err != nil {
			return nil, err
		}
		if sig.TimedOut {
			return "timed-out", nil
		}
		return "got:" + string(sig.Payload), nil
	}
	store, ex1 := newTestExecutor(t, dir, "wf-signal")
	ex1.maxAutoWait = 20 * time.Millisecond
	if r := ex1.Run(wf); r.Status != StatusWaiting || r.WaitReason != "signal" {
		t.Fatalf("first run: %v/%v, want waiting/signal", r.Status, r.WaitReason)
	}
	if _, err := store.SendSignal("wf-signal", "approval", json.RawMessage(`{"decision":"approve"}`)); err != nil {
		t.Fatal(err)
	}
	_, ex2 := newTestExecutor(t, dir, "wf-signal")
	r := ex2.Run(wf)
	if r.Status != StatusCompleted || !strings.Contains(string(r.Output), "decision") {
		t.Fatalf("after signal: %v output=%s err=%v", r.Status, r.Output, r.Err)
	}

	// Timeout path on a fresh workflow.
	_, ex3 := newTestExecutor(t, dir, "wf-timeout")
	r = ex3.Run(wf) // maxAutoWait is generous here: Run waits out the deadline itself
	if r.Status != StatusCompleted || string(r.Output) != `"timed-out"` {
		t.Fatalf("timeout run: %v output=%s err=%v", r.Status, r.Output, r.Err)
	}
}

func TestNonDeterminismDetected(t *testing.T) {
	dir := testDir(t)
	var calls int32
	reg := func(ex *Executor) {
		ex.RegisterActivity("a", countingActivity(&calls, `"a"`))
		ex.RegisterActivity("b", countingActivity(&calls, `"b"`))
		ex.RegisterActivity("c", countingActivity(&calls, `"c"`))
	}
	v1 := func(ctx *Context) (any, error) {
		if _, err := ctx.ExecuteActivity("a", nil, ActivityOptions{}); err != nil {
			return nil, err
		}
		// Stop after one command: history now records cmd0=a, and the
		// workflow then blocks on a long timer so it stays in flight.
		if err := ctx.Sleep(time.Hour); err != nil {
			return nil, err
		}
		if _, err := ctx.ExecuteActivity("b", nil, ActivityOptions{}); err != nil {
			return nil, err
		}
		return "v1", nil
	}
	_, ex1 := newTestExecutor(t, dir, "wf")
	ex1.maxAutoWait = 20 * time.Millisecond
	reg(ex1)
	if r := ex1.Run(v1); r.Status != StatusWaiting {
		t.Fatalf("v1 setup run: %v, want waiting", r.Status)
	}
	// "Deploy" v2: command 1 is now activity c instead of the timer.
	v2 := func(ctx *Context) (any, error) {
		if _, err := ctx.ExecuteActivity("a", nil, ActivityOptions{}); err != nil {
			return nil, err
		}
		if _, err := ctx.ExecuteActivity("c", nil, ActivityOptions{}); err != nil {
			return nil, err
		}
		return "v2", nil
	}
	store, ex2 := newTestExecutor(t, dir, "wf")
	reg(ex2)
	r := ex2.Run(v2)
	if r.Status != StatusParked {
		t.Fatalf("v2 run: status=%v err=%v, want parked", r.Status, r.Err)
	}
	var ndErr *NonDeterminismError
	if !errors.As(r.Err, &ndErr) {
		t.Fatalf("park error is %T, want *NonDeterminismError", r.Err)
	}
	if ndErr.Cmd != 1 {
		t.Fatalf("divergence at cmd %d, want 1", ndErr.Cmd)
	}
	events, err := store.Load("wf")
	if err != nil {
		t.Fatal(err)
	}
	parked := false
	for _, ev := range events {
		if ev.Type == EvWorkflowParked {
			parked = true
		}
	}
	if !parked {
		t.Fatal("no WorkflowParked event recorded")
	}
}

func TestEarlyReturnDetected(t *testing.T) {
	dir := testDir(t)
	var calls int32
	full := func(ctx *Context) (any, error) {
		if _, err := ctx.ExecuteActivity("a", nil, ActivityOptions{}); err != nil {
			return nil, err
		}
		if err := ctx.Sleep(time.Hour); err != nil {
			return nil, err
		}
		return "full", nil
	}
	_, ex1 := newTestExecutor(t, dir, "wf")
	ex1.maxAutoWait = 20 * time.Millisecond
	ex1.RegisterActivity("a", countingActivity(&calls, `"a"`))
	if r := ex1.Run(full); r.Status != StatusWaiting {
		t.Fatalf("setup run: %v, want waiting", r.Status)
	}
	short := func(ctx *Context) (any, error) {
		if _, err := ctx.ExecuteActivity("a", nil, ActivityOptions{}); err != nil {
			return nil, err
		}
		return "short", nil // history still has a timer command outstanding
	}
	_, ex2 := newTestExecutor(t, dir, "wf")
	ex2.RegisterActivity("a", countingActivity(&calls, `"a"`))
	if r := ex2.Run(short); r.Status != StatusParked {
		t.Fatalf("short run: %v (%v), want parked", r.Status, r.Err)
	}
}

func TestEffectsDedup(t *testing.T) {
	dir := testDir(t)
	eff := NewEffects(dir)
	_, already, err := eff.Do("k1", "email_sent", map[string]any{"template": "welcome"})
	if err != nil || already {
		t.Fatalf("first Do: already=%v err=%v", already, err)
	}
	rec, already, err := eff.Do("k1", "email_sent", map[string]any{"template": "welcome"})
	if err != nil || !already || rec.Key != "k1" {
		t.Fatalf("second Do: rec=%+v already=%v err=%v", rec, already, err)
	}
	all, err := eff.All()
	if err != nil || len(all) != 1 {
		t.Fatalf("journal holds %d effects, want 1", len(all))
	}
}

func TestTornFinalLineTolerated(t *testing.T) {
	dir := testDir(t)
	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append("wf", Event{Type: EvWorkflowStarted, Cmd: -1}); err != nil {
		t.Fatal(err)
	}
	// Simulate a kill -9 mid-append: a partial JSON line with no newline.
	f, err := os.OpenFile(filepath.Join(dir, "wf", "history.jsonl"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"seq":2,"type":"ActivitySched`); err != nil {
		t.Fatal(err)
	}
	f.Close()
	events, err := store.Load("wf")
	if err != nil {
		t.Fatalf("load with torn final line: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("loaded %d events, want 1 (torn line ignored)", len(events))
	}
}
