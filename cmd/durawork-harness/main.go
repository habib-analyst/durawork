// Command durawork-harness is the crash proof. For the demo workflow it
// enumerates every step boundary, starts the worker as a real OS process,
// stops it at the boundary, kills it with SIGKILL, restarts it, drives it to
// completion, and has an independent oracle verify that every externally
// visible effect happened exactly once and the final state is correct.
//
// The oracle is deliberately independent: it shares no code with the engine
// and judges only from the artefacts a real operator would have — the
// history file, the effects journal, and the workflow's result.
//
// It runs the identical procedure against durawork-naive, the baseline
// script, and reports the contrast.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// Boundary sets. Labels are emitted by the engine's hook points (and the
// naive script's identical instrumentation) at exact step boundaries.
// ---------------------------------------------------------------------------

var engineBoundaries = []string{
	"wf-start",
	"pre-activity-0", "mid-activity-0", "post-activity-0",
	"pre-activity-1", "mid-activity-1", "post-activity-1",
	"pre-signal-2", "post-signal-2",
	"pre-activity-3", "mid-activity-3", "post-activity-3",
	"pre-timer-4", "post-timer-4",
	"pre-activity-5", "mid-activity-5", "post-activity-5",
	"pre-timer-6", "post-timer-6",
	"pre-activity-7", "mid-activity-7", "post-activity-7",
	"pre-complete",
}

var naiveBoundaries = append(append([]string{}, engineBoundaries...),
	"ckptwrite-0", "ckptwrite-1", "ckptwrite-2", "ckptwrite-3",
	"ckptwrite-4", "ckptwrite-5", "ckptwrite-6", "ckptwrite-7")

// ---------------------------------------------------------------------------
// Process driving
// ---------------------------------------------------------------------------

type lockedBuf struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}
func (b *lockedBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type proc struct {
	cmd    *exec.Cmd
	lines  chan string
	done   chan error
	stderr *lockedBuf
}

func startProc(name string, args ...string) (*proc, error) {
	cmd := exec.Command(name, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	p := &proc{cmd: cmd, lines: make(chan string, 256), done: make(chan error, 1), stderr: &lockedBuf{}}
	cmd.Stderr = p.stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	go func() {
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for sc.Scan() {
			p.lines <- sc.Text()
		}
		close(p.lines)
	}()
	go func() { p.done <- cmd.Wait() }()
	return p, nil
}

// driveResult reports how a phase ended.
type driveResult struct {
	killed   bool
	exitCode int
	timedOut bool
}

// drive watches a process until it is killed at killLabel, exits, or times
// out. It delivers the approval signal the first time the process reports
// waiting for one.
func drive(p *proc, killLabel string, sendSignal func(), timeout time.Duration) driveResult {
	signalSent := false
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	lines := p.lines
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				lines = nil
				continue
			}
			if strings.HasPrefix(line, "KILLPOINT ") && killLabel != "" {
				_ = p.cmd.Process.Signal(syscall.SIGKILL)
				<-p.done
				return driveResult{killed: true, exitCode: -9}
			}
			if strings.Contains(line, "WAITING") && strings.Contains(line, "reason=signal") && !signalSent {
				signalSent = true
				sendSignal()
			}
		case err := <-p.done:
			code := 0
			if err != nil {
				if ee, ok := err.(*exec.ExitError); ok {
					code = ee.ExitCode()
				} else {
					code = -1
				}
			}
			return driveResult{exitCode: code}
		case <-timer.C:
			_ = p.cmd.Process.Signal(syscall.SIGKILL)
			<-p.done
			return driveResult{timedOut: true, exitCode: -9}
		}
	}
}

// ---------------------------------------------------------------------------
// Oracle — independent verification from artefacts only.
// ---------------------------------------------------------------------------

type histEvent struct {
	Type   string          `json:"type"`
	Result json.RawMessage `json:"result"`
	Reason string          `json:"reason"`
}

type effectLine struct {
	Key     string `json:"key"`
	Kind    string `json:"kind"`
	Payload struct {
		Template string `json:"template"`
	} `json:"payload"`
}

func readJSONLines[T any](path string) ([]T, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []T
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var v T
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			return nil, fmt.Errorf("unparsable line in %s: %w", path, err)
		}
		out = append(out, v)
	}
	return out, nil
}

type oracleVerdict struct {
	pass   bool
	detail string
}

// oracleEngine checks a completed durawork onboarding run: final state
// approved, and each of the five expected effects present exactly once.
func oracleEngine(dir, wfID string) oracleVerdict {
	hist, err := readJSONLines[histEvent](filepath.Join(dir, wfID, "history.jsonl"))
	if err != nil {
		return oracleVerdict{detail: "history: " + err.Error()}
	}
	completed := false
	for _, ev := range hist {
		if ev.Type == "WorkflowCompleted" {
			var out struct {
				Status    string `json:"status"`
				AccountID string `json:"account_id"`
			}
			if err := json.Unmarshal(ev.Result, &out); err == nil && out.Status == "approved" && out.AccountID == "acct-u1" {
				completed = true
			}
		}
		if ev.Type == "WorkflowParked" || ev.Type == "WorkflowFailed" {
			return oracleVerdict{detail: "workflow ended as " + ev.Type + ": " + ev.Reason}
		}
	}
	if !completed {
		return oracleVerdict{detail: "no WorkflowCompleted(approved) in history"}
	}
	effects, err := readJSONLines[effectLine](filepath.Join(dir, "effects.jsonl"))
	if err != nil {
		return oracleVerdict{detail: "effects: " + err.Error()}
	}
	counts := map[string]int{}
	for _, e := range effects {
		counts[e.Key]++
	}
	wantKeys := []string{
		wfID + "/0/create_account", wfID + "/1/send_email", wfID + "/3/provision_workspace",
		wfID + "/5/send_email", wfID + "/7/send_email",
	}
	for _, k := range wantKeys {
		if counts[k] != 1 {
			return oracleVerdict{detail: fmt.Sprintf("effect %s count=%d, want exactly 1", k, counts[k])}
		}
	}
	if len(effects) != len(wantKeys) {
		return oracleVerdict{detail: fmt.Sprintf("%d effects recorded, want %d (unexpected effects present)", len(effects), len(wantKeys))}
	}
	return oracleVerdict{pass: true}
}

// oracleNaive checks the baseline's artefacts the same way, by kind/template
// (the naive script has no keys — that absence is part of what is measured).
func oracleNaive(dir string) oracleVerdict {
	data, err := os.ReadFile(filepath.Join(dir, "result.json"))
	if err != nil {
		return oracleVerdict{detail: "no result.json: " + err.Error()}
	}
	var res struct {
		Status    string `json:"status"`
		AccountID string `json:"account_id"`
	}
	if err := json.Unmarshal(data, &res); err != nil || res.Status != "approved" || res.AccountID != "acct-u1" {
		return oracleVerdict{detail: fmt.Sprintf("final state status=%q account=%q", res.Status, res.AccountID)}
	}
	// The naive journal is flat ({"kind":..., "template":...} at top level) —
	// it has no envelope and no keys. Parse it in its own shape.
	type naiveEffect struct {
		Kind     string `json:"kind"`
		Template string `json:"template"`
	}
	effects, err := readJSONLines[naiveEffect](filepath.Join(dir, "effects.jsonl"))
	if err != nil {
		return oracleVerdict{detail: "effects: " + err.Error()}
	}
	counts := map[string]int{}
	for _, e := range effects {
		k := e.Kind
		if e.Template != "" {
			k += ":" + e.Template
		}
		counts[k]++
	}
	want := map[string]int{
		"account_created": 1, "email_sent:welcome": 1, "workspace_provisioned": 1,
		"email_sent:checkin_day2": 1, "email_sent:checkin_day3": 1,
	}
	for k, v := range want {
		if counts[k] != v {
			return oracleVerdict{detail: fmt.Sprintf("effect %s count=%d, want exactly 1 (all: %v)", k, counts[k], counts)}
		}
	}
	if len(effects) != 5 {
		return oracleVerdict{detail: fmt.Sprintf("%d effects recorded, want 5", len(effects))}
	}
	return oracleVerdict{pass: true}
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

type scenarioResult struct {
	Boundary string `json:"boundary"`
	Killed   bool   `json:"killed"`
	Pass     bool   `json:"oracle_pass"`
	Detail   string `json:"detail,omitempty"`
}

type suiteResult struct {
	BoundariesTested int              `json:"boundaries_tested"`
	BoundariesPassed int              `json:"boundaries_passed"`
	Results          []scenarioResult `json:"results"`
}

type bins struct {
	runner, signal, naive string
}

func runBoundary(kind, boundary string, b bins, runsRoot string) scenarioResult {
	dir := filepath.Join(runsRoot, kind, boundary)
	_ = os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return scenarioResult{Boundary: boundary, Detail: "mkdir: " + err.Error()}
	}
	res := scenarioResult{Boundary: boundary}

	var startArgs []string
	var binary string
	sendSignal := func() {}
	if kind == "durawork" {
		binary = b.runner
		startArgs = []string{"-dir", dir, "-id", "wf1", "-day-ms", "300", "-approval-timeout-ms", "6000"}
		sendSignal = func() {
			_ = exec.Command(b.signal, "-dir", dir, "-id", "wf1", "-name", "approval", "-payload", `{"decision":"approve"}`).Run()
		}
	} else {
		binary = b.naive
		startArgs = []string{"-dir", dir, "-day-ms", "300", "-approval-timeout-ms", "6000"}
		sendSignal = func() {
			_ = exec.Command(b.naive, "-dir", dir, "-signal", "approval", "-payload", `{"decision":"approve"}`).Run()
		}
	}

	// Phase 1: run to the boundary, then SIGKILL.
	p1, err := startProc(binary, append(append([]string{}, startArgs...), "-kill-at", boundary)...)
	if err != nil {
		res.Detail = "start phase1: " + err.Error()
		return res
	}
	d1 := drive(p1, boundary, sendSignal, 45*time.Second)
	if !d1.killed {
		res.Detail = fmt.Sprintf("killpoint %s never reached in phase 1 (exit=%d timeout=%v)", boundary, d1.exitCode, d1.timedOut)
		return res
	}
	res.Killed = true

	// Phase 2: restart and drive to completion.
	p2, err := startProc(binary, startArgs...)
	if err != nil {
		res.Detail = "start phase2: " + err.Error()
		return res
	}
	d2 := drive(p2, "", sendSignal, 45*time.Second)
	if d2.exitCode != 0 {
		res.Detail = fmt.Sprintf("phase 2 exit=%d timeout=%v", d2.exitCode, d2.timedOut)
		return res
	}

	// Oracle.
	var v oracleVerdict
	if kind == "durawork" {
		v = oracleEngine(dir, "wf1")
	} else {
		v = oracleNaive(dir)
	}
	res.Pass = v.pass
	res.Detail = v.detail
	return res
}

func runSuite(kind string, boundaries []string, b bins, runsRoot string) suiteResult {
	suite := suiteResult{BoundariesTested: len(boundaries)}
	for _, boundary := range boundaries {
		r := runBoundary(kind, boundary, b, runsRoot)
		if r.Pass {
			suite.BoundariesPassed++
		}
		status := "PASS"
		if !r.Pass {
			status = "FAIL"
		}
		fmt.Printf("[%s] %-16s %s %s\n", kind, boundary, status, r.Detail)
		suite.Results = append(suite.Results, r)
	}
	return suite
}

// runFunctional drives the reject and timeout paths (no kills).
func runFunctional(b bins, runsRoot string) map[string]bool {
	out := map[string]bool{}

	// Reject path.
	dir := filepath.Join(runsRoot, "functional", "reject")
	_ = os.RemoveAll(dir)
	_ = os.MkdirAll(dir, 0o755)
	p, err := startProc(b.runner, "-dir", dir, "-id", "wf1", "-day-ms", "200", "-approval-timeout-ms", "6000")
	if err == nil {
		send := func() {
			_ = exec.Command(b.signal, "-dir", dir, "-id", "wf1", "-name", "approval", "-payload", `{"decision":"reject"}`).Run()
		}
		d := drive(p, "", send, 30*time.Second)
		if d.exitCode == 0 {
			effects, err := readJSONLines[effectLine](filepath.Join(dir, "effects.jsonl"))
			hist, herr := readJSONLines[histEvent](filepath.Join(dir, "wf1", "history.jsonl"))
			rejected := false
			for _, ev := range hist {
				if ev.Type == "WorkflowCompleted" && strings.Contains(string(ev.Result), `"rejected"`) {
					rejected = true
				}
			}
			out["reject_path"] = err == nil && herr == nil && rejected && len(effects) == 2
		}
	}

	// Timeout path: nobody signals; the durable deadline must win the race.
	dir = filepath.Join(runsRoot, "functional", "timeout")
	_ = os.RemoveAll(dir)
	_ = os.MkdirAll(dir, 0o755)
	p, err = startProc(b.runner, "-dir", dir, "-id", "wf1", "-day-ms", "200", "-approval-timeout-ms", "800")
	if err == nil {
		d := drive(p, "", func() {}, 30*time.Second)
		if d.exitCode == 0 {
			hist, herr := readJSONLines[histEvent](filepath.Join(dir, "wf1", "history.jsonl"))
			timedOut := false
			for _, ev := range hist {
				if ev.Type == "WorkflowCompleted" && strings.Contains(string(ev.Result), `"approval_timed_out"`) {
					timedOut = true
				}
			}
			out["approval_timeout"] = herr == nil && timedOut
		}
	}
	return out
}

// runDeterminism: start versioned workflow under V1, kill after its first
// activity, then replay under V2 (which inserts a step). The engine must
// park it with a NonDeterminismError and produce no further effects.
func runDeterminism(b bins, runsRoot string) map[string]any {
	dir := filepath.Join(runsRoot, "determinism")
	_ = os.RemoveAll(dir)
	_ = os.MkdirAll(dir, 0o755)
	out := map[string]any{"scenario": "start under code v1, kill -9 after step_a and step_b are recorded, restart under code v2 (step inserted at command 1)"}

	p1, err := startProc(b.runner, "-dir", dir, "-id", "wf1", "-workflow", "versioned", "-version", "v1", "-kill-at", "post-activity-1")
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	d1 := drive(p1, "post-activity-1", func() {}, 30*time.Second)
	if !d1.killed {
		out["error"] = "phase 1 never reached the killpoint"
		return out
	}
	p2, err := startProc(b.runner, "-dir", dir, "-id", "wf1", "-workflow", "versioned", "-version", "v2")
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	d2 := drive(p2, "", func() {}, 30*time.Second)
	hist, herr := readJSONLines[histEvent](filepath.Join(dir, "wf1", "history.jsonl"))
	parked, reason := false, ""
	if herr == nil {
		for _, ev := range hist {
			if ev.Type == "WorkflowParked" {
				parked, reason = true, ev.Reason
			}
		}
	}
	effects, _ := readJSONLines[effectLine](filepath.Join(dir, "effects.jsonl"))
	out["exit_code_v2"] = d2.exitCode
	out["detected"] = parked && strings.Contains(reason, "non-determinism")
	out["parked"] = parked
	out["reason"] = reason
	out["effects_after_divergence_attempt"] = len(effects) // must remain 2 (step_a + step_b from v1; the inserted step never ran)
	return out
}

// ---------------------------------------------------------------------------

type report struct {
	GeneratedAt string          `json:"generated_at"`
	Workflow    string          `json:"demo_workflow"`
	Engine      suiteResult     `json:"durawork"`
	Naive       suiteResult     `json:"naive_baseline"`
	MidActivity map[string]any  `json:"mid_activity_crashes"`
	Functional  map[string]bool `json:"functional_scenarios"`
	Determinism map[string]any  `json:"determinism_contract"`
}

func midActivityStats(s suiteResult) (tested, passed int) {
	for _, r := range s.Results {
		if strings.HasPrefix(r.Boundary, "mid-activity-") {
			tested++
			if r.Pass {
				passed++
			}
		}
	}
	return
}

func main() {
	root := flag.String("root", ".", "module root")
	runsRoot := flag.String("runs", "", "scenario output root (default <root>/runs/harness)")
	outPath := flag.String("out", "", "results.json path (default <root>/results.json)")
	flag.Parse()
	if *runsRoot == "" {
		*runsRoot = filepath.Join(*root, "runs", "harness")
	}
	if *outPath == "" {
		*outPath = filepath.Join(*root, "results.json")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		goBin = filepath.Join(os.Getenv("HOME"), "workspace", "tools", "go", "bin", "go")
	}
	binDir := filepath.Join(*runsRoot, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	b := bins{
		runner: filepath.Join(binDir, "durawork-runner"),
		signal: filepath.Join(binDir, "durawork-signal"),
		naive:  filepath.Join(binDir, "durawork-naive"),
	}
	for name, pkg := range map[string]string{
		b.runner: "./cmd/durawork-runner", b.signal: "./cmd/durawork-signal", b.naive: "./cmd/durawork-naive",
	} {
		cmd := exec.Command(goBin, "build", "-o", name, pkg)
		cmd.Dir = *root
		cmd.Env = os.Environ()
		if out, err := cmd.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n%s\n", pkg, err, out)
			os.Exit(1)
		}
	}
	fmt.Println("== durawork engine: boundary-exhaustive crash proof ==")
	engineSuite := runSuite("durawork", engineBoundaries, b, *runsRoot)
	fmt.Println("== naive baseline: same harness ==")
	naiveSuite := runSuite("naive", naiveBoundaries, b, *runsRoot)
	fmt.Println("== functional scenarios ==")
	functional := runFunctional(b, *runsRoot)
	for k, v := range functional {
		fmt.Printf("[functional] %-18s %v\n", k, v)
	}
	fmt.Println("== determinism contract ==")
	determinism := runDeterminism(b, *runsRoot)
	fmt.Printf("[determinism] detected=%v parked=%v reason=%v\n", determinism["detected"], determinism["parked"], determinism["reason"])

	eTested, ePassed := midActivityStats(engineSuite)
	nTested, nPassed := midActivityStats(naiveSuite)
	rep := report{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Workflow:    "onboarding (approve path): create account -> welcome email -> approval signal -> provision -> day-2 check-in -> day-3 check-in",
		Engine:      engineSuite,
		Naive:       naiveSuite,
		MidActivity: map[string]any{
			"durawork":       map[string]int{"tested": eTested, "passed": ePassed},
			"naive_baseline": map[string]int{"tested": nTested, "passed": nPassed},
		},
		Functional:  functional,
		Determinism: determinism,
	}
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(*outPath, data, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("\ndurawork: %d/%d boundaries passed\n", engineSuite.BoundariesPassed, engineSuite.BoundariesTested)
	fmt.Printf("naive:    %d/%d boundaries passed\n", naiveSuite.BoundariesPassed, naiveSuite.BoundariesTested)
	fmt.Printf("results written to %s\n", *outPath)
	engineOK := engineSuite.BoundariesPassed == engineSuite.BoundariesTested &&
		functional["reject_path"] && functional["approval_timeout"] &&
		determinism["detected"] == true
	if !engineOK {
		fmt.Fprintln(os.Stderr, "durawork failed its own proof (see results.json)")
		os.Exit(1)
	}
}
