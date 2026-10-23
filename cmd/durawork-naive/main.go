// Command durawork-naive is the honest baseline: the onboarding flow written
// the way people usually write it — a plain sequential script with in-memory
// state and a checkpoint file rewritten after each step. No history, no
// replay, no idempotency keys, no fsync, non-atomic checkpoint writes. The
// crash harness runs the same boundary kills against it and counts the
// damage, so the engine's value is measured rather than asserted.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var (
	dir        string
	killAt     string
	dayMs      int64
	timeoutMs  int64
	userID     string
	email      string
	naiveState state
)

type state struct {
	Done      []string `json:"done"`
	AccountID string   `json:"account_id"`
	Decision  string   `json:"decision"`
	Status    string   `json:"status"`
}

func hook(label string) {
	if label == killAt && killAt != "" {
		fmt.Printf("KILLPOINT %s\n", label)
		select {} // block until the harness SIGKILLs us
	}
}

func statePath() string { return filepath.Join(dir, "state.json") }

func load() {
	data, err := os.ReadFile(statePath())
	if err != nil {
		return // no checkpoint yet: fresh start
	}
	if err := json.Unmarshal(data, &naiveState); err != nil {
		// The classic forgiving bug: a torn checkpoint is silently treated
		// as "start over", re-running every step.
		fmt.Fprintln(os.Stderr, "warning: checkpoint unreadable, starting fresh")
		naiveState = state{}
	}
}

func done(step string) bool {
	for _, d := range naiveState.Done {
		if d == step {
			return true
		}
	}
	return false
}

// save rewrites the whole checkpoint file — non-atomically, in two chunks,
// with no fsync. A kill between the chunks leaves a torn file.
func save(stepIdx int) {
	naiveState.Done = append(naiveState.Done, stepName(stepIdx))
	data, _ := json.Marshal(naiveState)
	f, err := os.Create(statePath())
	if err != nil {
		fmt.Fprintln(os.Stderr, "checkpoint:", err)
		os.Exit(1)
	}
	half := len(data) / 2
	_, _ = f.Write(data[:half])
	hook(fmt.Sprintf("ckptwrite-%d", stepIdx))
	_, _ = f.Write(data[half:])
	_ = f.Close()
}

func stepName(i int) string {
	return []string{"create_account", "welcome_email", "approval", "provision",
		"sleep_day2", "checkin_day2", "sleep_day3", "checkin_day3"}[i]
}

// effect appends to the outbox with no dedup of any kind.
func effect(kind string, payload map[string]any) {
	payload["kind"] = kind
	line, _ := json.Marshal(payload)
	f, err := os.OpenFile(filepath.Join(dir, "effects.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "effect:", err)
		os.Exit(1)
	}
	_, _ = f.Write(append(line, '\n'))
	_ = f.Close()
}

func effectStep(idx int, kind string, payload map[string]any) {
	if done(stepName(idx)) {
		return
	}
	hook(fmt.Sprintf("pre-activity-%d", idx))
	effect(kind, payload)
	hook(fmt.Sprintf("mid-activity-%d", idx))
	save(idx)
	hook(fmt.Sprintf("post-activity-%d", idx))
}

func finish(status string) {
	out, _ := json.Marshal(map[string]any{"status": status, "account_id": naiveState.AccountID})
	_ = os.WriteFile(filepath.Join(dir, "result.json"), out, 0o644)
	fmt.Printf("STATUS completed output=%s\n", string(out))
	os.Exit(0)
}

func main() {
	flag.StringVar(&dir, "dir", "", "data directory")
	flag.StringVar(&killAt, "kill-at", "", "stop and block at this hook label (harness use)")
	flag.Int64Var(&dayMs, "day-ms", 400, "compressed day length")
	flag.Int64Var(&timeoutMs, "approval-timeout-ms", 5000, "approval timeout")
	flag.StringVar(&userID, "user", "u1", "user id")
	flag.StringVar(&email, "email", "u1@example.com", "user email")
	sigName := flag.String("signal", "", "deliver a signal instead of running (only 'approval' is used)")
	sigPayload := flag.String("payload", "{}", "signal payload JSON")
	flag.Parse()

	if dir == "" {
		fmt.Fprintln(os.Stderr, "usage: durawork-naive -dir D")
		os.Exit(2)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if *sigName != "" {
		if err := os.WriteFile(filepath.Join(dir, "naive-signal.json"), []byte(*sigPayload), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("SIGNAL SENT name=%s\n", *sigName)
		return
	}

	load()
	hook("wf-start")

	// 0: create account
	if !done("create_account") {
		naiveState.AccountID = "acct-" + userID
	}
	effectStep(0, "account_created", map[string]any{"account_id": "acct-" + userID, "user_id": userID, "email": email})

	// 1: welcome email
	effectStep(1, "email_sent", map[string]any{"to": email, "template": "welcome", "user_id": userID})

	// 2: approval (polls a signal file; timeout races it)
	if !done("approval") {
		hook("pre-signal-2")
		fmt.Println("WAITING reason=signal name=approval")
		deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
		decision := ""
		for {
			data, err := os.ReadFile(filepath.Join(dir, "naive-signal.json"))
			if err == nil {
				var p struct {
					Decision string `json:"decision"`
				}
				if json.Unmarshal(data, &p) == nil {
					decision = p.Decision
				}
				break
			}
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		naiveState.Decision = decision
		save(2)
		hook("post-signal-2")
	}
	if naiveState.Decision == "" {
		finish("approval_timed_out")
	}
	if naiveState.Decision != "approve" {
		finish("rejected")
	}

	// 3: provision workspace
	effectStep(3, "workspace_provisioned", map[string]any{"workspace_id": "ws-" + userID, "account_id": naiveState.AccountID, "user_id": userID})

	// 4+5: day-2 sleep, then check-in email
	if !done("sleep_day2") {
		hook("pre-timer-4")
		time.Sleep(time.Duration(dayMs) * time.Millisecond)
		hook("post-timer-4")
		save(4)
	}
	effectStep(5, "email_sent", map[string]any{"to": email, "template": "checkin_day2", "user_id": userID})

	// 6+7: day-3 sleep, then check-in email
	if !done("sleep_day3") {
		hook("pre-timer-6")
		time.Sleep(time.Duration(dayMs) * time.Millisecond)
		hook("post-timer-6")
		save(6)
	}
	effectStep(7, "email_sent", map[string]any{"to": email, "template": "checkin_day3", "user_id": userID})

	hook("pre-complete")
	finish("approved")
}
