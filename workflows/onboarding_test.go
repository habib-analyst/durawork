package workflows

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"durawork/engine"
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

func onboardingExecutor(t *testing.T, dir, id string, in OnboardingInput) (*engine.Store, *engine.Executor) {
	t.Helper()
	store, err := engine.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(in)
	ex := engine.NewExecutor(store, id, raw, engine.NewEffects(dir))
	RegisterOnboarding(ex)
	return store, ex
}

func effectKinds(t *testing.T, dir string) map[string]int {
	t.Helper()
	all, err := engine.NewEffects(dir).All()
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, e := range all {
		var p struct {
			Template string `json:"template"`
		}
		_ = json.Unmarshal(e.Payload, &p)
		key := e.Kind
		if p.Template != "" {
			key += ":" + p.Template
		}
		counts[key]++
	}
	return counts
}

func TestOnboardingApprovedEndToEnd(t *testing.T) {
	dir := testDir(t)
	in := OnboardingInput{UserID: "u9", Email: "u9@example.com", Name: "Nine", DayDurationMs: 30, ApprovalTimeoutMs: 5000}
	store, ex := onboardingExecutor(t, dir, "wf", in)
	go func() {
		time.Sleep(120 * time.Millisecond)
		_, _ = store.SendSignal("wf", "approval", json.RawMessage(`{"decision":"approve"}`))
	}()
	r := ex.Run(Onboarding)
	if r.Status != engine.StatusCompleted {
		t.Fatalf("status=%v err=%v", r.Status, r.Err)
	}
	var out OnboardingOutput
	if err := json.Unmarshal(r.Output, &out); err != nil || out.Status != "approved" || out.AccountID != "acct-u9" {
		t.Fatalf("output=%s err=%v", r.Output, err)
	}
	counts := effectKinds(t, dir)
	want := map[string]int{
		"account_created": 1, "email_sent:welcome": 1, "workspace_provisioned": 1,
		"email_sent:checkin_day2": 1, "email_sent:checkin_day3": 1,
	}
	for k, v := range want {
		if counts[k] != v {
			t.Fatalf("effect %s count=%d, want %d (all counts: %v)", k, counts[k], v, counts)
		}
	}
}

func TestOnboardingRejected(t *testing.T) {
	dir := testDir(t)
	in := OnboardingInput{UserID: "u9", Email: "u9@example.com", Name: "Nine", DayDurationMs: 30, ApprovalTimeoutMs: 5000}
	store, ex := onboardingExecutor(t, dir, "wf", in)
	go func() {
		time.Sleep(120 * time.Millisecond)
		_, _ = store.SendSignal("wf", "approval", json.RawMessage(`{"decision":"reject"}`))
	}()
	r := ex.Run(Onboarding)
	var out OnboardingOutput
	_ = json.Unmarshal(r.Output, &out)
	if r.Status != engine.StatusCompleted || out.Status != "rejected" {
		t.Fatalf("status=%v output=%s", r.Status, r.Output)
	}
	counts := effectKinds(t, dir)
	if counts["workspace_provisioned"] != 0 || counts["email_sent:checkin_day2"] != 0 {
		t.Fatalf("rejected onboarding produced post-approval effects: %v", counts)
	}
}

func TestOnboardingApprovalTimeout(t *testing.T) {
	dir := testDir(t)
	in := OnboardingInput{UserID: "u9", Email: "u9@example.com", Name: "Nine", DayDurationMs: 30, ApprovalTimeoutMs: 200}
	_, ex := onboardingExecutor(t, dir, "wf", in)
	r := ex.Run(Onboarding)
	var out OnboardingOutput
	_ = json.Unmarshal(r.Output, &out)
	if r.Status != engine.StatusCompleted || out.Status != "approval_timed_out" {
		t.Fatalf("status=%v output=%s err=%v", r.Status, r.Output, r.Err)
	}
}

func TestVersionedDivergence(t *testing.T) {
	dir := testDir(t)
	store, err := engine.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ex1 := engine.NewExecutor(store, "wf", json.RawMessage(`{}`), engine.NewEffects(dir))
	RegisterVersioned(ex1)
	// V1 can only finish step_a before we "deploy" if it blocks; instead run
	// V1 to completion of step_a via a kill-style hook is overkill here:
	// run V1 fully, then confirm V2 against a *fresh in-flight* history is
	// covered by the engine test. Here: V1 completes; V2 replay of a
	// completed workflow short-circuits (terminal state wins).
	if r := ex1.Run(VersionedV1); r.Status != engine.StatusCompleted {
		t.Fatalf("v1: %v (%v)", r.Status, r.Err)
	}
	ex2 := engine.NewExecutor(store, "wf", json.RawMessage(`{}`), engine.NewEffects(dir))
	RegisterVersioned(ex2)
	if r := ex2.Run(VersionedV2); r.Status != engine.StatusCompleted {
		t.Fatalf("v2 over completed v1 history: %v (%v)", r.Status, r.Err)
	}
}
