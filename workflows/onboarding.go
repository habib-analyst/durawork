// Package workflows holds the demo workflow definitions and their activities.
package workflows

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"durawork/engine"
)

// OnboardingInput parameterises the demo workflow. Durations are parameters
// (not constants) so the crash harness can compress the "3 days" into
// milliseconds while a human run can use real ones.
type OnboardingInput struct {
	UserID            string `json:"user_id"`
	Email             string `json:"email"`
	Name              string `json:"name"`
	DayDurationMs     int64  `json:"day_duration_ms"`
	ApprovalTimeoutMs int64  `json:"approval_timeout_ms"`
}

// OnboardingOutput is the workflow's final state.
type OnboardingOutput struct {
	Status    string `json:"status"` // approved | rejected | approval_timed_out
	AccountID string `json:"account_id"`
}

// RegisterOnboarding registers the onboarding workflow's activities.
func RegisterOnboarding(ex *engine.Executor) {
	ex.RegisterActivity("create_account", createAccount)
	ex.RegisterActivity("send_email", sendEmail)
	ex.RegisterActivity("provision_workspace", provisionWorkspace)
}

// Onboarding is the demo workflow: a 3-day user onboarding.
//
//	create account -> welcome email -> human approval (raced vs timeout)
//	-> provision workspace -> day-2 check-in -> day-3 check-in -> complete
//
// Every externally visible effect goes through an activity that journals it
// under the command's idempotency key, so any crash/replay interleaving
// still produces each effect exactly once.
func Onboarding(ctx *engine.Context) (any, error) {
	var in OnboardingInput
	if err := json.Unmarshal(ctx.Input, &in); err != nil {
		return nil, fmt.Errorf("bad input: %w", err)
	}
	day := milliseconds(in.DayDurationMs)
	approvalTimeout := milliseconds(in.ApprovalTimeoutMs)

	res, err := ctx.ExecuteActivity("create_account", map[string]any{
		"user_id": in.UserID, "email": in.Email, "name": in.Name,
	}, engine.ActivityOptions{})
	if err != nil {
		return nil, err
	}
	var acct struct {
		AccountID string `json:"account_id"`
	}
	if err := json.Unmarshal(res, &acct); err != nil {
		return nil, err
	}

	if _, err := ctx.ExecuteActivity("send_email", map[string]any{
		"to": in.Email, "template": "welcome", "user_id": in.UserID,
	}, engine.ActivityOptions{}); err != nil {
		return nil, err
	}

	sig, err := ctx.WaitSignal("approval", approvalTimeout)
	if err != nil {
		return nil, err
	}
	if sig.TimedOut {
		return OnboardingOutput{Status: "approval_timed_out", AccountID: acct.AccountID}, nil
	}
	var decision struct {
		Decision string `json:"decision"`
	}
	if err := json.Unmarshal(sig.Payload, &decision); err != nil {
		return nil, fmt.Errorf("bad approval payload: %w", err)
	}
	if decision.Decision != "approve" {
		return OnboardingOutput{Status: "rejected", AccountID: acct.AccountID}, nil
	}

	if _, err := ctx.ExecuteActivity("provision_workspace", map[string]any{
		"account_id": acct.AccountID, "user_id": in.UserID,
	}, engine.ActivityOptions{}); err != nil {
		return nil, err
	}

	if err := ctx.Sleep(day); err != nil {
		return nil, err
	}
	if _, err := ctx.ExecuteActivity("send_email", map[string]any{
		"to": in.Email, "template": "checkin_day2", "user_id": in.UserID,
	}, engine.ActivityOptions{}); err != nil {
		return nil, err
	}

	if err := ctx.Sleep(day); err != nil {
		return nil, err
	}
	if _, err := ctx.ExecuteActivity("send_email", map[string]any{
		"to": in.Email, "template": "checkin_day3", "user_id": in.UserID,
	}, engine.ActivityOptions{}); err != nil {
		return nil, err
	}

	return OnboardingOutput{Status: "approved", AccountID: acct.AccountID}, nil
}

func milliseconds(ms int64) time.Duration {
	if ms <= 0 {
		ms = 1000
	}
	return time.Duration(ms) * time.Millisecond
}

// --- activities -------------------------------------------------------------
// Each activity performs its effect through Effects.Do under the command's
// idempotency key. Re-execution after a crash finds the key already present
// and returns the recorded effect instead of duplicating it.

func createAccount(_ context.Context, input json.RawMessage, meta engine.ActivityMeta) (json.RawMessage, error) {
	var in struct {
		UserID string `json:"user_id"`
		Email  string `json:"email"`
		Name   string `json:"name"`
	}
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	accountID := "acct-" + in.UserID
	_, _, err := meta.Effects.Do(meta.IdempotencyKey, "account_created", map[string]any{
		"account_id": accountID, "user_id": in.UserID, "email": in.Email, "name": in.Name,
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"account_id": accountID})
}

func sendEmail(_ context.Context, input json.RawMessage, meta engine.ActivityMeta) (json.RawMessage, error) {
	var in struct {
		To       string `json:"to"`
		Template string `json:"template"`
		UserID   string `json:"user_id"`
	}
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	rec, already, err := meta.Effects.Do(meta.IdempotencyKey, "email_sent", map[string]any{
		"to": in.To, "template": in.Template, "user_id": in.UserID,
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"queued": true, "template": in.Template, "duplicate_suppressed": already, "at_unix_ms": rec.AtUnixMs,
	})
}

func provisionWorkspace(_ context.Context, input json.RawMessage, meta engine.ActivityMeta) (json.RawMessage, error) {
	var in struct {
		AccountID string `json:"account_id"`
		UserID    string `json:"user_id"`
	}
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	wsID := "ws-" + in.UserID
	_, _, err := meta.Effects.Do(meta.IdempotencyKey, "workspace_provisioned", map[string]any{
		"workspace_id": wsID, "account_id": in.AccountID, "user_id": in.UserID,
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"workspace_id": wsID})
}
