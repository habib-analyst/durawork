// Command durawork-runner drives one workflow instance to completion (or to
// a wait point). The crash harness starts it with -kill-at <label> to stop it
// dead at an exact step boundary, SIGKILLs it, then restarts it without
// -kill-at and expects exactly-once completion.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"durawork/engine"
	"durawork/workflows"
)

func main() {
	dir := flag.String("dir", "", "data directory")
	id := flag.String("id", "", "workflow id")
	workflow := flag.String("workflow", "onboarding", "workflow name: onboarding | versioned")
	version := flag.String("version", "v1", "workflow code version (versioned workflow only)")
	killAt := flag.String("kill-at", "", "stop and block at this hook label (harness use)")
	inputJSON := flag.String("input", "", "workflow input JSON (onboarding)")
	dayMs := flag.Int64("day-ms", 400, "compressed day length for the onboarding input")
	approvalTimeoutMs := flag.Int64("approval-timeout-ms", 5000, "approval timeout for the onboarding input")
	flag.Parse()

	if *dir == "" || *id == "" {
		fmt.Fprintln(os.Stderr, "usage: durawork-runner -dir D -id ID [-workflow W] [-input JSON]")
		os.Exit(2)
	}
	store, err := engine.NewStore(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	effects := engine.NewEffects(*dir)

	var input json.RawMessage
	var wf engine.WorkflowFunc
	var ex *engine.Executor
	switch *workflow {
	case "onboarding":
		if *inputJSON != "" {
			input = json.RawMessage(*inputJSON)
		} else {
			input = json.RawMessage(fmt.Sprintf(
				`{"user_id":"u1","email":"u1@example.com","name":"User One","day_duration_ms":%d,"approval_timeout_ms":%d}`,
				*dayMs, *approvalTimeoutMs))
		}
		ex = engine.NewExecutor(store, *id, input, effects)
		workflows.RegisterOnboarding(ex)
		wf = workflows.Onboarding
	case "versioned":
		input = json.RawMessage(`{}`)
		ex = engine.NewExecutor(store, *id, input, effects)
		workflows.RegisterVersioned(ex)
		if *version == "v2" {
			wf = workflows.VersionedV2
		} else {
			wf = workflows.VersionedV1
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown workflow:", *workflow)
		os.Exit(2)
	}

	if *killAt != "" {
		target := *killAt
		ex.SetHook(func(label string) {
			if label == target {
				fmt.Printf("KILLPOINT %s\n", label)
				select {} // block until the harness SIGKILLs us
			}
		})
	}
	ex.SetOnWait(func(r engine.RunResult) {
		fmt.Printf("WAITING reason=%s name=%s until=%d\n", r.WaitReason, r.WaitName, r.WaitUntilUnixMs)
	})

	res := ex.Run(wf)
	switch res.Status {
	case engine.StatusCompleted:
		fmt.Printf("STATUS completed output=%s\n", string(res.Output))
		os.Exit(0)
	case engine.StatusWaiting:
		fmt.Printf("STATUS waiting reason=%s name=%s until=%d\n", res.WaitReason, res.WaitName, res.WaitUntilUnixMs)
		os.Exit(3)
	case engine.StatusParked:
		fmt.Printf("STATUS parked error=%v\n", res.Err)
		os.Exit(4)
	case engine.StatusFailed:
		fmt.Printf("STATUS failed error=%v\n", res.Err)
		os.Exit(5)
	}
}
