package workflows

import (
	"context"
	"encoding/json"

	"durawork/engine"
)

// The versioned workflows demonstrate the determinism contract the way it
// breaks in production: a deploy changes workflow code while executions are
// in flight. V1 runs step_a then step_b. V2 inserts step_inserted between
// them. An execution started under V1 and replayed under V2 issues a
// different command at index 1 than its history recorded, and the engine
// parks it with a NonDeterminismError instead of silently misaligning every
// subsequent recorded result.

func RegisterVersioned(ex *engine.Executor) {
	ex.RegisterActivity("step_a", markerActivity("a"))
	ex.RegisterActivity("step_b", markerActivity("b"))
	ex.RegisterActivity("step_inserted", markerActivity("inserted"))
}

func markerActivity(tag string) engine.ActivityFunc {
	return func(_ context.Context, _ json.RawMessage, meta engine.ActivityMeta) (json.RawMessage, error) {
		_, _, err := meta.Effects.Do(meta.IdempotencyKey, "step_done", map[string]any{"step": tag})
		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{"step": tag})
	}
}

// VersionedV1: step_a -> step_b.
func VersionedV1(ctx *engine.Context) (any, error) {
	if _, err := ctx.ExecuteActivity("step_a", nil, engine.ActivityOptions{}); err != nil {
		return nil, err
	}
	if _, err := ctx.ExecuteActivity("step_b", nil, engine.ActivityOptions{}); err != nil {
		return nil, err
	}
	return map[string]any{"version": "v1", "done": true}, nil
}

// VersionedV2: step_a -> step_inserted -> step_b. Diverges from V1 at command 1.
func VersionedV2(ctx *engine.Context) (any, error) {
	if _, err := ctx.ExecuteActivity("step_a", nil, engine.ActivityOptions{}); err != nil {
		return nil, err
	}
	if _, err := ctx.ExecuteActivity("step_inserted", nil, engine.ActivityOptions{}); err != nil {
		return nil, err
	}
	if _, err := ctx.ExecuteActivity("step_b", nil, engine.ActivityOptions{}); err != nil {
		return nil, err
	}
	return map[string]any{"version": "v2", "done": true}, nil
}
