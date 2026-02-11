# Titan Refactor Plan

## Goals
- Improve maintainability of the SQL core without behavior regressions.
- Split monolithic modules (`executor`, then `optimizer`) into isolated parts.
- Reduce coupling and implicit contracts between subsystems.

## Out of Scope for This Cycle
- Full storage/locking redesign.
- New SQL features before core stabilization.
- Aggressive performance tuning before decomposition is complete.

## Phase 0: Change Control
- [ ] Keep refactors in small commits without behavior changes.
- [ ] After each step: formatting, local checks, short changelog note.

## Phase 1: Baseline and Guardrails
- [x] Define mandatory checks with timeouts.
- [ ] Define target smoke scenarios (parser, planner, executor, WAL).
- [ ] Add a minimal PR/commit checklist for refactor work.

## Phase 2: Executor Decomposition (Priority)
- [x] Move utility helper functions/types into `executor/*`.
- [x] Split executors by domain: `scan`, `join`, `dml`, `ddl`, `eval`.
- [x] Keep a stable public facade at `executor::execute`.
- [ ] Reduce long parameter lists via local contexts (no logic changes).

## Phase 3: Optimizer Decomposition
- [ ] Split cost model, selectivity, join-order, and physical conversion.
- [ ] Remove duplicated cardinality/selectivity logic.
- [ ] Add explicit plan-stability tests.

## Phase 4: Reliability and Quality
- [ ] Reduce `unwrap` usage in production code.
- [ ] Unify planning/execution error flow.
- [ ] Tighten `clippy` policy for core modules.

## Current Status
- [x] Plan documented.
- [x] Phase 1 started with baseline artifacts.
- [x] Phase 2 started with the first safe extraction from `executor.rs`.
- [x] `executor` decomposition completed across `scan/join/pipeline/ddl/dml/eval/maintenance`.

## Next Execution Order
1. Reduce long parameter lists in hot paths with lightweight context structs.
2. Split `optimizer` helper layers (stats/selectivity/cost/util) into submodules.
3. Add smoke scenarios and PR checklist to complete Phase 1 guardrails.
