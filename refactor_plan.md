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
- [x] Keep refactors in small commits without behavior changes.
- [x] After each step: formatting, local checks, short changelog note.

## Phase 1: Baseline and Guardrails
- [x] Define mandatory checks with timeouts.
- [x] Define target smoke scenarios (parser, planner, executor, WAL).
- [x] Add a minimal PR/commit checklist for refactor work.

## Phase 2: Executor Decomposition (Priority)
- [x] Move utility helper functions/types into `executor/*`.
- [x] Split executors by domain: `scan`, `join`, `dml`, `ddl`, `eval`.
- [x] Keep a stable public facade at `executor::execute`.
- [x] Reduce long parameter lists via local contexts (no logic changes).

## Phase 3: Optimizer Decomposition
- [x] Split cost model, selectivity, join-order, and physical conversion.
- [x] Remove duplicated cardinality/selectivity logic.
- [x] Add explicit plan-stability tests.

## Phase 4: Reliability and Quality
- [x] Reduce lock-related `unwrap` usage in core refactor modules.
- [x] Unify planning/execution error flow.
- [x] Tighten `clippy` policy for core modules.

## Current Status
- [x] Plan documented.
- [x] Phase 1 started with baseline artifacts.
- [x] Phase 2 started with the first safe extraction from `executor.rs`.
- [x] `executor` decomposition completed across `scan/join/pipeline/ddl/dml/eval/maintenance`.
- [x] `optimizer` decomposition completed across `stats/model/join_order/convert`.
- [x] Refactor smoke scenarios and PR checklist documented.
- [x] Core refactor path converted away from lock-related `unwrap`.
- [x] Baseline checks include a dedicated `clippy-core` gate with timeout.

## Next Execution Order
1. Run full baseline checks in an environment with MSVC `link.exe`.
2. Prepare `0.2` release note from refactor checkpoints.
3. Continue global `unwrap` debt cleanup outside core refactor scope.
