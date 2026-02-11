# Refactor PR Checklist

## Scope and safety
- [ ] Change is scoped to one refactor objective.
- [ ] Behavior is unchanged or clearly documented if intentionally changed.
- [ ] No unrelated formatting-only churn in touched files.

## Structure
- [ ] Module boundaries are improved (smaller files, clearer ownership).
- [ ] Public APIs remain stable or migrations are documented.
- [ ] New helper/context types replace long parameter lists when appropriate.

## Quality gates
- [ ] `cargo fmt --all -- --check` passes.
- [ ] Baseline script passes or failures are explicitly explained.
- [ ] Smoke scenarios in `docs/refactor_smoke.md` were reviewed/executed.

## Review metadata
- [ ] Commit message explains intent and no-behavior-change expectation.
- [ ] Risks and follow-up tasks are listed in PR description.