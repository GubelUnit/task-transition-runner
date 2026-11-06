# Development model

Human governance, agent implementation, and deterministic checks are three different layers. None of them is a product capability of the runner.

## Human governance

The Human Operator holds intent, goals, the public and private boundary, constraints, scope, governance, trust choices, and release authority, including the decision to stop or continue. The Human Operator is not described here as the line-by-line implementation programmer and not as the line-by-line reviewer of every technical change.

This is not zero human involvement.

## Agent implementation

Implementation and technical review were performed primarily by AI agents. Depending on the work, agents planned, implemented, constructed and executed tests, reviewed code, analyzed failures, and prepared evidence. That work stayed inside an explicit task scope, allowed paths, validation requirements, and deterministic tests and Git evidence.

Agent confidence is not durable truth. "AI authored, therefore better" is not a claim. "Fully autonomous" is not a claim.

## Deterministic evidence

Tests, schema checks, identity checks, Git commits, state transitions, and validation commands are the mechanical evidence. They do not prove universal semantic correctness.

Prompts are instructions, not proof. The public reference prompts for developer, orchestrator, and validator are operating examples. They are not authority to promote.

The reference runner is sequential: one active task on one local target. That boundary is not a statement about other systems.

## Provenance

Implementation and technical review were performed primarily by AI agents. The Human Operator carried intent, goals, governance, boundary decisions, and release authority, and did not act as the line-by-line implementation programmer.

The only bound historical statement is the human role above. It is marked `NOT_PRODUCT_PROOF`. It is not zero human involvement, not fully autonomous operation, not a claim that AI authorship makes the product better, and not line-by-line human authorship or line-by-line human review. No numeric historical metric is bound. Rounded or remembered production figures are omitted.

A future public Git history is not evidence of private chronological origin. Historical statements in this section are not product proof.

## Projections

`AGENT_GUIDE.yaml` repeats the technical install, command, authority, and limit facts from `README.md`, `INSTALL.md`, `USAGE.md`, `REFERENCE_WORKFLOW.md`, and `KNOWN_LIMITS.md`. It adds no command and no capability.

`PROJECT_BACKGROUND.yaml` repeats this provenance section and the background section of `README.md`. It is not an install input, not a config file, and not required to run `init`, `run`, or `status`. Both projections are `NON_EXECUTION_RELEVANT` for the runner.

<!--
Traceability: [[claim:TRANSITION_CONTROL_LAYER]] [[claim:CANDIDATE_COVERAGE_BOUNDARY]] [[claim:WORKER_NONAUTHORITY]] [[claim:PARALLEL_TASKS]]
```oss8-authoritative
commands: []
qualifications:
  - NOT_PRODUCT_PROOF
  - HISTORICAL_HUMAN_ROLE
  - implementation_and_technical_review_primarily_by_ai_agents
  - human_operator_holds_intent_goals_governance_boundary_decisions_and_release_authority
  - human_operator_is_not_the_line_by_line_implementation_programmer
  - no_zero_human_involvement
  - no_fully_autonomous
  - no_ai_therefore_better
  - no_unproven_line_by_line_human_authorship
  - no_unproven_line_by_line_human_review
  - no_numeric_historical_kpi_bound
capabilities: []
```
-->
