# Usage

The public CLI is exactly three commands: `init`, `run`, and `status`.

There is no console script. Pass the command to `task_transition_runner.cli.main`. From the directory that contains the config file:

```bash
PYTHONDONTWRITEBYTECODE=1 python -c 'import sys
from task_transition_runner.cli import main
raise SystemExit(main(sys.argv[1:]))' init --config runner.yaml
```

Replace `init` with `run` or `status`. `status` accepts an optional `--json` flag. The other two commands do not. With `poetry run`, keep `PYTHONDONTWRITEBYTECODE=1` and prefix the same `python` command.

Every public runner invocation sets `PYTHONDONTWRITEBYTECODE=1`. Declared validation inherits that environment. The pytest flag `-p no:cacheprovider` only suppresses `.pytest_cache`. It does not replace this bytecode setting.

`--config` is required. The reference config uses paths relative to the directory of `runner.yaml`: `target`, `runtime`, and `tasks.yaml`.

Stdout is YAML. `status --json` writes one JSON document. Exit 0 means the payload field `ok` is true. Exit 1 means `ok` is false.

## Config

The reference file `examples/reference/runner.yaml` is the shape the loader accepts. Required groups are `schema_version` `1.0`, `target.repo_root`, `target.main_branch`, `runtime_root`, `task_source`, `baseline_validation.command`, `baseline_validation.timeout_seconds`, `adapter.kind`, `adapter.executable`, `adapter.timeout_seconds`, and prompt paths for `developer`, `orchestrator`, and `validator`.

The reference adapter kind is `cursor`. The reference executable is `cursor-agent`. The reference adapter timeout is 1800 seconds. The reference baseline command is `true`, with a 30 second timeout. That baseline check does not authorize task promotion.

`adapter.model` may be omitted. Unknown fields are rejected.

## init

`init` prepares an empty runtime for an existing local Git target.

Preconditions that the command enforces include: the target is a Git repository, the configured main branch is checked out, the work tree is clean, baseline validation exits 0, and the runner source authority checks pass. The reference main branch is `main`.

A successful payload has `ok: true`, `command: init`, `outcome: INITIALIZED`, and `reason_code: BOOTSTRAP_OK`. The payload lists `main_state.yaml` and `promotion_consumed.yaml` under `artifacts_written`. The same bootstrap also writes `runner_source_pin.yaml` into the runtime. It does not implement the active task.

Run `init` once on a fresh runtime. A second init is not part of the reference path. `init` does not migrate an existing runtime schema and does not import historical private state.

## run

`run` continues one local target until the next explicit boundary. It verifies source authority, checks the target, and holds one local lock. It does not plan the next task and does not run two tasks at once.

The runner starts the developer through the Cursor adapter. It does not start the orchestrator or the validator. Those two roles are external decision files. A worker success report cannot authorize promotion.

### Expected pause

The first productive `run` of a task seals validation, then stops:

```text
ok: false
command: run
outcome: AWAITING_DECISIONS
reason_code: PROMOTION_DECISION_MISSING
```

Exit code 1 is that expected pause, not a crashed process. The runner is waiting for decision files.

### Decision files

The pause writes a decision request at `runtime/runs/<origin_run_id>/decisions/decision_request.yaml`. Read the four identity values from that file: `task_id`, `origin_run_id`, `promotion_attempt_id`, and `result_sha`. Write the two files named by the request. Their contract names are `orchestrator_decision.yaml` and `validator_decision.yaml`, in that same `decisions` directory. Both decisions must bind those four values and no earlier task's values.

Closed fields:

- Both files use `schema_version: "1.0"`, `ready`, the four identity axes, and `evidence`.
- The orchestrator file uses `decision_type: orchestrator` and `promotion_class`. The accepted classes are `GREEN_PROMOTION`, `RED_TDD_PROMOTION`, and `BLOCKED`.
- The validator file uses `decision_type: validator` and `verdict`. The accepted verdicts are `GO` and `NO_GO`.
- `rationale` is the only optional field. Other extra fields are rejected.
- A ready `GREEN_PROMOTION` or `RED_TDD_PROMOTION` needs non-empty evidence.
- A `validation_log` evidence item has `kind`, a relative `ref`, and `sha256`. The digest must match the file bytes under the artifact root.
- A `git_result` evidence item has `kind` and `ref`. The `ref` must be exactly `result_sha`.

This page lists the closed contract. It does not supply a sample decision. Separate decision files prove separate contract roles and result binding. They do not prove cryptographic, organizational, personal, or process independence of the people or programs that wrote them.

### Later run results

After both decisions are in place, the next `run` can return:

```text
ok: true
outcome: PENDING_PROMOTION
reason_code: PROMOTION_INTENT_PERSISTED
```

The following `run` admits the already sealed validation. Do not start the task validation commands by hand between those two invocations. Admission re-reads the sealed logs. A manual rerun is a different run, not this admission.

That admission run can return:

```text
ok: true
outcome: DRAINED_AWAITING_TASK
reason_code: PROMOTION_EFFECT_ADMITTED
```

For the successor task, the same outer sequence can end as:

```text
ok: true
outcome: SETTLED
reason_code: CONTINUATION_CONSUMED
```

Task A of the reference path ends at `DRAINED_AWAITING_TASK`. It is not `SETTLED`. Task B is a new task id and ends `SETTLED`.

### Other supported boundaries

These windows are supported. They are not universal recovery.

- Direct GREEN does not require a prior expected-RED task. Expected RED is optional. When it is declared, the bound failure text must match as an exact substring. An unrelated failure does not satisfy it. A later GREEN closes that exact obligation.
- A second `run` process can resume the same task after a verified result and before the final decisions, without starting the worker again. It does not resume an incomplete Cursor worker.
- A second `run` process can verify a Git promotion that already exists and finish bookkeeping instead of repeating it.
- After a settled predecessor, a fresh ordinary task may start under a new `task_id`. That is not historical reopen.
- `RECOVERED` with `RECONCILIATION_COMPLETED` is the post-effect window above. It is not recovery from every crash.

Changing the runner source on an already managed target stays blocked. `status` on a source mismatch emits only a non-interpreting authority error.

## status

`status` is read-only. It does not mutate Git or the runtime. A projected view has `ok: true`, `command: status`, and `outcome: STATUS_VIEW_PROJECTED`. A failed view has `outcome: BLOCKED` and exit 1.

The view reports source identity, target head, main state, active task identity when one exists, run phase, pending, journal, consumption, drain, decisions, the next external action, and drift. On source mismatch it does not present runtime state as an ordinary status view.

## Terminal rest

After the predecessor is settled, a task source whose only active value is `active_task: null` is the end. A later `run` can return `outcome: TERMINAL_REST`. It writes no successor. Absence of a successor is not treated as interrupted recovery.

## Fail closed

Unknown command text returns `outcome: BLOCKED` and `reason_code: CLI_COMMAND_UNKNOWN`. A missing `--config` returns `CLI_CONFIG_REQUIRED`. Config and state failures return `BLOCKED` with a reason code. The runner does not guess across a missing decision, does not invent a task, and does not continue a Cursor worker that was lost mid-stream.

Do not repair a failed reference root in place and do not run a second init there. Start from a fresh target and a fresh runtime. That limitation is not arbitrary rollback.

<!--
Traceability: [[claim:FOURTH_CLI_COMMAND]] [[claim:CURSOR_ADAPTER]] [[claim:RUNTIME_MIGRATION]] [[claim:HISTORICAL_REOPEN]] [[claim:BUILTIN_PLANNING]] [[claim:PARALLEL_TASKS]] [[claim:TRANSITION_CONTROL_LAYER]] [[claim:WORKER_NONAUTHORITY]] [[claim:PRODUCER_INDEPENDENCE]] [[claim:DIRECT_GREEN]] [[claim:SUCCESSOR_AFTER_RECOVERY]] [[claim:UNIVERSAL_RECOVERY]] [[claim:ARBITRARY_ROLLBACK]] [[claim:EXPECTED_RED_CLOSURE]] [[claim:PRE_AUTHORIZATION_RESTART]] [[claim:POST_EFFECT_RECONCILIATION]] [[claim:CODE_AUTHORITY]] [[claim:RUNNER_SELF_UPGRADE]] [[claim:TERMINAL_REST]]
```oss8-authoritative
commands:
  - init
  - run
  - status
qualifications:
  - mechanical_admission_is_not_universal_semantic_correctness
  - limited_to_the_documented_reference_path_and_contract_proof
  - expected_red_is_optional
  - expected_red_matches_an_exact_substring
  - direct_green_does_not_require_a_prior_expected_red
  - worker_success_cannot_authorize_promotion
  - separate_decision_files_do_not_prove_producer_independence
  - pre_authorization_restart_is_a_second_cli_process_after_a_durable_result_and_before_final_decisions
  - an_incomplete_cursor_worker_is_not_resumed
  - post_effect_reconciliation_is_a_second_cli_process_after_a_durable_promotion_and_before_final_bookkeeping
  - successor_is_a_new_ordinary_task_with_a_fresh_identity
  - no_historical_reopen
  - no_universal_recovery
  - no_runner_self_upgrade
  - no_runtime_schema_migration
  - status_on_source_mismatch_is_a_noninterpreting_authority_error
  - reproducibility_is_the_reference_path_plus_the_contract_suite_exit_0
  - reproducibility_is_bound_to_the_exact_release_source_snapshot
  - no_identical_model_outputs
  - no_identical_diffs
  - no_identical_result_shas
  - no_provider_agnosticism
  - no_arbitrary_replacement_prompts
  - cursor_is_the_only_evidenced_adapter
  - ui_bridge_is_outside_the_headless_proof
  - terminal_rest_writes_no_successor
  - coverage_is_candidate_scope_evidence_not_product_law
  - public_cli_is_exactly_init_run_and_status
  - no_fourth_command
  - no_parallel_tasks
  - at_most_one_active_task
  - local_sequential_one_target_and_one_main_branch
  - no_pypi
  - no_distributed_coordination
  - no_remote_push_or_pull_request_orchestration
  - no_builtin_sandbox
  - allowed_paths_is_a_promotion_gate_not_host_protection
  - no_arbitrary_rollback
  - no_exactly_once
  - promotion_is_not_a_transaction
  - no_builtin_planning
  - export_seal_and_license_are_not_runner_capabilities
  - publication_time_is_unbound
  - python_constraint_is_ge_3_12_lt_3_13
  - poetry_install_and_external_git
  - no_vendored_python_and_no_vendored_cursor_binary
  - no_console_script
  - cursor_login_is_external
  - validation_commands_are_not_sandboxed
  - awaiting_decisions_exits_1_and_is_the_expected_pause
  - admission_rereads_sealed_validation_and_is_not_a_second_validation_run
  - task_b_is_activated_only_after_task_a_is_drained
  - terminal_rest_is_a_later_invocation_and_is_not_required_for_task_b_to_settle
capabilities:
  - TRANSITION_CONTROL_LAYER
  - DIRECT_GREEN
  - EXPECTED_RED_CLOSURE
  - WORKER_NONAUTHORITY
  - PRE_AUTHORIZATION_RESTART
  - POST_EFFECT_RECONCILIATION
  - SUCCESSOR_AFTER_RECOVERY
  - CODE_AUTHORITY
  - REFERENCE_REPRODUCIBILITY
  - CURSOR_ADAPTER
  - TERMINAL_REST
```
-->
