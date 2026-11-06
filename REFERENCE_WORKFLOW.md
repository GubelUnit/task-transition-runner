# Reference workflow

This page is one evidenced edge around the runner. It is not a general repository automation. The runner holds one active task. The next task must use a different `task_id`. `active_task: null` is the end. Task A and task B below are the fixture for this edge.

Validation runs before the decision and is sealed. The later admission `run` reads those sealed logs again. It does not launch the validation commands a second time. Do not start pytest yourself between a `PENDING_PROMOTION` result and the following admission `run`. The runner does not write an external pass record for the edge. The documented reference workflow can be executed with declared prerequisites and without private Hydra artifacts.

## Lay out one working directory

`runner.yaml` resolves every relative path from its own directory. Prompt paths must stay inside that directory. The repository stores the pieces apart, so copy them together before `init`:

- From `examples/reference/`: `runner.yaml`, `tasks.yaml`, `task_sources/`, and `task_specs/`. `tasks.yaml` is already task A, `reference-task-a`.
- From `prompts/`: `developer.md`, `orchestrator.md`, and `validator.md`, placed at `prompts/` inside the working directory.
- `target/`: a Git repository on branch `main`, with a clean work tree, seeded from `examples/fixture/` before its single commit. `src/reference_app/labels.py` and `summary.py` are not in that seed. The tasks add them.
- `runtime/`: an empty directory. It must not sit inside `target/`.

The working directory then contains `runner.yaml`, `tasks.yaml`, `task_sources/task_b.yaml`, `task_sources/terminal_rest.yaml`, the two task specs, `prompts/`, `target/`, and `runtime/`.

Task A allows only `src/reference_app/labels.py` and declares:

```text
python -m pytest -q -p no:cacheprovider tests/test_labels.py
```

Task B allows only `src/reference_app/summary.py` and declares that command, then:

```text
python -m pytest -q -p no:cacheprovider tests/test_summary.py
```

Each of those timeouts is 30 seconds. Both tasks expect `GREEN`. The public task files name pytest directly. `-p no:cacheprovider` stays on those declared commands and only suppresses `.pytest_cache`. It does not replace `PYTHONDONTWRITEBYTECODE=1`. Public runner invocations set that variable, and declared validation inherits it.

## Commands

Run this from the working directory. The word after the quoted program is `init`, `run`, or `status`.

```bash
PYTHONDONTWRITEBYTECODE=1 python -c 'import sys
from task_transition_runner.cli import main
raise SystemExit(main(sys.argv[1:]))' run --config runner.yaml
```

`poetry run python` is the same call inside the installed environment. There is no console script.

## The edge

Call `init` once. After success, and before the first `run`, the runtime holds `main_state.yaml`, `promotion_consumed.yaml`, and `runner_source_pin.yaml`. It has no `runs/` directory yet. Do not make a second baseline commit and do not call `init` again.

Then:

1. `run` with task A still in `tasks.yaml`. Stop at `AWAITING_DECISIONS` / `PROMOTION_DECISION_MISSING`. Exit 1 is the expected pause.
2. Open `runtime/runs/<origin_run_id>/decisions/decision_request.yaml`. Write task A's orchestrator and validator files from that request, using task A's four identity values. The field rules are in `USAGE.md`. This page does not supply a sample verdict.
3. `run` returns `PENDING_PROMOTION` / `PROMOTION_INTENT_PERSISTED`.
4. `run` returns `DRAINED_AWAITING_TASK` / `PROMOTION_EFFECT_ADMITTED`. Leave the sealed validation logs untouched between step 3 and step 4.
5. Only now, copy `task_sources/task_b.yaml` onto `tasks.yaml` once, byte for byte. Starting task B earlier is not this edge. Do not write that file again.
6. `run` until task B stops at its own `AWAITING_DECISIONS`.
7. Write new decision files from task B's own decision request. Use task B's four identity values.
8. `run` returns `PENDING_PROMOTION`.
9. `run` returns `SETTLED` / `CONTINUATION_CONSUMED`. Leave the sealed validation logs untouched between step 8 and step 9.

Task A is not `SETTLED`. Task B uses the code task A left behind and a new task id. After step 9, `runtime/post_tick_continuation.yaml` has status `CONSUMED`. The target work tree is clean. The runner source checkout is unchanged.

`changed_paths` for each worker result stay on that task's one allowed product file. `allowed_paths` gates promotion. It does not protect the host.

If a step returns `BLOCKED`, leave that working directory behind. Start again from a new target and a new runtime.

## Terminal rest after the edge

Task B can settle without another command. Terminal rest is a later, separate invocation. It is not required for step 9.

Copy `task_sources/terminal_rest.yaml` onto `tasks.yaml`. That file sets `active_task: null`. One further `run` returns `TERMINAL_REST` and writes no successor.

## Contract suite

The other half of the reproducibility claim is the deterministic contract suite on the exact release source snapshot. Run it from the package checkout, not from inside `target/`. Local source changes create a different proof subject:

```bash
bash scripts/validate.sh --cov="${PWD}/src/task_transition_runner" --cov-branch --cov-fail-under=100
```

That invocation measures the absolute checkout source directory `${PWD}/src/task_transition_runner`, with branch coverage, and fails under 100 percent. The default test selection excludes the `external_witness` marker. The live Cursor edge is marked that way, so this suite does not execute the edge above.

Exit 0 of this suite, together with the reference edge, is the reproducibility claim. It does not claim identical model outputs, diffs, or result SHAs, and it does not claim provider-agnostic execution.

<!--
Traceability: [[claim:PARALLEL_TASKS]] [[claim:DIRECT_GREEN]] [[claim:TERMINAL_REST]] [[claim:SUCCESSOR_AFTER_RECOVERY]] [[claim:SANDBOX_SAME_UID_ISOLATION]] [[claim:ARBITRARY_ROLLBACK]] [[claim:UNIVERSAL_RECOVERY]] [[claim:HISTORICAL_REOPEN]] [[claim:REFERENCE_REPRODUCIBILITY]] [[claim:CANDIDATE_COVERAGE_BOUNDARY]] [[claim:MODEL_OUTPUT_DETERMINISM]] [[claim:DIFF_DETERMINISM]] [[claim:RESULT_SHA_DETERMINISM]] [[claim:PROVIDER_AGNOSTICISM]]
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
