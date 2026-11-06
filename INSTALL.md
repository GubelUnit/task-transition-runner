# Install

From the package checkout:

```bash
poetry install
```

Python must be `>=3.12,<3.13`, as declared in `pyproject.toml`. The Poetry dev group supplies pytest and pytest-cov for the contract suite, and `poetry install` installs that group by default. There is no console script. Call the installed `python` by command name, usually through `poetry run python`.

The package does not include a Python interpreter or a Cursor binary. Git and Cursor login stay outside the package.

## Prerequisites

- Python `>=3.12,<3.13`, as declared in `pyproject.toml`.
- Poetry, for `poetry install`.
- Git on the command path. The runner does not configure Git identity. A Git identity that can create a commit must already exist. If it does not, a later promotion commit stops.
- For the evidenced adapter only: a `cursor-agent` executable on the command path. The reference config names that executable. Login to that adapter is external.
- Unset `CURSOR_API_ENDPOINT` before `run`. When that variable is present, the headless adapter rejects the invocation.

The reference validation commands also name `python` without a directory prefix. A PyPI package is not part of this install.

## What install does not do

Install does not initialize a target repository, does not start Cursor, and does not publish this project. Initialization is the separate `init` command in `USAGE.md`.

<!--
Traceability: [[claim:CURSOR_ADAPTER]] [[claim:PACKAGING_PYPI]]
```oss8-authoritative
commands: []
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
