# task-transition-runner

`task-transition-runner` is a local control layer for one active task and for durable Git promotion. It admits or blocks a declared task, its evidence, the external decisions, and the Git effect. Mechanical admission is not universal semantic correctness.

Start here:

1. Install with `INSTALL.md`.
2. Learn `init`, `run`, and `status` in `USAGE.md`. Exit 1 with outcome `AWAITING_DECISIONS` is the normal pause for decisions.
3. Follow one concrete edge in `REFERENCE_WORKFLOW.md`.
4. Read `KNOWN_LIMITS.md` before assuming a feature exists. Headings there name limits, and the sentences say what the runner does not do.

The public command set is exactly `init`, `run`, and `status`. There is no fourth command and no console script.

The evidenced execution adapter is one Cursor headless adapter. The package does not claim provider-agnostic execution.

Reproducibility means the reference path in `REFERENCE_WORKFLOW.md` plus the deterministic contract suite, with exit 0, on the exact release source snapshot. It does not mean identical model outputs, diffs, or result SHAs.

Read `INSTALL.md`, `USAGE.md`, `REFERENCE_WORKFLOW.md`, `KNOWN_LIMITS.md`, and `PROOF_INDEX.md` before treating any outcome as product proof. The machine-readable projection of those technical pages is `AGENT_GUIDE.yaml`. It does not add commands or authority.

## Background

The product is the runner described above. The only bound historical statement is that the Human Operator maintained intent, governance, and release authority. That statement is production provenance. It is not a product capability, not zero human involvement, and not line-by-line human authorship or review. No numeric historical metric is bound. See `DEVELOPMENT_MODEL.md`. This background is not required to install or run the runner.

## Identity

These identity facts come from the bound human record. They are not runner features.

```text
SPDX-License-Identifier: MIT
Copyright (c) 2026 GubelUnit
Attribution: GubelUnit - task-transition-runner
Public identity: task-transition-runner
Repository host: github.com
Repository namespace: GubelUnit
Repository name: task-transition-runner
publication_time: 2026-11-07
```

The bound repository target is `github.com/GubelUnit/task-transition-runner`. Publication 2026-11-07.

Package metadata in the pinned `pyproject.toml` names the distribution `task-transition-runner` and the import package `task_transition_runner`, version `0.1.0`.

<!--
Traceability: [[claim:TRANSITION_CONTROL_LAYER]] [[claim:FOURTH_CLI_COMMAND]] [[claim:CURSOR_ADAPTER]] [[claim:PROVIDER_AGNOSTICISM]] [[claim:REFERENCE_REPRODUCIBILITY]] [[claim:LICENSE_NOTICE]] [[claim:PUBLIC_REPOSITORY_EXPORT]] [[claim:CANDIDATE_SEAL]]
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
