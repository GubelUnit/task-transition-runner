# Known limits

Each heading below is the name of a limit. The sentences say what the runner does not do. They are not missing features to infer around. A supported behavior in `USAGE.md` stays inside its qualification.

## PUBLIC_REPOSITORY_EXPORT

Public repository export is not a Runner capability.

Export, seal and license work are release-process artifacts. They do not become Runner features, and the candidate proof includes no export.

## CANDIDATE_SEAL

A candidate seal is not a supported Runner capability.

The bound candidate proof does not include a seal and does not authorize publication.

## LICENSE_NOTICE

A license or notice is not a supported Runner capability.

License, copyright, attribution and public identity remain open human decisions and are not product features.

The package identity in `README.md` records SPDX `MIT`, the copyright line, and `publication_time: UNBOUND`. `init`, `run`, and `status` do not apply that identity.

## FOURTH_CLI_COMMAND

The public CLI does not provide a fourth command.

The public CLI is exactly init, run and status.

## UI_BRIDGE

A Cursor UI bridge is not a supported v1 capability.

An experimental UI bridge, if present, is optional and host-dependent. It is not part of the headless proof and cannot invent decisions.

## BUILTIN_PLANNING

The Runner does not plan, decompose or prioritize tasks.

Task planning stays outside the Runner. The Runner does not choose a next task from a queue.

## PACKAGING_PYPI

Packaging or PyPI distribution is not a v1 claim.

PyPI packaging is not a prerequisite of the reference proof and is not a supported product capability.

## PROVIDER_AGNOSTICISM

The Runner is not provider-agnostic.

One Cursor headless adapter is evidenced. No second provider and no arbitrary replacement prompts are claimed.

## MODEL_OUTPUT_DETERMINISM

Repeated reference runs do not claim identical model outputs.

Reference reproducibility is the documented reference path and the deterministic contract suite. It does not claim identical model outputs.

## DIFF_DETERMINISM

Repeated reference runs do not claim identical diffs.

Reference reproducibility is the documented reference path and the deterministic contract suite. It does not claim identical diffs.

## RESULT_SHA_DETERMINISM

Repeated reference runs do not claim identical result SHAs.

Reference reproducibility is the documented reference path and the deterministic contract suite. It does not claim identical result SHAs.

## PRODUCER_INDEPENDENCE

Separate orchestrator and validator artifacts do not prove producer independence.

They prove separate contract roles and result binding, not cryptographic, organizational, personal or process independence of decision producers.

## HISTORICAL_REOPEN

Public v1 has no first-class historical reopen.

New work after terminal rest uses an ordinary active task and a fresh identity. Historical private runtime is not imported.

## RUNNER_SELF_UPGRADE

The Runner does not upgrade itself on a managed target.

A different Runner source SHA on an already managed target stays blocked until a later explicit adoption or migration contract, which is not part of v1.

## RUNTIME_MIGRATION

Public v1 does not migrate an existing runtime schema.

Empty runtime together with recognized prior public protocol history blocks. Automatic runtime-schema migration is not supported.

## PARALLEL_TASKS

Public v1 does not run tasks in parallel.

Execution is local and sequential, with at most one active task.

## DISTRIBUTED_COORDINATION

Public v1 does not provide distributed runners or distributed consensus.

The reference scope is one local target repository and one main branch. Remote push and pull-request orchestration are not claimed.

## SANDBOX_SAME_UID_ISOLATION

The Runner does not sandbox commands or protect the host from same-UID processes.

allowed_paths is a canonical promotion gate, not host protection. An externally provided sandbox remains compatible, but no built-in hostile same-UID isolation is claimed.

## ARBITRARY_ROLLBACK

The Runner does not provide arbitrary rollback.

Supported recovery is limited to the documented restart windows. It is not recovery from every crash or from filesystem corruption.

## UNIVERSAL_RECOVERY

The Runner does not recover from every interruption.

Supported restart windows are after a verified result before final decisions, and after a materialized promotion before final bookkeeping. Loss of a Cursor worker mid-stream is not continued automatically.

## EXACTLY_ONCE

The Runner does not claim exactly-once execution or exactly-once promotion.

It rejects reuse of a consumed or conflicting promotion identity inside the documented local protocol. That rejection is not a distributed exactly-once guarantee.

## TRANSACTIONAL

Promotion is not a transaction and not an atomic agent workflow.

The Runner admits or blocks durable Git and state transitions. It does not claim transactional rollback of an arbitrary partial workflow.

## Surface boundaries

The same limits, in operator language:

- One active task. No parallel tasks, no second active task, and no multi-repository scheduling.
- No distributed runners, no distributed consensus, and no remote push or pull-request orchestration.
- No server or hosted service, and no generic task-platform adapter.
- No built-in sandbox. An external sandbox may still be supplied around the process. Validation commands are not sandboxed. `allowed_paths` does not protect the host from same-UID processes or from privileged manipulation.
- No arbitrary rollback, no recovery from filesystem corruption, and no automatic continuation of a Cursor worker lost mid-stream.
- No exactly-once execution and no transactional promotion.
- No automatic planning, prioritization, or validator trust decision.
- No historical reopen, no runner self-upgrade, and no runtime-schema migration.
- No `allowed_write_roots` setting is part of v1.
- Cursor login is an external prerequisite. A UI bridge is not part of the headless proof.
- No support commitment, service level, or roadmap is stated.
- Coverage configuration is evidence about the declared package scope. It is not product law and it is not a private test count.
- Code authority for the reference path uses a clean Git source checkout.

<!--
Traceability: [[claim:PUBLIC_REPOSITORY_EXPORT]] [[claim:CANDIDATE_SEAL]] [[claim:LICENSE_NOTICE]] [[claim:FOURTH_CLI_COMMAND]] [[claim:UI_BRIDGE]] [[claim:BUILTIN_PLANNING]] [[claim:PACKAGING_PYPI]] [[claim:PROVIDER_AGNOSTICISM]] [[claim:MODEL_OUTPUT_DETERMINISM]] [[claim:DIFF_DETERMINISM]] [[claim:RESULT_SHA_DETERMINISM]] [[claim:PRODUCER_INDEPENDENCE]] [[claim:HISTORICAL_REOPEN]] [[claim:RUNNER_SELF_UPGRADE]] [[claim:RUNTIME_MIGRATION]] [[claim:PARALLEL_TASKS]] [[claim:DISTRIBUTED_COORDINATION]] [[claim:SANDBOX_SAME_UID_ISOLATION]] [[claim:ARBITRARY_ROLLBACK]] [[claim:UNIVERSAL_RECOVERY]] [[claim:EXACTLY_ONCE]] [[claim:TRANSACTIONAL]] [[claim:WORKER_NONAUTHORITY]] [[claim:CURSOR_ADAPTER]] [[claim:CANDIDATE_COVERAGE_BOUNDARY]] [[claim:CODE_AUTHORITY]]
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
