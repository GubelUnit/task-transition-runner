# Proof index

Run the contract suite with the command below. The file list is the public test modules in the pinned tree. It is not a product-capability list and it does not publish a private test count. Coverage is evidence about the declared candidate scope, not product law.

The contract suite entry point is:

```bash
bash scripts/validate.sh --cov="${PWD}/src/task_transition_runner" --cov-branch --cov-fail-under=100
```

Run it from the package checkout. `--cov` names the absolute checkout source directory `${PWD}/src/task_transition_runner`. The same invocation requests branch coverage and fails under 100 percent. Default selection excludes tests marked `external_witness`. The live Cursor reference edge carries that marker, so this suite does not execute it. The real-Cursor A-to-B witness is historical and bound to the pre-export candidate history. It was not rerun against the current public `HEAD`.

The supported v0.1.0 reference path is direct GREEN. Current-snapshot reproducibility is limited to the documented direct-GREEN path and the deterministic contract suite when that suite exits 0 on the checked-out snapshot. Local source changes create a different proof subject. It does not claim identical model outputs, diffs, or result SHAs.

Among the integration modules below, `tests/integration/test_direct_green.py` exercises the supported direct-GREEN lifecycle. Expected RED is not a supported public CLI lifecycle in this release: `tests/integration/test_expected_red_green.py` exercises downstream RED admission with a pre-materialized `red_lock` (the public CLI does not currently create that lock), and the v0.1.0 failure-signature matcher reads the persisted validation log, including runner-added metadata, rather than raw process output alone.

Private control-plane records are not part of this index.

## Public test modules

- `tests/adapters/test_cursor_adapter_integration.py`
- `tests/adapters/test_cursor_evidence.py`
- `tests/adapters/test_cursor_headless_invocation.py`
- `tests/adapters/test_cursor_headless_process.py`
- `tests/adapters/test_cursor_live_component.py`
- `tests/adapters/test_cursor_privacy.py`
- `tests/adapters/test_cursor_prompt_frame.py`
- `tests/adapters/test_cursor_stream.py`
- `tests/integration/test_code_authority_topology.py`
- `tests/integration/test_direct_green.py`
- `tests/integration/test_expected_red_green.py`
- `tests/integration/test_git_lifecycle.py`
- `tests/integration/test_successor_after_recovery.py`
- `tests/proofs/test_traceability_index.py`
- `tests/test_cli_init.py`
- `tests/test_cli_run.py`
- `tests/test_config.py`
- `tests/test_decision_contracts.py`
- `tests/test_default_suite_hermetic_boundary.py`
- `tests/test_package_smoke.py`
- `tests/test_project_metadata.py`
- `tests/test_reference_bundle.py`
- `tests/test_reference_prompts.py`
- `tests/test_runner_code_authority.py`
- `tests/test_runner_continuation_contract.py`
- `tests/test_runner_continuation_lifecycle.py`
- `tests/test_runner_continuation_origin_proofs.py`
- `tests/test_runner_drain_transition.py`
- `tests/test_runner_git_context.py`
- `tests/test_runner_git_promotion.py`
- `tests/test_runner_main_state_bootstrap.py`
- `tests/test_runner_post_tick_continuation.py`
- `tests/test_runner_promotion_contracts.py`
- `tests/test_runner_promotion_journal.py`
- `tests/test_runner_promotion_reconciliation.py`
- `tests/test_runner_promotion_state.py`
- `tests/test_runner_start_tick_reconciliation.py`
- `tests/test_status.py`
- `tests/test_task_activation.py`
- `tests/test_task_contract.py`
- `tests/test_task_end_promotion.py`
- `tests/test_task_source.py`
- `tests/test_validate_entry.py`
- `tests/test_worker_result.py`

<!-- Traceability: [[claim:CANDIDATE_COVERAGE_BOUNDARY]] [[claim:REFERENCE_REPRODUCIBILITY]] [[claim:CURSOR_ADAPTER]] [[claim:DIRECT_GREEN]] [[claim:EXPECTED_RED_CLOSURE]] [[claim:MODEL_OUTPUT_DETERMINISM]] [[claim:DIFF_DETERMINISM]] [[claim:RESULT_SHA_DETERMINISM]] -->
