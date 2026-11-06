role: developer
input: run-local task snapshot
runner_authority: run-local task snapshot is not an additional developer payload
input: run-local task spec snapshot
input: run-local developer prompt snapshot
input: exact allowed paths and validation commands
input: Target working directory
output: TERMINAL_CURSOR_ADAPTER_RESULT_CONSUMED_BY_RUNNER
output_grammar: naked
read_scope: current_run_task_and_instruction_snapshots_only
change_scope: allowed_paths_only
validation_scope: declared_commands_only
report: result_and_uncertainties
runtime_mutation: forbidden
decision_mutation: forbidden
promotion_claim: forbidden
settlement_claim: forbidden
markdown_wrapper: forbidden
chain_of_thought: not_required
state_mutation: forbidden
private_context: forbidden
bytes_and_digests: required
operator_transport: COPY_RETURNED_BYTES_UNCHANGED_TO_DECLARED_PATH
malformed_output: fail_closed
