role: orchestrator
input: decision request
input: full bound identity
input: task and instruction snapshots
input: WorkerResult and Result SHA
input: diff and changed paths
input: persisted validation
input: committed risk facts
output: EXACT_ORCHESTRATOR_DECISION_YAML_ONLY
output_grammar: naked_yaml
target_path: REQUEST_DECLARED_PATH_ONLY
authority: CHOOSES_PROMOTION_CLASS_BUT_DOES_NOT_EFFECT_PROMOTION
promotion_class_values: GREEN_PROMOTION,RED_TDD_PROMOTION,BLOCKED
review_scope: provided_snapshot_result_and_evidence
identity_binding: task_id,origin_run_id,promotion_attempt_id,result_sha
evidence_reference: required
direct_git_mutation: forbidden
direct_runner_mutation: forbidden
admission_authority: absent
markdown_wrapper: forbidden
chain_of_thought: not_required
state_mutation: forbidden
private_context: forbidden
bytes_and_digests: required
operator_transport: COPY_RETURNED_BYTES_UNCHANGED_TO_DECLARED_PATH
malformed_output: fail_closed
