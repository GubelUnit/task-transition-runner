#!/usr/bin/env bash
set -euo pipefail

script_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
candidate_root="$(cd "${script_directory}/.." && pwd)"
cd "${candidate_root}"

if [[ $# -eq 0 ]]; then
    exec poetry run pytest \
        --cov=task_transition_runner \
        --cov-branch \
        --cov-fail-under=100
fi

exec poetry run pytest "$@"
