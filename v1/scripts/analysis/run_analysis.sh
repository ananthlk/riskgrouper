#!/bin/bash

DEFAULT_DATASETS=("claims_only")
DEFAULT_TARGETS=("y_any_60d")
DEFAULT_FORCE_REFRESH="true"

DATASETS_TO_RUN=("${DEFAULT_DATASETS[@]}")
TARGETS_TO_RUN=("${DEFAULT_TARGETS[@]}")
FORCE_REFRESH=$DEFAULT_FORCE_REFRESH

usage() {
    echo "Usage: $0 [--datasets <d1,d2...>] [--targets <t1,t2...>] [--force] [--help]"
    echo "Options:"
    echo "  --datasets   Comma-separated list of datasets (master, engaged, claims_only)."
    echo "  --targets    Comma-separated list of target variables."
    echo "  --force      Force a data refresh from Snowflake."
    echo "  --help       Display this help message."
    exit 1
}

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --datasets) DATASETS_STR="$2"; shift ;;
        --targets) TARGETS_STR="$2"; shift ;;
        --force) FORCE_REFRESH="true" ;;
        --help) usage ;;
        *) echo "Unknown parameter passed: $1"; usage ;;
    esac
    shift
done

if [ -n "$DATASETS_STR" ]; then
    IFS=',' read -r -a DATASETS_TO_RUN <<< "$DATASETS_STR"
fi
if [ -n "$TARGETS_STR" ]; then
    IFS=',' read -r -a TARGETS_TO_RUN <<< "$TARGETS_STR"
fi

REFRESH_FLAG=""
if [ "$FORCE_REFRESH" = "true" ]; then
    REFRESH_FLAG="--force-refresh"
    echo "INFO: Cache will be ignored. Forcing data refresh from Snowflake for all runs."
fi

TOTAL_DATASETS=${#DATASETS_TO_RUN[@]}
TOTAL_TARGETS=${#TARGETS_TO_RUN[@]}
TOTAL_RUNS=$((TOTAL_DATASETS * TOTAL_TARGETS))
CURRENT_RUN=0
echo "=============================================================================="
echo "Starting RiskGrouper Analysis Runner"
echo "=============================================================================="
echo "Datasets to run: ${DATASETS_TO_RUN[@]}"
echo "Target variables: ${TARGETS_TO_RUN[@]}"
echo "Total runs to execute: $TOTAL_RUNS"
echo "------------------------------------------------------------------------------"
for dataset in "${DATASETS_TO_RUN[@]}"; do
    for target in "${TARGETS_TO_RUN[@]}"; do
        CURRENT_RUN=$((CURRENT_RUN + 1))
        echo ""
        echo "--- RUN $CURRENT_RUN / $TOTAL_RUNS: STARTING ---"
        echo "Dataset: $dataset"
        echo "Target:  $target"
        COMMAND="PYTHONPATH=$(pwd) python src/RiskGrouper.py --dataset $dataset --target $target $REFRESH_FLAG"
        echo "Executing command: $COMMAND"
        echo "--------------------------------------------------"
        eval "$COMMAND"
        echo "--- RUN $CURRENT_RUN / $TOTAL_RUNS: COMPLETE ---"
    done
done
echo "=============================================================================="
echo "All analysis runs are complete."
echo "=============================================================================="