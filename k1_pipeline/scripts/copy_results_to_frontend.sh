#!/bin/bash
# Copies pipeline output to the React frontend's public directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
FRONTEND_DIR="$(dirname "$PROJECT_DIR")/frontend/public"

# Single-file pipeline results
SOURCE="$PROJECT_DIR/data/output/pipeline_results.json"
if [ -f "$SOURCE" ]; then
    cp "$SOURCE" "$FRONTEND_DIR/pipeline_results.json"
    echo "Copied pipeline_results.json to frontend/public/"
else
    echo "No single-file pipeline results found (optional)"
fi

# Batch pipeline results
BATCH_SOURCE="$PROJECT_DIR/data/output/batch/batch_pipeline_results.json"
if [ -f "$BATCH_SOURCE" ]; then
    mkdir -p "$FRONTEND_DIR/batch"
    cp "$BATCH_SOURCE" "$FRONTEND_DIR/batch/batch_pipeline_results.json"
    echo "Copied batch_pipeline_results.json to frontend/public/batch/"
else
    echo "No batch pipeline results found (optional)"
fi

# At least one result should exist
if [ ! -f "$SOURCE" ] && [ ! -f "$BATCH_SOURCE" ]; then
    echo "No pipeline results found at all."
    echo "Run the Dagster pipeline first: cd $PROJECT_DIR && uv run dg dev"
    exit 1
fi
