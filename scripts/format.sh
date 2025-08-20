#!/bin/bash
# format.sh - Format and lint Python code

set -e

# Change to the project root directory
cd "$(dirname "$0")/.."

echo "Formatting code with ruff..."
uv run ruff format src

echo "Linting code with ruff..."
uv run ruff check --fix src

echo "All done! Code is formatted and linted."