#!/bin/bash
set -euo pipefail

# Terminal colors
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Running tests for s3-index...${NC}"

# Ensure we're in the project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Run linting
echo -e "${YELLOW}Running linting checks...${NC}"
uv run ruff check src tests

# Run formatting check
echo -e "${YELLOW}Checking code formatting...${NC}"
uv run ruff format --check src tests

# Run unit tests
echo -e "${YELLOW}Running unit tests...${NC}"
uv run pytest tests/unit -v

# Run integration tests
echo -e "${YELLOW}Running integration tests...${NC}"
uv run pytest tests/integration -v

# Run coverage
echo -e "${YELLOW}Running tests with coverage...${NC}"
uv run pytest --cov=s3_index --cov-report=term-missing

echo -e "${GREEN}All tests passed!${NC}"