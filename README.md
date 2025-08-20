# S3 Index

A CLI tool to save S3 keys to a SQLite database and search them efficiently. Built with Python 3.13 and modern typing.

## Installation

Clone the repository and install using uv:

```bash
git clone <repository_url>
cd s3-index
uv pip install -e .
```

## Usage

### Save S3 keys to SQLite

Save keys from a specific S3 bucket:

```bash
s3-index save my-bucket-name
```

Save keys from all accessible S3 buckets:

```bash
s3-index save
```




### Search for keys

Search for keys containing a specific string:

```bash
s3-index search "prefix/to/search"
```

## Features

- Save keys from one or all S3 buckets to a local SQLite database
- Efficient batch processing to handle large S3 buckets with minimal memory usage
- Automatic duplicate detection to avoid redundant database entries
- Fast search through stored keys without accessing S3
- Rich text display of search results
- Automatic database creation and management

## Project Structure

The project follows a modular architecture:

```
s3-index/
├── src/
│   └── s3_index/
│       ├── cli/              # CLI command implementation
│       │   ├── __init__.py
│       │   └── app.py        # Main CLI application
│       ├── db/               # Database operations
│       │   ├── __init__.py
│       │   └── operations.py # SQLite database functions
│       ├── s3/               # S3 operations
│       │   ├── __init__.py
│       │   └── operations.py # S3 bucket and key functions with batch processing
│       ├── __init__.py       # Package initialization
│       └── version.py        # Version information
└── pyproject.toml           # Project configuration
```

## Development

### Setting up a Development Environment

1. Clone the repository
2. Create a virtual environment with uv:
   ```bash
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
3. Install development dependencies:
   ```bash
   uv pip install -e ".[dev]"
   uv sync
   ```

4. Run the tests to verify your setup:
   ```bash
   uv run pytest
   ```

### Code Quality

The project uses [ruff](https://github.com/astral-sh/ruff) for code formatting and linting:

```bash
# Format code
uv run ruff format src

# Lint code
uv run ruff check src

# Format and lint (with auto-fixes)
./scripts/format.sh
```

### Running Tests

The project uses pytest for automated testing. Run the tests with:

```bash
# Run all tests with the provided script
./scripts/test.sh

# Or run specific tests manually:
# Run all tests
uv run pytest

# Run only unit tests
uv run pytest tests/unit

# Run only integration tests
uv run pytest tests/integration

# Run with coverage report
uv run pytest --cov=s3_index
```

The test suite includes:

- **Unit tests**: Test individual functions in isolation
- **Integration tests**: Test the complete workflow with mocked S3
- **CLI tests**: Test the command-line interface functionality

The test script `scripts/test.sh` runs linting, formatting checks, unit tests, integration tests, and generates a coverage report.



### Adding Dependencies

```bash
# Add a runtime dependency
uv add package_name

# Add a development dependency
uv add --dev package_name

# Update dependencies
uv sync
```

### Advanced Usage

#### Batch Processing

The CLI supports processing S3 keys in batches to improve memory efficiency when dealing with large buckets:

```bash
# Save keys in batches of 200 (default is 100)
s3-index save --batch-size 200

# Save keys from a specific bucket with custom batch size
s3-index save my-bucket-name -b 500

# The CLI automatically detects and skips duplicate keys
s3-index save my-bucket-name -b 500
```

Benefits of batch processing:
- Reduced memory usage when dealing with millions of keys
- Progress updates during processing
- More resilient to connection issues

## Requirements

- Python 3.13+
- uv package manager (for dependency management)
- AWS credentials configured (via environment variables, AWS profile, or IAM role)
- pytest and moto (for running tests)

## License

This project is licensed under the MIT License - see the LICENSE file for details.
