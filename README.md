# S3 Index

A CLI tool to save S3 keys to a SQLite database and search them efficiently. Built with Python 3.13 and modern typing.

## Installation

Clone the repository and install using pip:

```bash
git clone <repository_url>
cd s3-index
pip install -e .
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
2. Create a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
3. Install development dependencies:
   ```bash
   pip install -e .
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

```bash
pytest
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
- AWS credentials configured (via environment variables, AWS profile, or IAM role)

## License

This project is licensed under the MIT License - see the LICENSE file for details.
