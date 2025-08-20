import os
import sqlite3
import tempfile
from collections.abc import Generator, Iterator
from typing import Any

import boto3
import pytest
from moto import mock_aws
from typer.testing import CliRunner


@pytest.fixture
def temp_db_path() -> Iterator[str]:
    """Provide a temporary database path for testing."""
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    yield path
    os.unlink(path)


@pytest.fixture
def mock_db_path(monkeypatch: pytest.MonkeyPatch, temp_db_path: str) -> str:
    """Mock the database path function to use a temporary database."""

    def mock_get_db_path() -> str:
        return temp_db_path

    monkeypatch.setattr("s3_index.db.operations.get_db_path", mock_get_db_path)
    return temp_db_path


@pytest.fixture
def db_connection(mock_db_path: str) -> Iterator[sqlite3.Connection]:
    """Provide a database connection to the test database."""
    from s3_index.db.operations import setup_database

    # Setup the database
    setup_database()

    # Open a connection
    conn = sqlite3.connect(mock_db_path)
    yield conn
    conn.close()


@pytest.fixture
def aws_mock() -> Generator[None, None, None]:
    """Provide a mocked S3 environment."""
    with mock_aws():
        print("Setting up mocked AWS environment")

        yield


@pytest.fixture
def s3_client(aws_mock: Any) -> boto3.client:
    """Provide a mocked S3 client."""
    return boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def setup_s3_buckets(s3_client: boto3.client) -> list[dict[str, str]]:
    """Setup test buckets and objects in the mocked S3 environment."""
    # Create test buckets
    buckets = ["test-bucket-1", "test-bucket-2"]
    for bucket in buckets:
        s3_client.create_bucket(Bucket=bucket)

    # Create test objects in buckets
    test_objects = [
        {"bucket": "test-bucket-1", "key": "folder1/test1.txt"},
        {"bucket": "test-bucket-1", "key": "folder1/test2.txt"},
        {"bucket": "test-bucket-1", "key": "folder2/test3.txt"},
        {"bucket": "test-bucket-2", "key": "folderA/testA.txt"},
        {"bucket": "test-bucket-2", "key": "folderB/testB.txt"},
    ]

    for obj in test_objects:
        s3_client.put_object(
            Bucket=obj["bucket"], Key=obj["key"], Body=f"Test content for {obj['key']}"
        )

    # Add last_modified field to test objects for comparison
    for obj in test_objects:
        response = s3_client.head_object(Bucket=obj["bucket"], Key=obj["key"])
        obj["last_modified"] = response["LastModified"].isoformat()

    return test_objects


@pytest.fixture
def populated_db(
    db_connection: sqlite3.Connection, setup_s3_buckets: list[dict[str, str]]
) -> list[dict[str, str]]:
    """Setup a database populated with test data."""
    cursor = db_connection.cursor()

    for obj in setup_s3_buckets:
        cursor.execute(
            "INSERT INTO s3_keys (bucket, key, last_modified) VALUES (?, ?, ?)",
            (obj["bucket"], obj["key"], obj["last_modified"]),
        )

    db_connection.commit()
    return setup_s3_buckets


@pytest.fixture
def cli_runner() -> CliRunner:
    """Provide a Typer CLI test runner."""
    return CliRunner()
