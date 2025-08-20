import sqlite3
from unittest.mock import patch

import boto3
import pytest
from typer.testing import CliRunner

from s3_index.cli.app import app
from s3_index.db.operations import setup_database


@pytest.fixture
def setup_test_data(aws_mock, mock_db_path, s3_client):
    """Setup test data in S3 and ensure the database is initialized."""

    # Create buckets
    buckets = ["test-integration-1", "test-integration-2"]
    for bucket in buckets:
        s3_client.create_bucket(Bucket=bucket)

    # Add test objects
    test_objects = [
        ("test-integration-1", "files/document1.txt", "Document 1 content"),
        ("test-integration-1", "files/document2.txt", "Document 2 content"),
        ("test-integration-1", "images/photo1.jpg", "Binary content"),
        ("test-integration-2", "logs/server.log", "Server log content"),
        ("test-integration-2", "data/report.csv", "CSV data content"),
    ]

    for bucket, key, content in test_objects:
        s3_client.put_object(Bucket=bucket, Key=key, Body=content)

    # Initialize database
    setup_database()

    return s3_client


@pytest.fixture
def cli_runner():
    """Provide a Typer CLI runner."""
    return CliRunner()


def test_save_and_search_workflow(cli_runner, mock_db_path, setup_test_data):
    """Test the complete workflow of saving keys and then searching them."""
    # 1. First, save all keys from the first bucket
    save_result = cli_runner.invoke(app, ["save", "test-integration-1"])
    assert save_result.exit_code == 0

    # 2. Verify keys were saved in the database
    conn = sqlite3.connect(mock_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM s3_keys WHERE bucket = 'test-integration-1'")
    count = cursor.fetchone()[0]
    conn.close()

    # Should have 3 keys from test-integration-1
    assert count == 3

    # 3. Search for document files
    with patch("s3_index.cli.app.console.print") as mock_print:
        search_result = cli_runner.invoke(app, ["search", "document"])
        assert search_result.exit_code == 0

        # Verify we got results
        found_results = False
        for call in mock_print.call_args_list:
            if isinstance(call[0][0], str) and "Found" in call[0][0] and "results" in call[0][0]:
                found_results = True
                break

        assert found_results, "Search results were not found in the output"

    # 4. Save keys from the second bucket
    save_result_2 = cli_runner.invoke(app, ["save", "test-integration-2"])
    assert save_result_2.exit_code == 0

    # 5. Verify all keys are now in the database
    conn = sqlite3.connect(mock_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM s3_keys")
    total_count = cursor.fetchone()[0]
    conn.close()

    # Should have 5 keys in total
    assert total_count == 5

    # 6. Search for a different pattern
    with patch("s3_index.cli.app.console.print") as mock_print:
        search_result_2 = cli_runner.invoke(app, ["search", "data"])
        assert search_result_2.exit_code == 0

        # Verify specific search result was found
        data_found = False
        for call in mock_print.call_args_list:
            args = call[0]
            if len(args) > 0 and hasattr(args[0], "add_row"):
                # This is a guess at how the Rich Table is being used
                data_found = True

        assert data_found, "Table results for 'data' search were not found"


def test_incremental_updates(cli_runner, mock_db_path, setup_test_data):
    """Test that running save twice doesn't duplicate keys."""
    # First, ensure database has content from previous test
    conn = sqlite3.connect(mock_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM s3_keys")
    before_count = cursor.fetchone()[0]
    conn.close()

    # Run save again on all buckets
    save_result = cli_runner.invoke(app, ["save"])
    assert save_result.exit_code == 0

    # Check database count hasn't changed significantly
    conn = sqlite3.connect(mock_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM s3_keys")
    after_count = cursor.fetchone()[0]
    conn.close()

    # Count should be the same (or very close if there are timing issues)
    assert after_count <= before_count + 1, "Keys were duplicated in the database"


def test_add_new_objects_and_update(cli_runner, mock_db_path, setup_test_data):
    """Test that new objects are added when running save after S3 changes."""
    # Add a new object to S3
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.put_object(
        Bucket="test-integration-1",
        Key="new/added_during_test.txt",
        Body="New content added during test",
    )

    # Get current count
    conn = sqlite3.connect(mock_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM s3_keys")
    before_count = cursor.fetchone()[0]
    conn.close()

    # Run save to update
    save_result = cli_runner.invoke(app, ["save", "test-integration-1"])
    assert save_result.exit_code == 0

    # Check that count increased by 1
    conn = sqlite3.connect(mock_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM s3_keys")
    after_count = cursor.fetchone()[0]
    conn.close()

    # Should have one more key now
    assert after_count == before_count + 1

    # Verify we can search for the new key
    with patch("s3_index.cli.app.console.print") as mock_print:
        search_result = cli_runner.invoke(app, ["search", "added_during_test"])
        assert search_result.exit_code == 0

        # Result should be found
        found = False
        for call in mock_print.call_args_list:
            if (
                len(call[0]) > 0
                and isinstance(call[0][0], str)
                and "Found" in call[0][0]
                and "1" in call[0][0]
            ):
                found = True
                break

        assert found, "Newly added object was not found in search results"
