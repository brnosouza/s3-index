import os
import sqlite3
from unittest.mock import patch

from s3_index.cli.app import app


def test_save_command(cli_runner, mock_db_path, aws_mock, setup_s3_buckets):
    """Test the save command with a specific bucket."""
    with patch("s3_index.cli.app.console.print") as mock_print:
        result = cli_runner.invoke(app, ["save", "test-bucket-1"])

        # Check that the command executed successfully
        assert result.exit_code == 0

        # Verify some expected console output was called
        mock_print.assert_any_call("[bold]Saving S3 keys to SQLite database...[/bold]")

        # Check that keys were saved to the database
        conn = sqlite3.connect(mock_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM s3_keys WHERE bucket = 'test-bucket-1'")
        count = cursor.fetchone()[0]
        conn.close()

        # Should have saved 3 keys from test-bucket-1
        assert count == 3


def test_save_command_all_buckets(cli_runner, mock_db_path, aws_mock, setup_s3_buckets):
    """Test the save command without specifying a bucket (all buckets)."""
    with patch("s3_index.cli.app.console.print"):
        result = cli_runner.invoke(app, ["save"])

        # Check that the command executed successfully
        assert result.exit_code == 0

        # Verify database has all the keys
        conn = sqlite3.connect(mock_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM s3_keys")
        count = cursor.fetchone()[0]
        conn.close()

        # Should have saved all 5 keys from both buckets
        assert count == 5


def test_save_command_with_batch_size(cli_runner, mock_db_path, aws_mock, setup_s3_buckets):
    """Test the save command with a custom batch size."""
    with patch("s3_index.cli.app.console.print") as mock_print:
        result = cli_runner.invoke(app, ["save", "--batch-size", "2"])

        # Check that the command executed successfully
        assert result.exit_code == 0

        # There should be multiple batches processed
        # At least 3 batch processing messages (for 5 keys with batch size 2)
        batch_count = sum(
            1 for call in mock_print.call_args_list if any("Batch" in str(arg) for arg in call[0])
        )
        assert batch_count >= 3


def test_search_command_with_results(cli_runner, mock_db_path, populated_db):
    """Test the search command with results."""
    with patch("s3_index.cli.app.console.print") as mock_print:
        result = cli_runner.invoke(app, ["search", "folder1"])

        # Check that the command executed successfully
        assert result.exit_code == 0

        # Verify expected output
        mock_print.assert_any_call(
            "[bold]Searching for keys containing: [cyan]folder1[/cyan][/bold]"
        )

        # Check for "Found X results" message
        results_msg_found = False
        for call in mock_print.call_args_list:
            args = call[0][0]
            if isinstance(args, str) and "Found" in args and "results" in args:
                results_msg_found = True
                break

        assert results_msg_found, "Expected 'Found X results' message was not printed"


def test_search_command_no_results(cli_runner, mock_db_path, populated_db):
    """Test the search command with no results."""
    with patch("s3_index.cli.app.console.print") as mock_print:
        result = cli_runner.invoke(app, ["search", "nonexistent"])

        # Check that the command executed successfully
        assert result.exit_code == 0

        # Verify expected output for no results
        mock_print.assert_any_call("[yellow]No results found[/yellow]")


def test_save_command_error_handling(cli_runner, mock_db_path):
    """Test error handling in the save command."""
    # Create a mock that simulates an error in list_s3_keys
    with patch("s3_index.cli.app.list_s3_keys") as mock_list_keys:
        mock_list_keys.side_effect = Exception("S3 connection error")

        # Run the command
        result = cli_runner.invoke(app, ["save"])

        # Should complete but with an error message
        assert result.exit_code != 0
        assert "S3 connection error" in result.stdout


def test_search_command_database_error(cli_runner, mock_db_path):
    """Test error handling in the search command when database has issues."""
    # Ensure the database doesn't exist or is corrupt
    if os.path.exists(mock_db_path):
        os.remove(mock_db_path)

    # Create an invalid database
    with open(mock_db_path, "w") as f:
        f.write("Not a valid SQLite database")

    # The search should still run without crashing, but might show 0 results
    with patch("s3_index.cli.app.console.print"):
        result = cli_runner.invoke(app, ["search", "test"])

        # Command should complete without exception
        assert result.exit_code == 0
