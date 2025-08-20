import sqlite3
from datetime import UTC, datetime
from unittest.mock import patch

from s3_index.db.operations import (
    get_db_path,
    get_existing_keys,
    save_keys_to_db,
    search_keys,
    setup_database,
)


def test_get_db_path():
    """Test that get_db_path returns a path in the user's home directory."""
    db_path = get_db_path()
    assert "/.keymanager-file-db.sqlite" in db_path


def test_setup_database(mock_db_path: str):
    """Test database setup creates the necessary tables."""
    # Setup the database
    path = setup_database()

    # Check that the path matches our mocked path
    assert path == mock_db_path

    # Verify the table structure
    conn = sqlite3.connect(mock_db_path)
    cursor = conn.cursor()

    # Get table info
    cursor.execute("PRAGMA table_info(s3_keys)")
    columns = cursor.fetchall()

    # Check column names and types
    assert len(columns) == 4
    assert columns[0][1] == "id"
    assert columns[1][1] == "bucket"
    assert columns[2][1] == "key"
    assert columns[3][1] == "last_modified"

    # Check unique constraint
    cursor.execute("PRAGMA index_list(s3_keys)")
    indexes = cursor.fetchall()

    # There should be at least one unique index
    assert any(index[2] == 1 for index in indexes)

    conn.close()


def test_get_existing_keys_empty_input(mock_db_path: str):
    """Test that get_existing_keys returns an empty set for empty input."""
    result = get_existing_keys([])
    assert result == set()


def test_get_existing_keys(mock_db_path: str, populated_db: list[dict[str, str]]):
    """Test that get_existing_keys correctly identifies existing keys."""
    # Create a test set of keys, some existing and some new
    test_keys = [
        {"bucket": "test-bucket-1", "key": "folder1/test1.txt"},  # Exists
        {"bucket": "test-bucket-2", "key": "folderA/testA.txt"},  # Exists
        {"bucket": "test-bucket-1", "key": "new/key1.txt"},  # New
        {"bucket": "test-bucket-3", "key": "new/key2.txt"},  # New bucket and key
    ]

    existing = get_existing_keys(test_keys)

    # Should identify the two existing keys
    assert len(existing) == 2
    assert ("test-bucket-1", "folder1/test1.txt") in existing
    assert ("test-bucket-2", "folderA/testA.txt") in existing
    assert ("test-bucket-1", "new/key1.txt") not in existing
    assert ("test-bucket-3", "new/key2.txt") not in existing


def test_save_keys_to_db_empty_input(mock_db_path: str, db_connection: sqlite3.Connection):
    """Test that save_keys_to_db handles empty input correctly."""
    saved, skipped = save_keys_to_db([])
    assert saved == 0
    assert skipped == 0


def test_save_keys_to_db_new_keys(mock_db_path: str, db_connection: sqlite3.Connection):
    """Test saving new keys to the database."""
    # Create test data
    now = datetime.now(UTC).isoformat()
    test_keys = [
        {"bucket": "test-bucket", "key": "key1.txt", "last_modified": now},
        {"bucket": "test-bucket", "key": "key2.txt", "last_modified": now},
    ]

    # Save the keys
    saved, skipped = save_keys_to_db(test_keys)

    # Check the results
    assert saved == 2
    assert skipped == 0

    # Verify the keys were saved to the database
    cursor = db_connection.cursor()
    cursor.execute("SELECT bucket, key FROM s3_keys")
    results = cursor.fetchall()

    assert len(results) == 2
    assert ("test-bucket", "key1.txt") in results
    assert ("test-bucket", "key2.txt") in results


def test_save_keys_to_db_existing_keys(mock_db_path: str, populated_db: list[dict[str, str]]):
    """Test saving a mix of new and existing keys to the database."""
    # Create test data with a mix of new and existing keys
    now = datetime.now(UTC).isoformat()
    test_keys = [
        # Existing key
        {"bucket": "test-bucket-1", "key": "folder1/test1.txt", "last_modified": now},
        # New key
        {"bucket": "test-bucket-1", "key": "new/key.txt", "last_modified": now},
    ]

    # Save the keys
    saved, skipped = save_keys_to_db(test_keys)

    # Check the results
    assert saved == 1  # Only one new key should be saved
    assert skipped == 1  # One existing key should be skipped

    # Verify the new key was saved
    conn = sqlite3.connect(mock_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT bucket, key FROM s3_keys WHERE key = 'new/key.txt'")
    results = cursor.fetchall()

    assert len(results) == 1
    assert results[0] == ("test-bucket-1", "new/key.txt")
    conn.close()


def test_save_keys_to_db_with_exception(mock_db_path: str, db_connection: sqlite3.Connection):
    """Test the fallback mechanism when batch insert fails."""
    # Create test data
    now = datetime.now(UTC).isoformat()
    test_keys = [
        {"bucket": "test-bucket", "key": "key1.txt", "last_modified": now},
        {"bucket": "test-bucket", "key": "key2.txt", "last_modified": now},
    ]

    # Mock executemany to fail but allow execute to succeed for the fallback
    with patch("sqlite3.Cursor.executemany") as mock_executemany:
        mock_executemany.side_effect = Exception("Test exception")

        # Save the keys - should use fallback mechanism
        saved, skipped = save_keys_to_db(test_keys)

    # Check the results
    assert saved == 2  # Both keys should be saved via individual inserts
    assert skipped == 0

    # Verify the keys were saved to the database
    cursor = db_connection.cursor()
    cursor.execute("SELECT bucket, key FROM s3_keys")
    results = cursor.fetchall()

    assert len(results) == 2
    assert ("test-bucket", "key1.txt") in results
    assert ("test-bucket", "key2.txt") in results


def test_search_keys_no_match(mock_db_path: str, populated_db: list[dict[str, str]]):
    """Test searching for keys with no matches."""
    results = search_keys("nonexistent")
    assert len(results) == 0


def test_search_keys_with_matches(mock_db_path: str, populated_db: list[dict[str, str]]):
    """Test searching for keys with matches."""
    # Search for keys containing 'folder1'
    results = search_keys("folder1")

    # Should find two keys in test-bucket-1
    assert len(results) == 2

    buckets = [r[0] for r in results]
    keys = [r[1] for r in results]

    assert all(b == "test-bucket-1" for b in buckets)
    assert "folder1/test1.txt" in keys
    assert "folder1/test2.txt" in keys


def test_search_keys_partial_match(mock_db_path: str, populated_db: list[dict[str, str]]):
    """Test searching for keys with partial string matches."""
    # Search for keys containing 'test'
    results = search_keys("test")

    # Should find all test keys
    assert len(results) == 5
