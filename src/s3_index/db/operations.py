import sqlite3
from pathlib import Path


def get_db_path() -> str:
    """Get the path to the SQLite database file"""
    db_path = Path.home() / ".keymanager-file-db.sqlite"
    return str(db_path)


def setup_database() -> str:
    """Create SQLite database and tables if they don't exist

    Returns:
        str: Path to the database file
    """
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table for S3 keys
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS s3_keys (
        id INTEGER PRIMARY KEY,
        bucket TEXT,
        key TEXT,
        last_modified TEXT,
        UNIQUE(bucket, key)
    )
    """)

    conn.commit()
    conn.close()
    return db_path


def get_existing_keys(keys: list[dict[str, object]]) -> set[tuple[str, str]]:
    """Check which keys already exist in the database

    Args:
        keys: List of dictionaries containing key information

    Returns:
        set[tuple[str, str]]: Set of (bucket, key) tuples that already exist
    """
    if not keys:
        return set()

    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Extract bucket and key pairs from the input
    bucket_key_pairs = [(k["bucket"], k["key"]) for k in keys]

    # Build placeholders for the SQL query
    placeholders = ",".join(["(?, ?)"] * len(bucket_key_pairs))

    # Flatten the list of tuples for the SQL parameters
    flat_params = [item for pair in bucket_key_pairs for item in pair]

    # Query to find existing keys
    query = f"""
        SELECT bucket, key FROM s3_keys 
        WHERE (bucket, key) IN ({placeholders})
    """

    cursor.execute(query, flat_params)
    existing_keys = set(cursor.fetchall())
    conn.close()

    return existing_keys


def save_keys_to_db(keys: list[dict[str, object]]) -> tuple[int, int]:
    """Save keys to SQLite database

    Args:
        keys: List of dictionaries containing key information

    Returns:
        tuple[int, int]: (Number of keys successfully saved, Number of keys skipped)
    """
    if not keys:
        return (0, 0)

    db_path = setup_database()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Find keys that already exist in the database
    existing_keys = get_existing_keys(keys)

    # Separate new and existing keys
    new_keys = []
    skipped = 0

    for key_info in keys:
        bucket = key_info["bucket"]
        key = key_info["key"]

        if (bucket, key) in existing_keys:
            skipped += 1
        else:
            new_keys.append(key_info)

    count = 0

    try:
        # Use a transaction for better performance
        conn.execute("BEGIN TRANSACTION")

        if new_keys:
            # Insert only new keys
            sql = "INSERT INTO s3_keys (bucket, key, last_modified) VALUES (?, ?, ?)"
            values = [
                (key_info["bucket"], key_info["key"], key_info["last_modified"])
                for key_info in new_keys
            ]

            # Execute batch insert
            cursor.executemany(sql, values)

            # Count affected rows
            count = cursor.rowcount if cursor.rowcount > 0 else len(new_keys)

        # Commit the transaction
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error during batch save: {str(e)}")

        # Fall back to individual inserts if batch fails
        count = 0
        for key_info in new_keys:
            try:
                cursor.execute(
                    "INSERT INTO s3_keys (bucket, key, last_modified) VALUES (?, ?, ?)",
                    (key_info["bucket"], key_info["key"], key_info["last_modified"]),
                )
                count += 1
            except Exception as e:
                print(f"Error saving key {key_info['key']}: {str(e)}")

        conn.commit()
    finally:
        conn.close()

    return (count, skipped)


def search_keys(query: str) -> list[tuple[str, str, str]]:
    """Search for keys in the database

    Args:
        query: Search string to look for in key names

    Returns:
        List of tuples containing (bucket, key, last_modified)
    """
    db_path = get_db_path()

    # Create the database if it doesn't exist
    setup_database()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    search_term = f"%{query}%"
    cursor.execute(
        "SELECT bucket, key, last_modified FROM s3_keys WHERE key LIKE ?", (search_term,)
    )

    results = cursor.fetchall()
    conn.close()

    return results
