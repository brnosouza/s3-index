from unittest.mock import MagicMock, patch

from s3_index.s3.operations import list_s3_keys


def test_list_s3_keys_specific_bucket(aws_mock, setup_s3_buckets):
    """Test listing keys from a specific bucket."""
    # Call the function with a specific bucket
    results = []
    for batch in list_s3_keys("test-bucket-1", batch_size=2):
        results.extend(batch)

    # Verify the results
    assert len(results) == 3  # test-bucket-1 has 3 objects

    # Check that all objects are from the specified bucket
    assert all(item["bucket"] == "test-bucket-1" for item in results)

    # Check that all keys from the bucket are included
    keys = [item["key"] for item in results]
    assert "folder1/test1.txt" in keys
    assert "folder1/test2.txt" in keys
    assert "folder2/test3.txt" in keys


def test_list_s3_keys_all_buckets(aws_mock, setup_s3_buckets):
    """Test listing keys from all buckets."""
    # Call the function without specifying a bucket
    results = []
    for batch in list_s3_keys(batch_size=2):
        results.extend(batch)

    # Verify the results
    assert len(results) == 5  # Total of 5 objects across all buckets

    # Check bucket distribution
    bucket1_items = [item for item in results if item["bucket"] == "test-bucket-1"]
    bucket2_items = [item for item in results if item["bucket"] == "test-bucket-2"]

    assert len(bucket1_items) == 3
    assert len(bucket2_items) == 2


def test_list_s3_keys_batch_size(aws_mock, setup_s3_buckets):
    """Test that list_s3_keys respects the batch size parameter."""
    # Use a batch size of 2 with 5 total objects
    batches = list(list_s3_keys(batch_size=2))

    # Should yield 3 batches: 2 full and 1 partial
    assert len(batches) == 3

    for idx, batch_size in enumerate([2, 2, 1]):
        # Each batch should not exceed the specified size
        assert len(batches[idx]) == batch_size


def test_list_s3_keys_bucket_error():
    """Test handling of errors when accessing a bucket."""
    with patch("boto3.client") as mock_boto:
        # Setup mock to raise an exception for the bucket
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        paginator = MagicMock()
        mock_s3.get_paginator.return_value = paginator

        # Make paginator.paginate raise an exception
        paginator.paginate.side_effect = Exception("Access denied")

        # Call the function
        mock_s3.list_buckets.return_value = {"Buckets": [{"Name": "error-bucket"}]}

        # Function should yield an empty batch when encountering an error
        batches = list(list_s3_keys())

        # No successful batches, but no exception raised
        assert len(batches) == 0


def test_list_s3_keys_empty_bucket(aws_mock, s3_client):
    """Test handling of empty buckets."""
    # Create an empty bucket
    s3_client.create_bucket(Bucket="empty-bucket")

    # Call the function with the empty bucket
    batches = list(list_s3_keys("empty-bucket"))

    # Should yield no batches for an empty bucket
    assert len(batches) == 0


def test_list_s3_keys_with_continuation(aws_mock, s3_client):
    """Test handling of pagination in S3 responses."""
    # Create a bucket with many objects to trigger pagination
    bucket_name = "pagination-test"
    s3_client.create_bucket(Bucket=bucket_name)

    # Add 25 objects - enough to ensure pagination with a small MaxKeys
    for i in range(25):
        s3_client.put_object(
            Bucket=bucket_name, Key=f"object-{i}.txt", Body=f"Content for object-{i}"
        )

    # Set a small batch size to test the batching logic
    batch_size = 5
    results = []

    # List keys with a small batch size
    for batch in list_s3_keys(bucket_name, batch_size=batch_size):
        results.extend(batch)
        # Verify each batch is not larger than the specified size
        assert len(batch) <= batch_size

    # We should get all 25 objects
    assert len(results) == 25

    # Verify all objects have the expected format
    for item in results:
        assert "bucket" in item
        assert "key" in item
        assert "last_modified" in item
        assert item["bucket"] == bucket_name
        assert item["key"].startswith("object-")
