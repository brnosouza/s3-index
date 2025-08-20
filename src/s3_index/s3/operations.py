from collections.abc import Generator

import boto3
from rich.console import Console

console = Console()


def list_s3_keys(
    bucket_name: str | None = None, batch_size: int = 100
) -> Generator[list[dict[str, str]], None, None]:
    """List all keys in the specified S3 bucket or all buckets, yielding batches of keys.

    This generator function processes S3 keys in batches to minimize memory usage
    when dealing with large buckets. Each batch is yielded as soon as it reaches
    the specified size.

    Args:
        bucket_name: Optional name of a specific bucket to list keys from.
                    If None, all accessible buckets will be processed.
        batch_size: Number of keys to include in each batch (default: 100)

    Yields:
        list[dict[str, str]]: Batches of S3 key information, where each key is
                             represented as a dictionary with 'bucket', 'key',
                             and 'last_modified' fields.
    """
    s3_client = boto3.client("s3")

    buckets = [{"Name": bucket_name}] if bucket_name else s3_client.list_buckets()["Buckets"]

    for bucket in buckets:
        bucket_name = bucket["Name"]
        console.print(f"Listing keys from bucket: [bold]{bucket_name}[/bold]")

        batch = []
        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        batch.append(
                            {
                                "bucket": bucket_name,
                                "key": obj["Key"],
                                "last_modified": obj["LastModified"].isoformat(),
                            }
                        )

                        # When we reach the batch size, yield the batch and start a new one
                        if len(batch) >= batch_size:
                            yield batch
                            batch = []

            # Yield any remaining keys in the last batch
            if batch:
                yield batch

        except Exception as e:
            console.print(f"[red]Error accessing bucket {bucket_name}: {str(e)}[/red]")
            # Even if there's an error, yield any keys we've collected so far
            if batch:
                yield batch
