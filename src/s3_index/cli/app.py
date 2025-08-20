import typer
from rich.console import Console
from rich.table import Table

from s3_index.db.operations import save_keys_to_db, search_keys, setup_database
from s3_index.s3.operations import list_s3_keys

app = typer.Typer(help="CLI tool to save and search S3 keys in SQLite")
console = Console()


@app.command()
def save(
    bucket: str | None = typer.Argument(
        None, help="S3 bucket name (if not provided, all accessible buckets will be used)"
    ),
    batch_size: int = typer.Option(
        100, "--batch-size", "-b", help="Number of keys to process in each batch"
    ),
):
    """Save all S3 keys to SQLite database"""
    console.print("[bold]Saving S3 keys to SQLite database...[/bold]")

    # Setup database
    db_path = setup_database()
    console.print(f"Using database at: {db_path}")

    # Process keys from S3 in batches
    total_count = 0
    batch_count = 0

    with console.status("[bold green]Processing S3 keys...") as status:
        # Get keys from S3 as batches and save them to the database
        for batch in list_s3_keys(bucket, batch_size=batch_size):
            batch_count += 1
            status.update(f"[bold green]Processing batch {batch_count}... ({len(batch)} keys)")

            # Save batch to SQLite
            saved_count, skipped_count = save_keys_to_db(batch)
            total_count += saved_count

            if skipped_count > 0:
                console.print(
                    f"Batch {batch_count}: Saved {saved_count} keys, "
                    f"skipped {skipped_count} existing keys "
                    f"(Total saved: {total_count})"
                )
            else:
                console.print(
                    f"Batch {batch_count}: Saved {saved_count} keys (Total: {total_count})"
                )

    console.print(
        f"[green]Successfully saved {total_count} keys to database in {batch_count} batches[/green]"
    )


@app.command()
def search(query: str = typer.Argument(..., help="Text to search for in S3 keys")):
    """Search for S3 keys in the SQLite database"""
    console.print(f"[bold]Searching for keys containing: [cyan]{query}[/cyan][/bold]")

    # Search in SQLite
    results = search_keys(query)

    # Display results
    if not results:
        console.print("[yellow]No results found[/yellow]")
        return

    table = Table(show_header=True, header_style="bold")
    table.add_column("Bucket")
    table.add_column("Key")
    table.add_column("Last Modified")

    for row in results:
        table.add_row(row[0], row[1], row[2])

    console.print(f"Found [green]{len(results)}[/green] results:")
    console.print(table)
