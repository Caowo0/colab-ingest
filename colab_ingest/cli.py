"""CLI interface for colab-ingest using Typer.

This module provides the main entry point for the colab-ingest tool,
with commands for running the pipeline, checking status, resetting tasks,
cleaning up, and verifying dependencies.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

from .core.pipeline import Pipeline, PipelineConfig
from .core.state import StateDB, Task, TaskStatus
from .utils.logging import setup_logging

# Load .env file if present
load_dotenv()

app = typer.Typer(
    name="colab-ingest",
    help="Download files from Pixeldrain, BuzzHeavier, Bunkr and upload to Google Drive.",
    add_completion=False,
)

console = Console()


def _validate_links_file(path: Path) -> None:
    """Validate that the links file exists and is readable.
    
    Args:
        path: Path to the links file.
        
    Raises:
        typer.Exit: If validation fails.
    """
    if not path.exists():
        console.print(f"[red]Error:[/red] Links file not found: {path}")
        raise typer.Exit(1)
    if not path.is_file():
        console.print(f"[red]Error:[/red] Not a file: {path}")
        raise typer.Exit(1)


def _build_drive_path(drive_dest: str) -> Path:
    """Build the full Google Drive path from user input.
    
    Handles formats like:
    - "MyDrive/Uploads" -> /content/drive/MyDrive/Uploads
    - "/content/drive/MyDrive/Uploads" -> unchanged
    - "My Drive/Uploads" -> /content/drive/My Drive/Uploads
    
    Args:
        drive_dest: User-provided drive destination.
        
    Returns:
        Full path to the drive destination.
    """
    # If already absolute path starting with /content/drive, use as-is
    if drive_dest.startswith("/content/drive"):
        return Path(drive_dest)
    
    # Strip leading slash if present
    drive_dest = drive_dest.lstrip("/")
    
    return Path("/content/drive") / drive_dest


def _truncate_url(url: str, max_length: int = 50) -> str:
    """Truncate a URL for display.
    
    Args:
        url: The URL to truncate.
        max_length: Maximum length of the result.
        
    Returns:
        Truncated URL with ellipsis if needed.
    """
    if len(url) <= max_length:
        return url
    return url[: max_length - 3] + "..."


def _format_status(status: TaskStatus) -> str:
    """Format status with color for rich output.
    
    Args:
        status: The task status.
        
    Returns:
        Formatted status string with color markup.
    """
    color_map = {
        TaskStatus.DONE: "green",
        TaskStatus.FAILED: "red",
        TaskStatus.PENDING: "white",
        TaskStatus.DOWNLOADING: "yellow",
        TaskStatus.EXTRACTING: "yellow",
        TaskStatus.UPLOADING: "yellow",
    }
    color = color_map.get(status, "white")
    return f"[{color}]{status.value}[/{color}]"


def _format_bytes(size_bytes: int) -> str:
    """Format bytes to human-readable size.
    
    Args:
        size_bytes: Size in bytes.
        
    Returns:
        Human-readable size string.
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def _get_dir_size(path: Path) -> int:
    """Calculate total size of a directory.
    
    Args:
        path: Directory path.
        
    Returns:
        Total size in bytes.
    """
    total = 0
    if path.exists() and path.is_dir():
        for item in path.rglob("*"):
            if item.is_file():
                try:
                    total += item.stat().st_size
                except OSError:
                    pass
    return total


@app.command()
def run(
    links: Path = typer.Option(
        ...,
        "--links",
        "-l",
        help="Path to file containing URLs (one per line)",
    ),
    drive_dest: str = typer.Option(
        ...,
        "--drive-dest",
        "-d",
        help="Destination on Google Drive (e.g., 'MyDrive/Uploads')",
    ),
    workdir: Path = typer.Option(
        Path("/content/work"),
        "--workdir",
        "-w",
        help="Working directory for downloads",
    ),
    concurrency: int = typer.Option(
        3,
        "--concurrency",
        "-c",
        help="Number of concurrent downloads",
    ),
    pixeldrain_api_key: Optional[str] = typer.Option(
        None,
        "--pixeldrain-api-key",
        envvar="PIXELDRAIN_API_KEY",
        help="Pixeldrain API key",
    ),
    max_retries: int = typer.Option(
        3,
        "--max-retries",
        help="Maximum retry attempts per task",
    ),
    retry_failed: bool = typer.Option(
        False,
        "--retry-failed",
        help="Retry previously failed tasks",
    ),
    keep_temp: bool = typer.Option(
        False,
        "--keep-temp",
        help="Keep temporary files (for debugging)",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be done without executing",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging",
    ),
) -> None:
    """
    Run the download → extract → upload pipeline.
    
    Example:
        colab-ingest run --links /path/to/links.txt --drive-dest "MyDrive/Uploads"
    """
    # Validate inputs
    _validate_links_file(links)
    
    # Build full drive path
    full_drive_path = _build_drive_path(drive_dest)
    
    # Check if drive is accessible (when not in dry-run mode)
    if not dry_run:
        drive_base = Path("/content/drive")
        if not drive_base.exists():
            console.print(
                "[yellow]Warning:[/yellow] Google Drive not mounted at /content/drive. "
                "Upload may fail."
            )
    
    # Setup logging
    logger = setup_logging(workdir, verbose=verbose)
    
    # Create pipeline config
    config = PipelineConfig(
        links_file=links,
        drive_dest=full_drive_path,
        workdir=workdir,
        concurrency=concurrency,
        pixeldrain_api_key=pixeldrain_api_key,
        max_retries=max_retries,
        retry_failed=retry_failed,
        keep_temp=keep_temp,
        dry_run=dry_run,
    )
    
    # Display configuration
    console.print("\n[bold cyan]Pipeline Configuration[/bold cyan]")
    console.print(f"  Links file:      {links}")
    console.print(f"  Drive dest:      {full_drive_path}")
    console.print(f"  Working dir:     {workdir}")
    console.print(f"  Concurrency:     {concurrency}")
    console.print(f"  Max retries:     {max_retries}")
    console.print(f"  Retry failed:    {retry_failed}")
    console.print(f"  Keep temp:       {keep_temp}")
    console.print(f"  Dry run:         {dry_run}")
    console.print(f"  Verbose:         {verbose}")
    if pixeldrain_api_key:
        masked_key = "*" * (len(pixeldrain_api_key) - 4) + pixeldrain_api_key[-4:]
        console.print(f"  Pixeldrain key:  {masked_key}")
    console.print()
    
    # Run pipeline
    try:
        pipeline = Pipeline(config, logger=logger)
        stats = pipeline.run()
        
        # Display summary
        console.print("\n[bold cyan]Pipeline Summary[/bold cyan]")
        console.print(f"  Total tasks:   {stats.total_tasks}")
        console.print(f"  Completed:     [green]{stats.completed}[/green]")
        console.print(f"  Failed:        [red]{stats.failed}[/red]")
        console.print(f"  Skipped:       {stats.skipped}")
        console.print(f"  Downloaded:    {_format_bytes(stats.bytes_downloaded)}")
        console.print(f"  Uploaded:      {_format_bytes(stats.bytes_uploaded)}")
        console.print(f"  Duration:      {stats.duration_seconds():.1f}s")
        console.print()
        
        # Exit with appropriate code
        if stats.failed > 0:
            console.print("[yellow]Some tasks failed. Use 'status' to see details.[/yellow]")
            raise typer.Exit(1)
        else:
            console.print("[green]All tasks completed successfully![/green]")
            raise typer.Exit(0)
            
    except KeyboardInterrupt:
        console.print("\n[yellow]Pipeline interrupted by user.[/yellow]")
        raise typer.Exit(130)
    except Exception as e:
        console.print(f"[red]Pipeline error:[/red] {e}")
        logger.exception("Pipeline failed with exception")
        raise typer.Exit(1)


@app.command()
def status(
    workdir: Path = typer.Option(
        Path("/content/work"),
        "--workdir",
        "-w",
        help="Working directory",
    ),
) -> None:
    """
    Show status of all tasks in the state database.
    """
    state_db_path = workdir / "state.db"
    
    if not state_db_path.exists():
        console.print(f"[yellow]No state database found at {state_db_path}[/yellow]")
        console.print("Run the pipeline first to create tasks.")
        raise typer.Exit(0)
    
    db = StateDB(state_db_path)
    db.init_db()
    
    tasks = db.get_all_tasks()
    
    if not tasks:
        console.print("[yellow]No tasks found in the database.[/yellow]")
        raise typer.Exit(0)
    
    # Create table
    table = Table(title="Task Status")
    table.add_column("URL", style="cyan", no_wrap=False, max_width=50)
    table.add_column("Host", style="magenta")
    table.add_column("Status")
    table.add_column("Retries", justify="right")
    table.add_column("Updated")
    table.add_column("Error", style="red", max_width=30)
    
    for task in tasks:
        table.add_row(
            _truncate_url(task.url),
            task.host.value,
            _format_status(task.status),
            str(task.retries),
            task.updated_at.strftime("%Y-%m-%d %H:%M"),
            (task.error[:27] + "...") if task.error and len(task.error) > 30 else (task.error or ""),
        )
    
    console.print(table)
    
    # Show summary
    stats = db.get_stats()
    console.print(f"\n[bold]Summary:[/bold] "
                  f"Total: {stats.get('total', 0)}, "
                  f"[green]Done: {stats.get('done', 0)}[/green], "
                  f"[red]Failed: {stats.get('failed', 0)}[/red], "
                  f"Pending: {stats.get('pending', 0)}")


@app.command()
def reset(
    workdir: Path = typer.Option(
        Path("/content/work"),
        "--workdir",
        "-w",
        help="Working directory",
    ),
    url: Optional[str] = typer.Option(
        None,
        "--url",
        help="Reset specific URL (otherwise reset all failed)",
    ),
    all_tasks: bool = typer.Option(
        False,
        "--all",
        help="Reset ALL tasks (dangerous!)",
    ),
) -> None:
    """
    Reset failed tasks to pending status for retry.
    """
    state_db_path = workdir / "state.db"
    
    if not state_db_path.exists():
        console.print(f"[red]Error:[/red] No state database found at {state_db_path}")
        raise typer.Exit(1)
    
    db = StateDB(state_db_path)
    db.init_db()
    
    if url:
        # Reset specific URL
        task = db.get_task_by_url(url)
        if not task:
            console.print(f"[red]Error:[/red] Task not found for URL: {url}")
            raise typer.Exit(1)
        
        db.reset_task(task.id)
        console.print(f"[green]Reset task:[/green] {_truncate_url(url)}")
        
    elif all_tasks:
        # Confirm before resetting all
        all_task_list = db.get_all_tasks()
        if not all_task_list:
            console.print("[yellow]No tasks to reset.[/yellow]")
            raise typer.Exit(0)
        
        confirm = typer.confirm(
            f"Reset ALL {len(all_task_list)} tasks to pending? This cannot be undone."
        )
        if not confirm:
            console.print("[yellow]Cancelled.[/yellow]")
            raise typer.Exit(0)
        
        reset_count = 0
        for task in all_task_list:
            db.reset_task(task.id)
            reset_count += 1
        
        console.print(f"[green]Reset {reset_count} task(s) to pending.[/green]")
        
    else:
        # Reset all failed tasks
        failed_tasks = db.get_tasks_by_status(TaskStatus.FAILED)
        
        if not failed_tasks:
            console.print("[yellow]No failed tasks to reset.[/yellow]")
            raise typer.Exit(0)
        
        reset_count = 0
        for task in failed_tasks:
            db.reset_task(task.id)
            reset_count += 1
        
        console.print(f"[green]Reset {reset_count} failed task(s) to pending.[/green]")


@app.command()
def clean(
    workdir: Path = typer.Option(
        Path("/content/work"),
        "--workdir",
        "-w",
        help="Working directory",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Force cleanup without confirmation",
    ),
) -> None:
    """
    Clean up temporary files and directories.
    
    Removes downloads/ and extracted/ directories.
    Keeps logs/ and state.db.
    """
    downloads_dir = workdir / "downloads"
    extracted_dir = workdir / "extracted"
    
    # Calculate sizes
    downloads_size = _get_dir_size(downloads_dir)
    extracted_size = _get_dir_size(extracted_dir)
    total_size = downloads_size + extracted_size
    
    if total_size == 0:
        console.print("[yellow]No temporary files to clean.[/yellow]")
        raise typer.Exit(0)
    
    console.print("[bold]Directories to clean:[/bold]")
    if downloads_dir.exists():
        console.print(f"  downloads/  ({_format_bytes(downloads_size)})")
    if extracted_dir.exists():
        console.print(f"  extracted/  ({_format_bytes(extracted_size)})")
    console.print(f"\n[bold]Total space to free:[/bold] {_format_bytes(total_size)}")
    
    console.print("\n[dim]Keeping: logs/, state.db[/dim]")
    
    if not force:
        confirm = typer.confirm("\nProceed with cleanup?")
        if not confirm:
            console.print("[yellow]Cancelled.[/yellow]")
            raise typer.Exit(0)
    
    # Perform cleanup
    cleaned_size = 0
    
    if downloads_dir.exists():
        try:
            shutil.rmtree(downloads_dir)
            cleaned_size += downloads_size
            console.print("[green]Removed downloads/[/green]")
        except Exception as e:
            console.print(f"[red]Failed to remove downloads/:[/red] {e}")
    
    if extracted_dir.exists():
        try:
            shutil.rmtree(extracted_dir)
            cleaned_size += extracted_size
            console.print("[green]Removed extracted/[/green]")
        except Exception as e:
            console.print(f"[red]Failed to remove extracted/:[/red] {e}")
    
    console.print(f"\n[bold green]Freed {_format_bytes(cleaned_size)}[/bold green]")


@app.command()
def check() -> None:
    """
    Check system dependencies and configuration.
    
    Verifies:
    - Python version
    - Required packages
    - Third-party downloader scripts
    - Extraction tools (unrar, 7z)
    - Google Drive mount
    """
    table = Table(title="System Check")
    table.add_column("Component", style="cyan")
    table.add_column("Status")
    table.add_column("Details")
    
    all_ok = True
    
    # Python version
    py_version = sys.version_info
    py_ok = py_version >= (3, 9)
    table.add_row(
        "Python",
        "[green]OK[/green]" if py_ok else "[red]FAIL[/red]",
        f"{py_version.major}.{py_version.minor}.{py_version.micro}",
    )
    if not py_ok:
        all_ok = False
    
    # Required packages
    packages = ["typer", "rich", "python-dotenv", "httpx"]
    for pkg in packages:
        try:
            __import__(pkg.replace("-", "_"))
            table.add_row(f"Package: {pkg}", "[green]OK[/green]", "Installed")
        except ImportError:
            table.add_row(f"Package: {pkg}", "[red]FAIL[/red]", "Not installed")
            all_ok = False
    
    # Third-party downloader scripts
    third_party_dir = Path(__file__).parent.parent / "third_party"
    bunkr_script = third_party_dir / "BunkrDownloader" / "bunkr_downloader.py"
    buzz_script = third_party_dir / "buzzheavier-downloader" / "buzzheavier_downloader.py"
    
    if bunkr_script.exists():
        table.add_row("BunkrDownloader", "[green]OK[/green]", str(bunkr_script))
    else:
        table.add_row(
            "BunkrDownloader",
            "[yellow]WARN[/yellow]",
            "Not found - Bunkr downloads won't work",
        )
    
    if buzz_script.exists():
        table.add_row("BuzzHeavier", "[green]OK[/green]", str(buzz_script))
    else:
        table.add_row(
            "BuzzHeavier",
            "[yellow]WARN[/yellow]",
            "Not found - BuzzHeavier downloads won't work",
        )
    
    # Extraction tools
    unrar_path = shutil.which("unrar")
    if unrar_path:
        table.add_row("unrar", "[green]OK[/green]", unrar_path)
    else:
        table.add_row("unrar", "[yellow]WARN[/yellow]", "Not found - RAR extraction limited")
    
    sevenz_path = shutil.which("7z") or shutil.which("7za")
    if sevenz_path:
        table.add_row("7-Zip", "[green]OK[/green]", sevenz_path)
    else:
        table.add_row("7-Zip", "[yellow]WARN[/yellow]", "Not found - 7z extraction limited")
    
    # Google Drive mount
    drive_path = Path("/content/drive")
    if drive_path.exists() and drive_path.is_dir():
        # Check if MyDrive exists
        mydrive = drive_path / "MyDrive"
        if mydrive.exists():
            table.add_row("Google Drive", "[green]OK[/green]", "Mounted with MyDrive")
        else:
            table.add_row(
                "Google Drive",
                "[yellow]WARN[/yellow]",
                "Mounted but MyDrive not found",
            )
    else:
        table.add_row(
            "Google Drive",
            "[red]FAIL[/red]",
            "Not mounted at /content/drive",
        )
        all_ok = False
    
    # Environment variables
    pixeldrain_key = os.environ.get("PIXELDRAIN_API_KEY")
    if pixeldrain_key:
        masked = "*" * (len(pixeldrain_key) - 4) + pixeldrain_key[-4:]
        table.add_row("PIXELDRAIN_API_KEY", "[green]OK[/green]", f"Set ({masked})")
    else:
        table.add_row(
            "PIXELDRAIN_API_KEY",
            "[yellow]WARN[/yellow]",
            "Not set - Pixeldrain downloads won't work",
        )
    
    console.print(table)
    
    if all_ok:
        console.print("\n[bold green]All checks passed![/bold green]")
    else:
        console.print("\n[bold yellow]Some checks failed or have warnings.[/bold yellow]")
        console.print("See details above for more information.")


def main() -> None:
    """Main entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
