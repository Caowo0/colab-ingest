#!/usr/bin/env python3
"""
Example: Using colab_ingest programmatically.

This script demonstrates how to use the colab_ingest library
directly in Python code instead of via CLI.

This is useful when you want to:
- Integrate downloading into a larger workflow
- Customize behavior beyond CLI options
- Add custom callbacks for progress tracking
- Process results programmatically
"""

from pathlib import Path
import logging
import tempfile

# Import the core pipeline components
from colab_ingest.core.pipeline import Pipeline, PipelineConfig, PipelineStats
from colab_ingest.core.state import StateDB, Task, TaskStatus
from colab_ingest.utils.paths import WorkdirManager
from colab_ingest.utils.url_detect import (
    HostType,
    detect_host,
    parse_links_file,
    validate_url,
)
from colab_ingest.utils.extract import extract_archive, is_archive


def example_basic_pipeline():
    """
    Example 1: Basic Pipeline Usage
    
    Shows how to set up and run the pipeline with minimal configuration.
    """
    print("\n=== Example 1: Basic Pipeline ===\n")
    
    # Configuration - customize these paths for your environment
    config = PipelineConfig(
        links_file=Path("./links.txt"),  # Your links file
        drive_dest=Path("/content/drive/MyDrive/Downloads"),  # Google Drive destination
        workdir=Path("./workdir"),  # Working directory for downloads
        concurrency=3,  # Number of parallel downloads
        pixeldrain_api_key="YOUR_API_KEY_HERE",  # Optional: for Pixeldrain
    )
    
    # Create and run the pipeline
    pipeline = Pipeline(config)
    stats = pipeline.run()
    
    # Access results
    print(f"Completed: {stats.completed}")
    print(f"Failed: {stats.failed}")
    print(f"Skipped: {stats.skipped}")
    print(f"Duration: {stats.duration_seconds():.1f} seconds")
    print(f"Downloaded: {stats.bytes_downloaded:,} bytes")
    print(f"Uploaded: {stats.bytes_uploaded:,} bytes")


def example_url_detection():
    """
    Example 2: URL Detection and Parsing
    
    Shows how to detect host types and extract IDs from URLs.
    """
    print("\n=== Example 2: URL Detection ===\n")
    
    test_urls = [
        "https://pixeldrain.com/u/abc12345",
        "https://buzzheavier.com/f/abc123def456",
        "https://bzzhr.co/xyz789abc012",
        "https://bunkr.si/a/my-album-name",
        "https://unknown-site.com/file",
    ]
    
    for url in test_urls:
        host_type = detect_host(url)
        is_valid, error = validate_url(url)
        
        print(f"URL: {url}")
        print(f"  Host Type: {host_type.value}")
        print(f"  Valid: {is_valid}")
        if error:
            print(f"  Error: {error}")
        print()


def example_parse_links_file():
    """
    Example 3: Parse Links File
    
    Shows how to read and parse a links file programmatically.
    """
    print("\n=== Example 3: Parse Links File ===\n")
    
    # Create a sample links file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write("# Sample links file\n")
        f.write("https://pixeldrain.com/u/abc12345\n")
        f.write("https://buzzheavier.com/f/abc123def456\n")
        f.write("\n")  # Empty line
        f.write("# Comment line\n")
        f.write("https://bunkr.si/a/my-album\n")
        temp_file = Path(f.name)
    
    try:
        # Parse the file
        links = parse_links_file(temp_file)
        
        print(f"Found {len(links)} valid links:\n")
        for original_url, host_type, extracted_id in links:
            print(f"  URL: {original_url}")
            print(f"  Host: {host_type.value}")
            print(f"  ID/Normalized: {extracted_id}")
            print()
    finally:
        # Cleanup
        temp_file.unlink()


def example_state_management():
    """
    Example 4: State Database Management
    
    Shows how to interact with the state database directly.
    """
    print("\n=== Example 4: State Management ===\n")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "state.db"
        
        # Initialize the database
        state_db = StateDB(db_path)
        state_db.init_db()
        
        # Create some tasks
        task1 = state_db.create_task(
            "https://pixeldrain.com/u/abc12345",
            HostType.PIXELDRAIN
        )
        task2 = state_db.create_task(
            "https://buzzheavier.com/f/def67890",
            HostType.BUZZHEAVIER
        )
        
        print(f"Created task: {task1.id[:8]}... for {task1.url}")
        print(f"Created task: {task2.id[:8]}... for {task2.url}")
        
        # Update task status
        state_db.update_status(task1.id, TaskStatus.DOWNLOADING)
        print(f"\nUpdated task1 status to: DOWNLOADING")
        
        state_db.update_status(task1.id, TaskStatus.DONE)
        print(f"Updated task1 status to: DONE")
        
        state_db.update_status(task2.id, TaskStatus.FAILED, error="Connection timeout")
        print(f"Updated task2 status to: FAILED")
        
        # Add output paths
        state_db.add_output_path(task1.id, "/content/drive/MyDrive/file1.zip")
        print(f"\nAdded output path to task1")
        
        # Get statistics
        stats = state_db.get_stats()
        print(f"\nDatabase Statistics:")
        print(f"  Total: {stats['total']}")
        print(f"  Pending: {stats['pending']}")
        print(f"  Done: {stats['done']}")
        print(f"  Failed: {stats['failed']}")
        
        # Query tasks
        failed_tasks = state_db.get_tasks_by_status(TaskStatus.FAILED)
        print(f"\nFailed tasks: {len(failed_tasks)}")
        for task in failed_tasks:
            print(f"  - {task.url}: {task.error}")


def example_workdir_manager():
    """
    Example 5: Working Directory Management
    
    Shows how to use WorkdirManager for file organization.
    """
    print("\n=== Example 5: WorkdirManager ===\n")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Initialize the manager
        manager = WorkdirManager(Path(tmpdir))
        manager.ensure_dirs()
        
        print(f"Workdir: {manager.workdir}")
        print(f"Downloads: {manager.downloads_dir}")
        print(f"Extracted: {manager.extracted_dir}")
        print(f"Logs: {manager.logs_dir}")
        print(f"State DB: {manager.state_db_path}")
        
        # Create task-specific directories
        task_id = "example-task-123"
        download_dir, extract_dir = manager.ensure_task_dirs(task_id)
        
        print(f"\nTask directories created:")
        print(f"  Download: {download_dir}")
        print(f"  Extract: {extract_dir}")
        
        # Create some test files
        (download_dir / "test_file.txt").write_text("test content")
        (extract_dir / "extracted.txt").write_text("extracted content")
        
        # Get task files
        files = manager.get_task_files(task_id, "downloads")
        print(f"\nFiles in download dir: {[f.name for f in files]}")
        
        # Get disk usage
        usage = manager.get_disk_usage()
        print(f"\nDisk usage:")
        print(f"  Downloads: {usage['downloads']} bytes")
        print(f"  Extracted: {usage['extracted']} bytes")
        print(f"  Total: {usage['total']} bytes")
        
        # Cleanup
        manager.cleanup_task(task_id)
        print(f"\nTask directories cleaned up")


def example_extraction():
    """
    Example 6: Archive Extraction
    
    Shows how to use the extraction utilities.
    """
    print("\n=== Example 6: Archive Extraction ===\n")
    
    import zipfile
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create a test ZIP file
        zip_path = tmpdir / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("file1.txt", "Content of file 1")
            zf.writestr("file2.txt", "Content of file 2")
            zf.writestr("subdir/file3.txt", "Content of file 3")
        
        print(f"Created test ZIP: {zip_path}")
        print(f"Is archive: {is_archive(zip_path)}")
        
        # Extract the archive
        extract_dir = tmpdir / "extracted"
        result = extract_archive(zip_path, extract_dir, delete_after=False)
        
        print(f"\nExtraction result:")
        print(f"  Success: {result.success}")
        print(f"  Archive type: {result.archive_type}")
        print(f"  Extracted files: {len(result.extracted_files)}")
        
        for f in result.extracted_files:
            print(f"    - {f.name}")
        
        if result.error:
            print(f"  Error: {result.error}")


def example_custom_pipeline():
    """
    Example 7: Custom Pipeline with Callbacks
    
    Shows how to create a customized pipeline workflow.
    """
    print("\n=== Example 7: Custom Pipeline Workflow ===\n")
    
    # This example shows the structure without actually running downloads
    
    print("Custom pipeline workflow structure:")
    print("""
    1. Parse links file
       - Filter by host type
       - Validate URLs
       - Skip already processed
    
    2. For each URL:
       a. Download phase
          - Select appropriate downloader
          - Track progress with callbacks
          - Handle retries on failure
       
       b. Extract phase
          - Detect archive type
          - Extract to task directory
          - Handle nested archives
       
       c. Upload phase
          - Copy to Google Drive
          - Track upload progress
          - Record output paths
       
       d. Cleanup phase
          - Remove temporary files
          - Update state database
    
    3. Generate summary report
       - Tasks completed/failed
       - Total bytes transferred
       - Duration statistics
    """)
    
    # Example of how you might customize the workflow:
    print("\nCustomization example - filtering by host:")
    
    sample_urls = [
        ("https://pixeldrain.com/u/abc123", HostType.PIXELDRAIN),
        ("https://buzzheavier.com/f/def456", HostType.BUZZHEAVIER),
        ("https://bunkr.si/a/album1", HostType.BUNKR),
        ("https://pixeldrain.com/u/ghi789", HostType.PIXELDRAIN),
    ]
    
    # Filter to only Pixeldrain URLs
    pixeldrain_only = [
        (url, host) for url, host in sample_urls 
        if host == HostType.PIXELDRAIN
    ]
    
    print(f"Total URLs: {len(sample_urls)}")
    print(f"Pixeldrain only: {len(pixeldrain_only)}")
    for url, _ in pixeldrain_only:
        print(f"  - {url}")


def example_dry_run():
    """
    Example 8: Dry Run Mode
    
    Shows how to preview what would happen without executing.
    """
    print("\n=== Example 8: Dry Run Mode ===\n")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create a sample links file
        links_file = tmpdir / "links.txt"
        links_file.write_text("""
https://pixeldrain.com/u/abc12345
https://buzzheavier.com/f/def67890
https://bunkr.si/a/test-album
""")
        
        # Configure with dry_run=True
        config = PipelineConfig(
            links_file=links_file,
            drive_dest=Path("/content/drive/MyDrive/Test"),
            workdir=tmpdir / "workdir",
            dry_run=True,  # No actual downloads!
        )
        
        print("Dry run configuration:")
        print(f"  Links file: {config.links_file}")
        print(f"  Drive dest: {config.drive_dest}")
        print(f"  Dry run: {config.dry_run}")
        print()
        print("In dry run mode, the pipeline will:")
        print("  - Parse the links file")
        print("  - Create task entries in the state database")
        print("  - Log what would be done")
        print("  - NOT actually download, extract, or upload anything")


def main():
    """Run all examples."""
    print("=" * 60)
    print("colab_ingest Library Usage Examples")
    print("=" * 60)
    
    # Run examples that don't require network/external resources
    example_url_detection()
    example_parse_links_file()
    example_state_management()
    example_workdir_manager()
    example_extraction()
    example_custom_pipeline()
    example_dry_run()
    
    # These examples require actual setup and would fail without proper config:
    # example_basic_pipeline()  # Requires actual links.txt and API key
    
    print("\n" + "=" * 60)
    print("Examples completed!")
    print("=" * 60)
    print("""
To run the actual pipeline:

1. Create a links.txt file with your URLs
2. Set up your Pixeldrain API key (if using Pixeldrain)
3. Mount Google Drive (in Colab)
4. Run:
   
   from colab_ingest.core.pipeline import Pipeline, PipelineConfig
   
   config = PipelineConfig(
       links_file=Path("links.txt"),
       drive_dest=Path("/content/drive/MyDrive/Downloads"),
       workdir=Path("./workdir"),
       pixeldrain_api_key="YOUR_KEY",
   )
   
   pipeline = Pipeline(config)
   stats = pipeline.run()
   print(stats.summary())
""")


if __name__ == "__main__":
    main()
