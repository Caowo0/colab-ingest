"""Main pipeline orchestrator for download → extract → upload workflow.

This module provides the Pipeline class that coordinates downloads, extraction,
and uploads for multiple URLs with concurrency support, progress tracking,
and graceful shutdown handling.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from rich.console import Console
from rich.progress import (
    Progress,
    TaskID,
    TextColumn,
    BarColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
    SpinnerColumn,
)

from ..core.state import StateDB, Task, TaskStatus
from ..downloaders.bunkr_adapter import BunkrDownloaderAdapter, BunkrDownloadResult
from ..downloaders.buzzheavier_adapter import (
    BuzzHeavierDownloaderAdapter,
    BuzzHeavierDownloadResult,
)
from ..downloaders.pixeldrain import PixeldrainDownloader, DownloadResult
from ..utils.extract import extract_archive, ExtractionResult
from ..utils.logging import setup_logging, TaskLogAdapter
from ..utils.paths import WorkdirManager
from ..utils.upload import upload_to_drive, UploadResult
from ..utils.url_detect import HostType, parse_links_file


@dataclass
class PipelineConfig:
    """Configuration for the pipeline.

    Attributes:
        links_file: Path to the file containing URLs to process.
        drive_dest: Destination path on Google Drive (e.g., /content/drive/MyDrive/Uploads).
        workdir: Working directory for downloads, extraction, and state.
        concurrency: Number of concurrent workers (default 3).
        pixeldrain_api_key: Optional API key for Pixeldrain authentication.
        max_retries: Maximum retry attempts for failed operations (default 3).
        retry_failed: If True, retry previously failed tasks (default False).
        keep_temp: If True, keep temporary files after upload (default False).
        dry_run: If True, log actions without executing (default False).
    """

    links_file: Path
    drive_dest: Path
    workdir: Path
    concurrency: int = 3
    pixeldrain_api_key: Optional[str] = None
    max_retries: int = 3
    retry_failed: bool = False
    keep_temp: bool = False
    dry_run: bool = False


@dataclass
class PipelineStats:
    """Statistics for pipeline execution.

    Attributes:
        total_tasks: Total number of tasks to process.
        completed: Number of successfully completed tasks.
        failed: Number of failed tasks.
        skipped: Number of skipped tasks (already done).
        bytes_downloaded: Total bytes downloaded across all tasks.
        bytes_uploaded: Total bytes uploaded to Drive.
        start_time: Pipeline start timestamp.
        end_time: Pipeline end timestamp (None if still running).
    """

    total_tasks: int = 0
    completed: int = 0
    failed: int = 0
    skipped: int = 0
    bytes_downloaded: int = 0
    bytes_uploaded: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None

    def duration_seconds(self) -> float:
        """Calculate the duration of the pipeline execution.

        Returns:
            Duration in seconds.
        """
        end = self.end_time or datetime.now()
        return (end - self.start_time).total_seconds()

    def summary(self) -> str:
        """Generate a summary string of the pipeline execution.

        Returns:
            Human-readable summary of statistics.
        """
        duration = self.duration_seconds()
        return (
            f"Pipeline completed in {duration:.1f}s: "
            f"{self.completed} completed, {self.failed} failed, {self.skipped} skipped. "
            f"Downloaded: {self.bytes_downloaded:,} bytes, "
            f"Uploaded: {self.bytes_uploaded:,} bytes"
        )


class Pipeline:
    """Main orchestrator for download → extract → upload workflow.

    Features:
    - Concurrent task execution with configurable workers
    - Per-task cleanup after upload
    - Resume support via state database
    - Progress tracking with rich console
    - Graceful shutdown on interrupt

    Attributes:
        config: Pipeline configuration.
        logger: Logger instance for the pipeline.
    """

    def __init__(
        self,
        config: PipelineConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize the pipeline.

        Args:
            config: Pipeline configuration object.
            logger: Optional logger. If None, sets up logging automatically.
        """
        self.config = config
        self._workdir_manager = WorkdirManager(config.workdir)
        self._workdir_manager.ensure_dirs()

        # Setup logging
        if logger is None:
            self.logger = setup_logging(config.workdir, verbose=True)
        else:
            self.logger = logger

        # Initialize state database
        self._state_db = StateDB(self._workdir_manager.state_db_path)
        self._state_db.init_db()

        # Rich console for progress display
        self._console = Console()

        # Shutdown handling
        self._shutdown_requested = False
        self._shutdown_lock = threading.Lock()
        self._active_futures: Dict[str, Future] = {}

        # Statistics tracking
        self._stats = PipelineStats()
        self._stats_lock = threading.Lock()

        # Progress tracking
        self._progress: Optional[Progress] = None
        self._overall_task_id: Optional[TaskID] = None
        self._task_progress_ids: Dict[str, TaskID] = {}

        self.logger.info(f"Pipeline initialized with config: {config}")

    def run(self) -> PipelineStats:
        """Execute the full pipeline synchronously.

        Returns:
            PipelineStats with execution statistics.
        """
        return asyncio.run(self.run_async())

    async def run_async(self) -> PipelineStats:
        """Async version of run for better concurrency.

        Returns:
            PipelineStats with execution statistics.
        """
        self._stats = PipelineStats(start_time=datetime.now())

        # Register signal handlers for graceful shutdown
        self._register_signal_handlers()

        try:
            # Load and prepare tasks
            tasks = self._load_tasks()
            self._stats.total_tasks = len(tasks)

            if not tasks:
                self.logger.info("No tasks to process")
                self._stats.end_time = datetime.now()
                return self._stats

            self.logger.info(f"Loaded {len(tasks)} task(s) to process")

            if self.config.dry_run:
                self._dry_run_tasks(tasks)
                self._stats.end_time = datetime.now()
                return self._stats

            # Process tasks with concurrency
            await self._process_tasks_concurrent(tasks)

        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        finally:
            self._stats.end_time = datetime.now()
            self.logger.info(self._stats.summary())

        return self._stats

    def _load_tasks(self) -> List[Task]:
        """Parse links file and create/update tasks in state DB.

        Returns:
            List of tasks that need processing.
        """
        self.logger.info(f"Loading tasks from: {self.config.links_file}")

        # Parse links file
        try:
            parsed_links = parse_links_file(self.config.links_file)
        except FileNotFoundError as e:
            self.logger.error(f"Links file not found: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error parsing links file: {e}")
            return []

        tasks_to_process: List[Task] = []

        for original_url, host_type, extracted_id in parsed_links:
            # Skip unknown hosts
            if host_type == HostType.UNKNOWN:
                self.logger.warning(f"Skipping unknown host: {original_url}")
                continue

            # Check if task exists in state DB
            existing_task = self._state_db.get_task_by_url(original_url)

            if existing_task:
                # Handle existing task based on status
                if existing_task.status == TaskStatus.DONE:
                    self.logger.debug(f"Skipping completed task: {original_url}")
                    with self._stats_lock:
                        self._stats.skipped += 1
                    continue

                if existing_task.status == TaskStatus.FAILED:
                    if self.config.retry_failed:
                        self.logger.info(f"Retrying failed task: {original_url}")
                        self._state_db.reset_task(existing_task.id)
                        self._state_db.increment_retry(existing_task.id)
                        # Refresh task from DB
                        existing_task = self._state_db.get_task_by_id(existing_task.id)
                        if existing_task:
                            tasks_to_process.append(existing_task)
                    else:
                        self.logger.debug(f"Skipping failed task (retry_failed=False): {original_url}")
                        with self._stats_lock:
                            self._stats.skipped += 1
                    continue

                # Task is in progress (PENDING, DOWNLOADING, EXTRACTING, UPLOADING)
                # Add to processing list
                tasks_to_process.append(existing_task)
            else:
                # Create new task
                task = self._state_db.create_task(original_url, host_type)
                self.logger.debug(f"Created new task: {task.id} for {original_url}")
                tasks_to_process.append(task)

        return tasks_to_process

    def _dry_run_tasks(self, tasks: List[Task]) -> None:
        """Log what would be done without executing.

        Args:
            tasks: List of tasks to describe.
        """
        self.logger.info("=== DRY RUN MODE ===")
        for task in tasks:
            self.logger.info(
                f"Would process: {task.url} (host: {task.host.value}, status: {task.status.value})"
            )
            self.logger.info(f"  - Download to: {self._workdir_manager.get_task_download_dir(task.id)}")
            self.logger.info(f"  - Extract to: {self._workdir_manager.get_task_extract_dir(task.id)}")
            self.logger.info(f"  - Upload to: {self.config.drive_dest / task.id}")
        self.logger.info(f"=== Would process {len(tasks)} task(s) ===")

    async def _process_tasks_concurrent(self, tasks: List[Task]) -> None:
        """Process tasks concurrently with progress tracking.

        Args:
            tasks: List of tasks to process.
        """
        # Create progress display
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=self._console,
            transient=False,
        ) as progress:
            self._progress = progress

            # Create overall progress task
            self._overall_task_id = progress.add_task(
                "[cyan]Overall Progress",
                total=len(tasks),
            )

            # Use ThreadPoolExecutor for concurrent processing
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor(max_workers=self.config.concurrency) as executor:
                # Submit all tasks
                futures: List[Future] = []
                for task in tasks:
                    if self._shutdown_requested:
                        break

                    future = executor.submit(self._process_task, task)
                    futures.append(future)
                    with self._shutdown_lock:
                        self._active_futures[task.id] = future

                # Wait for all tasks to complete
                for future in futures:
                    if self._shutdown_requested:
                        self.logger.info("Shutdown requested, waiting for active tasks...")
                        break

                    try:
                        # Use asyncio to wait without blocking
                        await loop.run_in_executor(None, future.result)
                    except Exception as e:
                        self.logger.error(f"Task execution error: {e}")

            self._progress = None

    def _process_task(self, task: Task) -> bool:
        """Process single task: download → extract → upload → cleanup.

        Args:
            task: The task to process.

        Returns:
            True if task completed successfully, False otherwise.
        """
        task_logger = TaskLogAdapter(self.logger, task.id)
        task_logger.info(f"Starting task: {task.url}")

        # Create progress task for this specific task
        progress_task_id: Optional[TaskID] = None
        if self._progress:
            progress_task_id = self._progress.add_task(
                f"[yellow]{task.id[:8]}...",
                total=None,  # Indeterminate initially
            )
            self._task_progress_ids[task.id] = progress_task_id

        try:
            # Ensure task directories exist
            download_dir, extract_dir = self._workdir_manager.ensure_task_dirs(task.id)

            # Phase 1: Download
            success, downloaded_files = self._download_task(task, task_logger)
            if not success:
                self._mark_task_failed(task, "Download failed")
                return False

            if not downloaded_files:
                task_logger.warning("No files downloaded")
                self._mark_task_failed(task, "No files downloaded")
                return False

            # Phase 2: Extract
            success, files_to_upload = self._extract_task(task, downloaded_files, task_logger)
            if not success:
                self._mark_task_failed(task, "Extraction failed")
                return False

            # Phase 3: Upload
            success = self._upload_task(task, files_to_upload, task_logger)
            if not success:
                self._mark_task_failed(task, "Upload failed")
                return False

            # Phase 4: Cleanup
            if not self.config.keep_temp:
                self._cleanup_task(task, task_logger)

            # Mark task as complete
            self._state_db.update_status(task.id, TaskStatus.DONE)
            task_logger.info("Task completed successfully")

            with self._stats_lock:
                self._stats.completed += 1

            # Update overall progress
            if self._progress and self._overall_task_id is not None:
                self._progress.update(self._overall_task_id, advance=1)

            return True

        except Exception as e:
            task_logger.error(f"Unexpected error: {e}", exc_info=True)
            self._mark_task_failed(task, str(e))
            return False
        finally:
            # Remove task progress
            if self._progress and progress_task_id is not None:
                self._progress.remove_task(progress_task_id)
            if task.id in self._task_progress_ids:
                del self._task_progress_ids[task.id]
            if task.id in self._active_futures:
                with self._shutdown_lock:
                    del self._active_futures[task.id]

    def _download_task(
        self,
        task: Task,
        task_logger: TaskLogAdapter,
    ) -> Tuple[bool, List[Path]]:
        """Execute download based on host type.

        Args:
            task: The task to download.
            task_logger: Logger with task context.

        Returns:
            Tuple of (success, list of downloaded file paths).
        """
        task_logger.info(f"Downloading from {task.host.value}")
        self._state_db.update_status(task.id, TaskStatus.DOWNLOADING)

        download_dir = self._workdir_manager.get_task_download_dir(task.id)
        downloaded_files: List[Path] = []

        # Create progress callback
        progress_callback = self._create_progress_callback(task, "Downloading")

        try:
            if task.host == HostType.PIXELDRAIN:
                downloaded_files = self._download_pixeldrain(
                    task, download_dir, progress_callback, task_logger
                )
            elif task.host == HostType.BUNKR:
                downloaded_files = self._download_bunkr(
                    task, download_dir, task_logger
                )
            elif task.host == HostType.BUZZHEAVIER:
                downloaded_files = self._download_buzzheavier(
                    task, download_dir, task_logger
                )
            else:
                task_logger.error(f"Unsupported host type: {task.host}")
                return False, []

            if downloaded_files:
                with self._stats_lock:
                    for f in downloaded_files:
                        if f.exists():
                            self._stats.bytes_downloaded += f.stat().st_size

                task_logger.info(f"Downloaded {len(downloaded_files)} file(s)")
                return True, downloaded_files
            else:
                return False, []

        except Exception as e:
            task_logger.error(f"Download error: {e}")
            self._state_db.update_status(task.id, TaskStatus.FAILED, error=str(e))
            return False, []

    def _download_pixeldrain(
        self,
        task: Task,
        download_dir: Path,
        progress_callback: Optional[Callable],
        task_logger: TaskLogAdapter,
    ) -> List[Path]:
        """Download from Pixeldrain.

        Args:
            task: The task to download.
            download_dir: Directory to save files.
            progress_callback: Progress callback function.
            task_logger: Logger with task context.

        Returns:
            List of downloaded file paths.
        """
        if not self.config.pixeldrain_api_key:
            task_logger.error("Pixeldrain API key not configured")
            return []

        # Extract file ID from URL
        from ..utils.url_detect import extract_pixeldrain_id
        file_id = extract_pixeldrain_id(task.url)
        if not file_id:
            task_logger.error(f"Could not extract Pixeldrain ID from: {task.url}")
            return []

        downloader = PixeldrainDownloader(
            api_key=self.config.pixeldrain_api_key,
            download_dir=download_dir,
            max_retries=self.config.max_retries,
            logger=self.logger,
        )

        # Create wrapper callback for pixeldrain's signature
        def pd_progress_callback(downloaded: int, total: int, speed: float, eta: float) -> None:
            if progress_callback:
                progress_callback(downloaded, total, speed)

        result: DownloadResult = downloader.download(file_id, pd_progress_callback)

        if result.success and result.file_path:
            return [result.file_path]
        else:
            task_logger.error(f"Pixeldrain download failed: {result.error}")
            return []

    def _download_bunkr(
        self,
        task: Task,
        download_dir: Path,
        task_logger: TaskLogAdapter,
    ) -> List[Path]:
        """Download from Bunkr.

        Args:
            task: The task to download.
            download_dir: Directory to save files.
            task_logger: Logger with task context.

        Returns:
            List of downloaded file paths.
        """
        downloader = BunkrDownloaderAdapter(
            download_dir=download_dir,
            max_retries=self.config.max_retries,
            logger=self.logger,
        )

        # Verify installation
        if not downloader.verify_installation():
            task_logger.error("BunkrDownloader not installed or not found")
            return []

        def output_callback(line: str) -> None:
            task_logger.debug(f"[bunkr] {line}")

        result: BunkrDownloadResult = downloader.download(task.url, output_callback)

        if result.success:
            return result.downloaded_files
        else:
            task_logger.error(f"Bunkr download failed: {result.error}")
            return []

    def _download_buzzheavier(
        self,
        task: Task,
        download_dir: Path,
        task_logger: TaskLogAdapter,
    ) -> List[Path]:
        """Download from BuzzHeavier.

        Args:
            task: The task to download.
            download_dir: Directory to save files.
            task_logger: Logger with task context.

        Returns:
            List of downloaded file paths.
        """
        downloader = BuzzHeavierDownloaderAdapter(
            download_dir=download_dir,
            logger=self.logger,
        )

        # Verify installation
        if not downloader.verify_installation():
            task_logger.error("BuzzHeavier downloader not installed or not found")
            return []

        # Extract file ID from URL
        from ..utils.url_detect import extract_buzzheavier_id
        file_id = extract_buzzheavier_id(task.url)
        if not file_id:
            # Fall back to using the full URL
            file_id = task.url

        def output_callback(line: str) -> None:
            task_logger.debug(f"[buzzheavier] {line}")

        result: BuzzHeavierDownloadResult = downloader.download(file_id, output_callback)

        if result.success:
            return result.downloaded_files
        else:
            task_logger.error(f"BuzzHeavier download failed: {result.error}")
            return []

    def _extract_task(
        self,
        task: Task,
        downloaded_files: List[Path],
        task_logger: TaskLogAdapter,
    ) -> Tuple[bool, List[Path]]:
        """Extract archives from downloaded files.

        Args:
            task: The task being processed.
            downloaded_files: List of downloaded files.
            task_logger: Logger with task context.

        Returns:
            Tuple of (success, list of files/directories to upload).
        """
        task_logger.info(f"Extracting {len(downloaded_files)} file(s)")
        self._state_db.update_status(task.id, TaskStatus.EXTRACTING)

        extract_dir = self._workdir_manager.get_task_extract_dir(task.id)
        files_to_upload: List[Path] = []
        all_success = True

        for file_path in downloaded_files:
            if not file_path.exists():
                task_logger.warning(f"File not found: {file_path}")
                continue

            # Extract (or copy non-archives)
            delete_after = not self.config.keep_temp
            result: ExtractionResult = extract_archive(
                file_path,
                extract_dir,
                delete_after=delete_after,
                logger=self.logger,
            )

            if result.success:
                # Add extracted files to upload list
                if result.extracted_files:
                    files_to_upload.extend(result.extracted_files)
                    task_logger.info(f"Extracted {len(result.extracted_files)} file(s) from {file_path.name}")
                else:
                    # No extracted files but success - add the extraction directory
                    files_to_upload.append(result.extracted_path)
            else:
                task_logger.error(f"Extraction failed for {file_path.name}: {result.error}")
                all_success = False

        # If we have files to upload, consider it a success even if some failed
        if files_to_upload:
            return True, files_to_upload
        else:
            return all_success, files_to_upload

    def _upload_task(
        self,
        task: Task,
        files_to_upload: List[Path],
        task_logger: TaskLogAdapter,
    ) -> bool:
        """Upload files to Google Drive.

        Args:
            task: The task being processed.
            files_to_upload: List of files/directories to upload.
            task_logger: Logger with task context.

        Returns:
            True if all uploads succeeded, False otherwise.
        """
        task_logger.info(f"Uploading {len(files_to_upload)} item(s) to Drive")
        self._state_db.update_status(task.id, TaskStatus.UPLOADING)

        # Create task-specific destination directory
        task_dest = self.config.drive_dest / task.id
        all_success = True
        output_paths: List[str] = []

        # Create progress callback
        progress_callback = self._create_progress_callback(task, "Uploading")

        for file_path in files_to_upload:
            if not file_path.exists():
                task_logger.warning(f"File not found for upload: {file_path}")
                continue

            delete_after = not self.config.keep_temp
            result: UploadResult = upload_to_drive(
                source=file_path,
                drive_dest=task_dest,
                delete_after=delete_after,
                progress_callback=progress_callback,
                logger=self.logger,
            )

            if result.success:
                output_paths.append(str(result.dest_path / file_path.name))
                with self._stats_lock:
                    self._stats.bytes_uploaded += result.bytes_copied
                task_logger.info(f"Uploaded: {file_path.name}")
            else:
                task_logger.error(f"Upload failed for {file_path.name}: {result.error}")
                all_success = False

        # Record output paths in state DB
        if output_paths:
            self._state_db.set_output_paths(task.id, output_paths)

        return all_success or bool(output_paths)

    def _cleanup_task(self, task: Task, task_logger: TaskLogAdapter) -> None:
        """Remove task's temporary directories.

        Args:
            task: The task to clean up.
            task_logger: Logger with task context.
        """
        task_logger.info("Cleaning up temporary files")
        try:
            self._workdir_manager.cleanup_task(task.id)
            task_logger.debug("Cleanup complete")
        except Exception as e:
            task_logger.warning(f"Cleanup failed: {e}")

    def _mark_task_failed(self, task: Task, error: str) -> None:
        """Mark a task as failed and update statistics.

        Args:
            task: The task that failed.
            error: Error message.
        """
        self._state_db.update_status(task.id, TaskStatus.FAILED, error=error)
        with self._stats_lock:
            self._stats.failed += 1

        # Update overall progress
        if self._progress and self._overall_task_id is not None:
            self._progress.update(self._overall_task_id, advance=1)

    def _create_progress_callback(
        self,
        task: Task,
        operation: str,
    ) -> Callable[[int, int, float], None]:
        """Create progress callback for rich progress display.

        Args:
            task: The task being tracked.
            operation: Description of the operation (e.g., "Downloading").

        Returns:
            Callback function for progress updates.
        """
        def callback(bytes_done: int, bytes_total: int, speed_bps: float) -> None:
            if self._progress and task.id in self._task_progress_ids:
                task_id = self._task_progress_ids[task.id]
                self._progress.update(
                    task_id,
                    description=f"[yellow]{operation} {task.id[:8]}...",
                    completed=bytes_done,
                    total=bytes_total,
                )

        return callback

    def _register_signal_handlers(self) -> None:
        """Register signal handlers for graceful shutdown."""
        # Only register on main thread
        try:
            if threading.current_thread() is threading.main_thread():
                signal.signal(signal.SIGINT, self._handle_shutdown)
                signal.signal(signal.SIGTERM, self._handle_shutdown)
                self.logger.debug("Signal handlers registered for graceful shutdown")
        except ValueError:
            # Signal handling may not work in some contexts (e.g., threads)
            self.logger.debug("Could not register signal handlers (not main thread)")

    def _handle_shutdown(self, signum: int, frame: object) -> None:
        """Handle graceful shutdown on SIGINT/SIGTERM.

        Args:
            signum: Signal number.
            frame: Current stack frame.
        """
        signal_name = signal.Signals(signum).name
        self.logger.warning(f"Received {signal_name}, initiating graceful shutdown...")

        with self._shutdown_lock:
            self._shutdown_requested = True

        # Log current state
        self.logger.info(f"Waiting for {len(self._active_futures)} active task(s) to complete...")

        # Don't forcefully exit - let the main loop handle it
        # The ThreadPoolExecutor will complete current tasks

    def __repr__(self) -> str:
        """Return string representation of Pipeline.

        Returns:
            String representation showing configuration.
        """
        return (
            f"Pipeline("
            f"links_file={self.config.links_file!r}, "
            f"drive_dest={self.config.drive_dest!r}, "
            f"concurrency={self.config.concurrency})"
        )
