"""Upload utilities for copying files to Google Drive mount.

This module provides utilities for uploading files and directories to
Google Drive's mounted filesystem in Google Colab. It supports both
rsync-based uploads (preferred for reliability and progress) and
Python-based fallback using shutil.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Tuple


@dataclass
class UploadResult:
    """Result of an upload operation.

    Attributes:
        success: Whether the upload completed successfully.
        source_path: Path to the source file or directory.
        dest_path: Path to the destination on Drive.
        bytes_copied: Total bytes copied during the upload.
        error: Error message if upload failed, None otherwise.
    """

    success: bool
    source_path: Path
    dest_path: Path
    bytes_copied: int
    error: Optional[str] = None


def check_rsync_available() -> bool:
    """Check if rsync is installed and available.

    Returns:
        True if rsync is available, False otherwise.
    """
    try:
        result = subprocess.run(
            ["rsync", "--version"],
            capture_output=True,
            timeout=10,
        )
        return result.returncode == 0
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        return False


def parse_rsync_progress(line: str) -> Optional[Tuple[int, int, float]]:
    """Parse rsync --info=progress2 output for progress information.

    The progress2 format outputs lines like:
        1,234,567  50%   10.00MB/s    0:00:05

    Args:
        line: A line of rsync output.

    Returns:
        Tuple of (bytes_copied, total_bytes, speed_bps) if parseable,
        None otherwise.
    """
    # Pattern for rsync --info=progress2 output
    # Example: "  1,234,567  50%   10.00MB/s    0:00:05"
    # Or:      "     12,345 100%    5.00kB/s    0:00:01 (xfr#1, to-chk=0/1)"
    pattern = r"^\s*([\d,]+)\s+(\d+)%\s+([\d.]+)([kMGT]?)B/s"

    match = re.match(pattern, line)
    if not match:
        return None

    try:
        # Parse bytes copied (remove commas)
        bytes_copied = int(match.group(1).replace(",", ""))

        # Parse percentage to estimate total
        percentage = int(match.group(2))
        if percentage > 0:
            total_bytes = int(bytes_copied * 100 / percentage)
        else:
            total_bytes = 0

        # Parse speed
        speed_value = float(match.group(3))
        speed_unit = match.group(4)

        # Convert to bytes per second
        unit_multipliers = {
            "": 1,
            "k": 1024,
            "M": 1024 * 1024,
            "G": 1024 * 1024 * 1024,
            "T": 1024 * 1024 * 1024 * 1024,
        }
        speed_bps = speed_value * unit_multipliers.get(speed_unit, 1)

        return (bytes_copied, total_bytes, speed_bps)

    except (ValueError, ZeroDivisionError):
        return None


def _get_total_size(path: Path) -> int:
    """Calculate total size of a file or directory in bytes.

    Args:
        path: Path to file or directory.

    Returns:
        Total size in bytes.
    """
    if path.is_file():
        return path.stat().st_size

    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            try:
                total += item.stat().st_size
            except OSError:
                pass
    return total


def upload_with_rsync(
    source: Path,
    dest: Path,
    delete_source: bool,
    progress_callback: Optional[Callable[[int, int, float], None]],
    logger: Optional[logging.Logger],
) -> UploadResult:
    """Upload files using rsync for reliability and progress tracking.

    Uses rsync with --info=progress2 for overall progress information.
    If delete_source is True, uses --remove-source-files to clean up
    after successful transfer.

    Args:
        source: Path to source file or directory.
        dest: Path to destination on Drive.
        delete_source: If True, remove source files after successful upload.
        progress_callback: Optional callback for progress updates.
            Called with (bytes_copied, total_bytes, speed_bps).
        logger: Optional logger for progress messages.

    Returns:
        UploadResult with upload status and bytes copied.
    """
    log = logger or logging.getLogger(__name__)

    try:
        # Ensure destination directory exists
        dest.mkdir(parents=True, exist_ok=True)

        # Build rsync command
        cmd = [
            "rsync",
            "-a",  # Archive mode (preserves permissions, times, etc.)
            "--info=progress2",  # Show overall progress
            "--no-inc-recursive",  # Disable incremental recursion for accurate progress
        ]

        if delete_source:
            cmd.append("--remove-source-files")

        # Add source and destination
        # For directories, add trailing slash to copy contents
        source_str = str(source)
        if source.is_dir():
            source_str = source_str.rstrip("/") + "/"

        cmd.extend([source_str, str(dest) + "/"])

        log.debug(f"Running: {' '.join(cmd)}")

        # Calculate total size for progress
        total_size = _get_total_size(source)
        bytes_copied = 0

        # Run rsync with live progress parsing
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        # Read output lines for progress
        if process.stdout:
            for line in process.stdout:
                line = line.strip()
                if line:
                    progress = parse_rsync_progress(line)
                    if progress and progress_callback:
                        bytes_copied, estimated_total, speed = progress
                        # Use our calculated total if rsync's estimate seems off
                        if total_size > 0:
                            estimated_total = total_size
                        progress_callback(bytes_copied, estimated_total, speed)

        # Wait for completion
        _, stderr = process.communicate(timeout=86400)  # 24 hour timeout

        if process.returncode != 0:
            error_msg = stderr.strip() if stderr else f"rsync failed with code {process.returncode}"
            log.error(f"rsync failed: {error_msg}")
            return UploadResult(
                success=False,
                source_path=source,
                dest_path=dest,
                bytes_copied=bytes_copied,
                error=error_msg,
            )

        # If rsync used --remove-source-files, we need to clean up empty directories
        if delete_source and source.is_dir() and source.exists():
            try:
                # Remove empty directories left by rsync
                _remove_empty_dirs(source)
                # Remove the source directory itself if empty
                if source.exists() and not any(source.iterdir()):
                    source.rmdir()
            except OSError as e:
                log.warning(f"Failed to clean up empty directories: {e}")

        log.info(f"Successfully uploaded {total_size} bytes via rsync")
        return UploadResult(
            success=True,
            source_path=source,
            dest_path=dest,
            bytes_copied=total_size,
            error=None,
        )

    except subprocess.TimeoutExpired:
        log.error("rsync upload timed out")
        return UploadResult(
            success=False,
            source_path=source,
            dest_path=dest,
            bytes_copied=0,
            error="Upload timed out after 24 hours",
        )
    except Exception as e:
        log.error(f"rsync upload failed: {e}")
        return UploadResult(
            success=False,
            source_path=source,
            dest_path=dest,
            bytes_copied=0,
            error=f"rsync upload failed: {e}",
        )


def _remove_empty_dirs(path: Path) -> None:
    """Recursively remove empty directories.

    Args:
        path: Root path to clean up.
    """
    if not path.is_dir():
        return

    # Process subdirectories first (depth-first)
    for item in list(path.iterdir()):
        if item.is_dir():
            _remove_empty_dirs(item)

    # Remove this directory if empty
    if path.is_dir() and not any(path.iterdir()):
        try:
            path.rmdir()
        except OSError:
            pass


def _copy_file_with_progress(
    src: Path,
    dst: Path,
    progress_callback: Optional[Callable[[int, int, float], None]],
    current_copied: int,
    total_size: int,
    chunk_size: int = 1024 * 1024,  # 1MB chunks
) -> int:
    """Copy a single file with progress tracking.

    Args:
        src: Source file path.
        dst: Destination file path.
        progress_callback: Optional callback for progress updates.
        current_copied: Bytes already copied before this file.
        total_size: Total size of all files being copied.
        chunk_size: Size of chunks to read/write.

    Returns:
        Number of bytes copied.
    """
    bytes_copied = 0
    start_time = time.time()

    with open(src, "rb") as fsrc:
        with open(dst, "wb") as fdst:
            while True:
                chunk = fsrc.read(chunk_size)
                if not chunk:
                    break

                fdst.write(chunk)
                bytes_copied += len(chunk)

                if progress_callback:
                    elapsed = time.time() - start_time
                    speed = bytes_copied / elapsed if elapsed > 0 else 0
                    progress_callback(
                        current_copied + bytes_copied,
                        total_size,
                        speed,
                    )

    # Preserve metadata
    shutil.copystat(src, dst)

    return bytes_copied


def upload_with_python(
    source: Path,
    dest: Path,
    delete_source: bool,
    progress_callback: Optional[Callable[[int, int, float], None]],
    logger: Optional[logging.Logger],
) -> UploadResult:
    """Upload files using Python's shutil with progress tracking.

    Fallback method when rsync is not available. Uses chunked reading
    to provide progress updates.

    Args:
        source: Path to source file or directory.
        dest: Path to destination on Drive.
        delete_source: If True, remove source files after successful upload.
        progress_callback: Optional callback for progress updates.
            Called with (bytes_copied, total_bytes, speed_bps).
        logger: Optional logger for progress messages.

    Returns:
        UploadResult with upload status and bytes copied.
    """
    log = logger or logging.getLogger(__name__)

    try:
        # Ensure destination directory exists
        dest.mkdir(parents=True, exist_ok=True)

        # Calculate total size
        total_size = _get_total_size(source)
        bytes_copied = 0

        log.info(f"Uploading {total_size} bytes using Python copy")

        if source.is_file():
            # Single file copy
            dest_file = dest / source.name
            bytes_copied = _copy_file_with_progress(
                source,
                dest_file,
                progress_callback,
                0,
                total_size,
            )

            if delete_source:
                source.unlink()
                log.debug(f"Deleted source file: {source}")

        else:
            # Directory copy
            files_to_copy = [f for f in source.rglob("*") if f.is_file()]

            for src_file in files_to_copy:
                # Calculate relative path
                rel_path = src_file.relative_to(source)
                dest_file = dest / rel_path

                # Ensure parent directory exists
                dest_file.parent.mkdir(parents=True, exist_ok=True)

                # Copy with progress
                file_bytes = _copy_file_with_progress(
                    src_file,
                    dest_file,
                    progress_callback,
                    bytes_copied,
                    total_size,
                )
                bytes_copied += file_bytes
                log.debug(f"Copied: {rel_path}")

            # Delete source after all files are copied
            if delete_source:
                shutil.rmtree(source)
                log.debug(f"Deleted source directory: {source}")

        log.info(f"Successfully uploaded {bytes_copied} bytes")
        return UploadResult(
            success=True,
            source_path=source,
            dest_path=dest,
            bytes_copied=bytes_copied,
            error=None,
        )

    except PermissionError as e:
        log.error(f"Permission denied: {e}")
        return UploadResult(
            success=False,
            source_path=source,
            dest_path=dest,
            bytes_copied=bytes_copied,
            error=f"Permission denied: {e}",
        )
    except OSError as e:
        # Catch disk full, quota exceeded, etc.
        if e.errno == 28:  # ENOSPC - No space left on device
            error_msg = "No space left on destination device"
        elif e.errno == 122:  # EDQUOT - Disk quota exceeded
            error_msg = "Disk quota exceeded on destination"
        else:
            error_msg = f"OS error: {e}"

        log.error(error_msg)
        return UploadResult(
            success=False,
            source_path=source,
            dest_path=dest,
            bytes_copied=bytes_copied,
            error=error_msg,
        )
    except Exception as e:
        log.error(f"Upload failed: {e}")
        return UploadResult(
            success=False,
            source_path=source,
            dest_path=dest,
            bytes_copied=bytes_copied,
            error=f"Upload failed: {e}",
        )


def upload_to_drive(
    source: Path,
    drive_dest: Path,
    delete_after: bool = True,
    progress_callback: Optional[Callable[[int, int, float], None]] = None,
    logger: Optional[logging.Logger] = None,
) -> UploadResult:
    """Upload a file or directory to Google Drive mount.

    This is the main entry point for uploading files to Drive. It uses
    rsync when available for reliability and progress tracking, falling
    back to Python's shutil when rsync is not installed.

    The Drive mount is typically at /content/drive/MyDrive/ in Google Colab.

    Args:
        source: Path to the source file or directory to upload.
        drive_dest: Destination path on Drive (e.g., /content/drive/MyDrive/Uploads).
        delete_after: If True, delete source files after successful upload.
            Defaults to True to save disk space in Colab.
        progress_callback: Optional callback for progress updates.
            Called with (bytes_copied, total_bytes, speed_bps).
        logger: Optional logger for progress messages.

    Returns:
        UploadResult containing upload status, bytes copied, and any errors.

    Example:
        >>> result = upload_to_drive(
        ...     Path("/content/extracted/video.mp4"),
        ...     Path("/content/drive/MyDrive/Downloads"),
        ...     delete_after=True,
        ...     progress_callback=lambda c, t, s: print(f"{c}/{t} bytes, {s:.0f} B/s"),
        ... )
        >>> if result.success:
        ...     print(f"Uploaded {result.bytes_copied} bytes")
        ... else:
        ...     print(f"Upload failed: {result.error}")
    """
    log = logger or logging.getLogger(__name__)

    # Validate source
    if not source.exists():
        return UploadResult(
            success=False,
            source_path=source,
            dest_path=drive_dest,
            bytes_copied=0,
            error=f"Source not found: {source}",
        )

    # Log upload start
    total_size = _get_total_size(source)
    log.info(
        f"Uploading {'directory' if source.is_dir() else 'file'}: "
        f"{source.name} ({total_size:,} bytes) to {drive_dest}"
    )

    # Check if rsync is available and use it preferentially
    if check_rsync_available():
        log.debug("Using rsync for upload")
        return upload_with_rsync(
            source,
            drive_dest,
            delete_after,
            progress_callback,
            log,
        )
    else:
        log.debug("rsync not available, using Python fallback")
        return upload_with_python(
            source,
            drive_dest,
            delete_after,
            progress_callback,
            log,
        )
