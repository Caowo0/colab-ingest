"""BuzzHeavier downloader adapter wrapping the bundled buzzheavier-downloader module.

This module provides the BuzzHeavierDownloaderAdapter class for downloading files
from BuzzHeavier using the bundled buzzheavier-downloader tool via subprocess calls.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, Set, Tuple

from colab_ingest.utils.logging import get_logger


# Default configuration
DEFAULT_TIMEOUT_SECONDS = 30 * 60  # 30 minutes per download


@dataclass
class BuzzHeavierDownloadResult:
    """Result of a BuzzHeavier download operation.

    Attributes:
        success: Whether the download completed successfully.
        downloaded_files: List of all files downloaded.
        file_id: The BuzzHeavier ID or URL that was downloaded.
        error: Error message if download failed (None if successful).
        output_dir: Directory where files were downloaded.
    """

    success: bool
    downloaded_files: List[Path]
    file_id: str
    error: Optional[str]
    output_dir: Path


class BuzzHeavierDownloaderError(Exception):
    """Base exception for BuzzHeavier downloader errors."""

    pass


class BuzzHeavierScriptNotFoundError(BuzzHeavierDownloaderError):
    """Raised when the buzzheavier-downloader script cannot be found."""

    pass


class BuzzHeavierDownloadTimeoutError(BuzzHeavierDownloaderError):
    """Raised when download times out."""

    pass


class BuzzHeavierDownloaderAdapter:
    """Adapter for the bundled buzzheavier-downloader module.

    Wraps the buzzheavier-downloader tool using subprocess calls for downloading
    files from BuzzHeavier using ID or full URL.

    The script downloads files to the current working directory, so we set
    cwd=download_dir when invoking the subprocess.

    Attributes:
        download_dir: Directory to save downloaded files.
        third_party_path: Path to the bundled buzzheavier-downloader directory.
        timeout: Timeout in seconds for each download.
    """

    def __init__(
        self,
        download_dir: Path,
        third_party_path: Optional[Path] = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize the BuzzHeavier downloader adapter.

        Args:
            download_dir: Directory where files will be downloaded.
            third_party_path: Path to the buzzheavier-downloader directory.
                If None, uses the bundled module in the same directory.
            timeout: Timeout in seconds for each download (default 30 minutes).
            logger: Optional logger instance. If None, uses default logger.
        """
        self.download_dir = Path(download_dir)
        self.timeout = timeout
        self._logger = logger or get_logger("downloaders.buzzheavier")

        # Auto-detect third_party path if not provided
        if third_party_path is None:
            self.third_party_path = self._auto_detect_third_party_path()
        else:
            self.third_party_path = Path(third_party_path)

        # Ensure download directory exists
        self.download_dir.mkdir(parents=True, exist_ok=True)

        self._logger.debug(
            f"BuzzHeavierDownloaderAdapter initialized with download_dir: {download_dir}, "
            f"third_party_path: {self.third_party_path}"
        )

    def _auto_detect_third_party_path(self) -> Path:
        """Auto-detect the buzzheavier module directory.

        Returns the path to the bundled buzzheavier downloader module which is
        located in the same directory as this adapter file.

        Returns:
            Path to the buzzheavier downloader directory.
        """
        # The buzzheavier module is bundled in the same directory as this adapter
        return Path(__file__).resolve().parent / "buzzheavier"

    def _find_downloader_script(self) -> Path:
        """Locate the bhdownload.py script in the bundled buzzheavier directory.

        Returns:
            Path to the bhdownload.py script.

        Raises:
            BuzzHeavierScriptNotFoundError: If the script cannot be found.
        """
        script_path = self.third_party_path / "bhdownload.py"

        if not script_path.exists():
            error_msg = (
                f"buzzheavier-downloader script not found at: {script_path}\n\n"
                "The buzzheavier-downloader module should be bundled at:\n"
                f"  {self.third_party_path}\n\n"
                "Please ensure the module is properly installed."
            )
            self._logger.error(error_msg)
            raise BuzzHeavierScriptNotFoundError(error_msg)

        return script_path

    def verify_installation(self) -> bool:
        """Check if buzzheavier-downloader is available and properly installed.

        Verifies that:
        - The bhdownload.py script exists
        - The script is readable

        Returns:
            True if buzzheavier-downloader is properly installed, False otherwise.
        """
        try:
            script_path = self._find_downloader_script()

            # Check if script is readable
            if not os.access(script_path, os.R_OK):
                self._logger.warning(
                    f"buzzheavier-downloader script is not readable: {script_path}"
                )
                return False

            self._logger.debug("buzzheavier-downloader installation verified successfully")
            return True

        except BuzzHeavierScriptNotFoundError:
            return False
        except Exception as e:
            self._logger.warning(f"Error verifying buzzheavier-downloader installation: {e}")
            return False

    def _stream_process_output(
        self,
        process: subprocess.Popen,
        callback: Optional[Callable[[str], None]],
    ) -> Tuple[int, str]:
        """Stream stdout/stderr from subprocess in real-time.

        Reads process output line-by-line and passes each line to the callback.
        Collects all output for error reporting.

        Args:
            process: The subprocess.Popen instance to stream from.
            callback: Optional callback function that receives each line of output.

        Returns:
            Tuple of (return_code, collected_output).
        """
        collected_output: List[str] = []

        def read_output() -> None:
            """Read and process output lines from the process."""
            if process.stdout is None:
                return

            for line in iter(process.stdout.readline, ""):
                if not line:
                    break

                line = line.rstrip("\n\r")
                collected_output.append(line)

                # Log with prefix
                self._logger.debug(f"[BUZZHEAVIER] {line}")

                # Call user callback if provided
                if callback:
                    try:
                        callback(line)
                    except Exception as e:
                        self._logger.warning(f"Output callback error: {e}")

        # Read output in a separate thread to handle blocking
        output_thread = threading.Thread(target=read_output, daemon=True)
        output_thread.start()

        # Wait for process completion with timeout
        try:
            return_code = process.wait(timeout=self.timeout)
        except subprocess.TimeoutExpired:
            self._logger.warning(f"Process timed out after {self.timeout}s, terminating...")
            self._terminate_process(process)
            raise BuzzHeavierDownloadTimeoutError(
                f"Download timed out after {self.timeout} seconds"
            )

        # Wait for output thread to finish
        output_thread.join(timeout=5.0)

        return return_code, "\n".join(collected_output)

    def _terminate_process(self, process: subprocess.Popen) -> None:
        """Terminate a subprocess gracefully, then forcefully if needed.

        Args:
            process: The subprocess to terminate.
        """
        try:
            # Try graceful termination first (SIGTERM)
            process.terminate()
            try:
                process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                # Force kill if still running (SIGKILL)
                self._logger.warning("Process did not terminate gracefully, forcing kill...")
                process.kill()
                process.wait(timeout=5.0)
        except Exception as e:
            self._logger.error(f"Error terminating process: {e}")

    def _collect_downloaded_files(
        self, output_dir: Path, before_files: Set[Path]
    ) -> List[Path]:
        """Identify newly downloaded files by comparing before/after.

        Args:
            output_dir: Directory to scan for downloaded files.
            before_files: Set of files that existed before download.

        Returns:
            List of newly downloaded file paths.
        """
        if not output_dir.exists():
            return []

        # Get current files
        current_files: Set[Path] = set()
        for item in output_dir.rglob("*"):
            if item.is_file():
                current_files.add(item)

        # Find new files
        new_files = current_files - before_files

        self._logger.debug(
            f"Collected {len(new_files)} newly downloaded file(s) from {output_dir}"
        )

        return sorted(list(new_files))

    def _get_files_before_download(self, output_dir: Path) -> Set[Path]:
        """Get set of existing files before download starts.

        Args:
            output_dir: Directory to scan.

        Returns:
            Set of existing file paths.
        """
        if not output_dir.exists():
            return set()

        return {f for f in output_dir.rglob("*") if f.is_file()}

    def download(
        self,
        file_id: str,
        output_callback: Optional[Callable[[str], None]] = None,
    ) -> BuzzHeavierDownloadResult:
        """Download from BuzzHeavier using subprocess.

        Executes the bhdownload.py script with the provided ID or URL and streams
        output in real-time to the optional callback.

        The script downloads to the current working directory, so we set
        cwd=download_dir in the subprocess call.

        Args:
            file_id: The BuzzHeavier ID or full URL to download.
            output_callback: Optional callback for real-time log streaming.
                Receives each line of subprocess output.

        Returns:
            BuzzHeavierDownloadResult with download status and file information.
        """
        start_time = time.time()
        self._logger.info(f"Starting BuzzHeavier download: {file_id}")

        # Verify installation
        try:
            script_path = self._find_downloader_script()
        except BuzzHeavierScriptNotFoundError as e:
            return BuzzHeavierDownloadResult(
                success=False,
                downloaded_files=[],
                file_id=file_id,
                error=str(e),
                output_dir=self.download_dir,
            )

        # Ensure output directory exists
        self.download_dir.mkdir(parents=True, exist_ok=True)

        # Get files before download to identify new files
        files_before = self._get_files_before_download(self.download_dir)

        # Build command
        # The script downloads to CWD, so we run it with cwd=download_dir
        # We need to provide the absolute path to the script since we're changing cwd
        cmd = [
            sys.executable,  # Use same Python interpreter
            str(script_path.resolve()),  # Absolute path to script
            file_id,
        ]

        self._logger.debug(f"Executing command: {' '.join(cmd)}")
        self._logger.debug(f"Working directory: {self.download_dir}")

        try:
            # Start subprocess
            # Set cwd to download_dir so files are downloaded there
            # Add script's directory to PYTHONPATH for relative imports
            env = {
                **os.environ,
                "PYTHONUNBUFFERED": "1",  # Ensure unbuffered output
                "PYTHONPATH": str(self.third_party_path.resolve())
                + os.pathsep
                + os.environ.get("PYTHONPATH", ""),
            }

            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Combine stderr with stdout
                text=True,  # String output instead of bytes
                cwd=str(self.download_dir),  # Download to this directory
                env=env,
            )

            # Stream output and wait for completion
            return_code, output = self._stream_process_output(process, output_callback)

            # Calculate elapsed time
            elapsed_time = time.time() - start_time

            # Get new files (files downloaded during this operation)
            downloaded_files = self._collect_downloaded_files(
                self.download_dir, files_before
            )

            # Check result
            if return_code == 0:
                self._logger.info(
                    f"Download completed successfully in {elapsed_time:.1f}s. "
                    f"Downloaded {len(downloaded_files)} file(s)."
                )

                # Log downloaded files
                for file_path in downloaded_files:
                    self._logger.info(f"  - {file_path.name}")

                return BuzzHeavierDownloadResult(
                    success=True,
                    downloaded_files=downloaded_files,
                    file_id=file_id,
                    error=None,
                    output_dir=self.download_dir,
                )
            else:
                error_msg = f"Download failed with exit code {return_code}"
                if output:
                    # Get last few lines for error context
                    output_lines = output.strip().split("\n")
                    last_lines = (
                        output_lines[-5:] if len(output_lines) > 5 else output_lines
                    )
                    error_msg += f"\nLast output:\n" + "\n".join(last_lines)

                self._logger.error(error_msg)

                return BuzzHeavierDownloadResult(
                    success=False,
                    downloaded_files=downloaded_files,  # May have partial downloads
                    file_id=file_id,
                    error=error_msg,
                    output_dir=self.download_dir,
                )

        except BuzzHeavierDownloadTimeoutError as e:
            # Collect any files that may have been downloaded before timeout
            downloaded_files = self._collect_downloaded_files(
                self.download_dir, files_before
            )

            return BuzzHeavierDownloadResult(
                success=False,
                downloaded_files=downloaded_files,
                file_id=file_id,
                error=str(e),
                output_dir=self.download_dir,
            )

        except FileNotFoundError as e:
            error_msg = f"Failed to execute Python interpreter: {e}"
            self._logger.error(error_msg)

            return BuzzHeavierDownloadResult(
                success=False,
                downloaded_files=[],
                file_id=file_id,
                error=error_msg,
                output_dir=self.download_dir,
            )

        except Exception as e:
            error_msg = f"Unexpected error during download: {e}"
            self._logger.error(error_msg, exc_info=True)

            # Try to collect any downloaded files
            downloaded_files = self._collect_downloaded_files(
                self.download_dir, files_before
            )

            return BuzzHeavierDownloadResult(
                success=False,
                downloaded_files=downloaded_files,
                file_id=file_id,
                error=error_msg,
                output_dir=self.download_dir,
            )

    def __repr__(self) -> str:
        """Return string representation of BuzzHeavierDownloaderAdapter.

        Returns:
            String representation showing configuration.
        """
        return (
            f"BuzzHeavierDownloaderAdapter("
            f"download_dir={self.download_dir!r}, "
            f"timeout={self.timeout})"
        )
