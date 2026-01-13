"""Bunkr downloader adapter wrapping the bundled BunkrDownloader module.

This module provides the BunkrDownloaderAdapter class for downloading files
from Bunkr using the bundled BunkrDownloader tool via subprocess calls.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional, Set, Tuple

from colab_ingest.utils.logging import get_logger


# Default configuration
DEFAULT_MAX_RETRIES = 3
DEFAULT_TIMEOUT_SECONDS = 30 * 60  # 30 minutes per URL


@dataclass
class BunkrDownloadResult:
    """Result of a Bunkr download operation.

    Attributes:
        success: Whether the download completed successfully.
        downloaded_files: List of all files downloaded.
        url: The Bunkr URL that was downloaded.
        error: Error message if download failed (None if successful).
        output_dir: Directory where files were downloaded.
    """

    success: bool
    downloaded_files: List[Path]
    url: str
    error: Optional[str]
    output_dir: Path


class BunkrDownloaderError(Exception):
    """Base exception for Bunkr downloader errors."""

    pass


class BunkrScriptNotFoundError(BunkrDownloaderError):
    """Raised when the BunkrDownloader script cannot be found."""

    pass


class BunkrDownloadTimeoutError(BunkrDownloaderError):
    """Raised when download times out."""

    pass


class BunkrDownloaderAdapter:
    """Adapter for the bundled BunkrDownloader module.

    Wraps the BunkrDownloader tool using subprocess calls for downloading
    files from Bunkr URLs (both album /a/ and file /f/ URLs).

    Attributes:
        download_dir: Directory to save downloaded files.
        max_retries: Maximum number of retry attempts for failed downloads.
        third_party_path: Path to the bundled bunkr downloader directory.
        timeout: Timeout in seconds for each download.
    """

    def __init__(
        self,
        download_dir: Path,
        max_retries: int = DEFAULT_MAX_RETRIES,
        third_party_path: Optional[Path] = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize the Bunkr downloader adapter.

        Args:
            download_dir: Directory where files will be downloaded.
            max_retries: Maximum retry attempts for failed downloads.
            third_party_path: Path to the BunkrDownloader directory.
                If None, uses the bundled module in the same directory.
            timeout: Timeout in seconds for each download (default 30 minutes).
            logger: Optional logger instance. If None, uses default logger.
        """
        self.download_dir = Path(download_dir)
        self.max_retries = max_retries
        self.timeout = timeout
        self._logger = logger or get_logger("downloaders.bunkr")

        # Auto-detect third_party path if not provided
        if third_party_path is None:
            self.third_party_path = self._auto_detect_third_party_path()
        else:
            self.third_party_path = Path(third_party_path)

        # Ensure download directory exists
        self.download_dir.mkdir(parents=True, exist_ok=True)

        self._logger.debug(
            f"BunkrDownloaderAdapter initialized with download_dir: {download_dir}, "
            f"third_party_path: {self.third_party_path}"
        )

    def _auto_detect_third_party_path(self) -> Path:
        """Auto-detect the bunkr module directory.

        Returns the path to the bundled bunkr downloader module which is
        located in the same directory as this adapter file.

        Returns:
            Path to the bunkr downloader directory.
        """
        # The bunkr module is bundled in the same directory as this adapter
        return Path(__file__).resolve().parent / "bunkr"

    def _find_downloader_script(self) -> Path:
        """Locate the downloader.py script in the bundled bunkr directory.

        Returns:
            Path to the downloader.py script.

        Raises:
            BunkrScriptNotFoundError: If the script cannot be found.
        """
        script_path = self.third_party_path / "downloader.py"

        if not script_path.exists():
            error_msg = (
                f"BunkrDownloader script not found at: {script_path}\n\n"
                "The BunkrDownloader module should be bundled at:\n"
                f"  {self.third_party_path}\n\n"
                "Please ensure the module is properly installed."
            )
            self._logger.error(error_msg)
            raise BunkrScriptNotFoundError(error_msg)

        return script_path

    def verify_installation(self) -> bool:
        """Check if BunkrDownloader is available and properly installed.

        Verifies that:
        - The downloader.py script exists
        - The script is readable
        - Basic Python imports work (optional check)

        Returns:
            True if BunkrDownloader is properly installed, False otherwise.
        """
        try:
            script_path = self._find_downloader_script()

            # Check if script is readable
            if not os.access(script_path, os.R_OK):
                self._logger.warning(f"BunkrDownloader script is not readable: {script_path}")
                return False

            # Check if the third_party directory has expected structure
            expected_files = ["downloader.py"]
            for expected_file in expected_files:
                if not (self.third_party_path / expected_file).exists():
                    self._logger.warning(f"Missing expected file: {expected_file}")
                    return False

            self._logger.debug("BunkrDownloader installation verified successfully")
            return True

        except BunkrScriptNotFoundError:
            return False
        except Exception as e:
            self._logger.warning(f"Error verifying BunkrDownloader installation: {e}")
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
        
        def read_output():
            """Read and process output lines from the process."""
            if process.stdout is None:
                return
                
            for line in iter(process.stdout.readline, ""):
                if not line:
                    break
                    
                line = line.rstrip("\n\r")
                collected_output.append(line)
                
                # Log with prefix
                self._logger.debug(f"[BUNKR] {line}")
                
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
            raise BunkrDownloadTimeoutError(
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

    def _collect_downloaded_files(self, output_dir: Path) -> List[Path]:
        """Scan output directory for downloaded files after process completes.

        Args:
            output_dir: Directory to scan for downloaded files.

        Returns:
            List of file paths found in the output directory.
        """
        if not output_dir.exists():
            return []

        files: List[Path] = []
        
        # Recursively find all files
        for item in output_dir.rglob("*"):
            if item.is_file():
                files.append(item)

        self._logger.debug(f"Collected {len(files)} downloaded file(s) from {output_dir}")
        
        return sorted(files)

    def _get_files_before_download(self, output_dir: Path) -> Set[Path]:
        """Get set of existing files before download starts.

        Args:
            output_dir: Directory to scan.

        Returns:
            Set of existing file paths (resolved to absolute paths).
        """
        if not output_dir.exists():
            return set()
        
        return {f.resolve() for f in output_dir.rglob("*") if f.is_file()}

    def download(
        self,
        url: str,
        output_callback: Optional[Callable[[str], None]] = None,
    ) -> BunkrDownloadResult:
        """Download from Bunkr URL using subprocess.

        Executes the BunkrDownloader script with the provided URL and streams
        output in real-time to the optional callback.

        Args:
            url: The Bunkr URL to download (album /a/ or file /f/ URL).
            output_callback: Optional callback for real-time log streaming.
                Receives each line of subprocess output.

        Returns:
            BunkrDownloadResult with download status and file information.
        """
        start_time = time.time()
        self._logger.info(f"Starting Bunkr download: {url}")

        # Verify installation
        try:
            script_path = self._find_downloader_script()
        except BunkrScriptNotFoundError as e:
            return BunkrDownloadResult(
                success=False,
                downloaded_files=[],
                url=url,
                error=str(e),
                output_dir=self.download_dir,
            )

        # Ensure output directory exists
        self.download_dir.mkdir(parents=True, exist_ok=True)

        # Get files before download to identify new files
        files_before = self._get_files_before_download(self.download_dir)

        # Build command
        cmd = [
            sys.executable,  # Use same Python interpreter
            str(script_path),
            url,
            "--custom-path", str(self.download_dir),
            "--max-retries", str(self.max_retries),
            "--disable-ui",  # Disable progress UI for logging
        ]

        self._logger.debug(f"Executing command: {' '.join(cmd)}")
        self._logger.debug(f"Working directory: {self.third_party_path}")

        try:
            # Start subprocess
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Combine stderr with stdout
                text=True,  # String output instead of bytes
                cwd=str(self.third_party_path),  # Set working directory for imports
                env={**os.environ, "PYTHONUNBUFFERED": "1"},  # Ensure unbuffered output
            )

            # Stream output and wait for completion
            return_code, output = self._stream_process_output(process, output_callback)

            # Calculate elapsed time
            elapsed_time = time.time() - start_time

            # Get new files (files downloaded during this operation)
            files_after = self._get_files_before_download(self.download_dir)
            new_files = files_after - files_before
            downloaded_files = sorted(list(new_files))

            # Check result
            if return_code == 0:
                self._logger.info(
                    f"Download completed successfully in {elapsed_time:.1f}s. "
                    f"Downloaded {len(downloaded_files)} file(s)."
                )
                
                # Log downloaded files
                for file_path in downloaded_files:
                    self._logger.info(f"  - {file_path.name}")

                return BunkrDownloadResult(
                    success=True,
                    downloaded_files=downloaded_files,
                    url=url,
                    error=None,
                    output_dir=self.download_dir,
                )
            else:
                error_msg = f"Download failed with exit code {return_code}"
                if output:
                    # Get last few lines for error context
                    output_lines = output.strip().split("\n")
                    last_lines = output_lines[-5:] if len(output_lines) > 5 else output_lines
                    error_msg += f"\nLast output:\n" + "\n".join(last_lines)

                self._logger.error(error_msg)

                return BunkrDownloadResult(
                    success=False,
                    downloaded_files=downloaded_files,  # May have partial downloads
                    url=url,
                    error=error_msg,
                    output_dir=self.download_dir,
                )

        except BunkrDownloadTimeoutError as e:
            # Collect any files that may have been downloaded before timeout
            files_after = self._get_files_before_download(self.download_dir)
            new_files = files_after - files_before
            downloaded_files = sorted(list(new_files))

            return BunkrDownloadResult(
                success=False,
                downloaded_files=downloaded_files,
                url=url,
                error=str(e),
                output_dir=self.download_dir,
            )

        except FileNotFoundError as e:
            error_msg = f"Failed to execute Python interpreter: {e}"
            self._logger.error(error_msg)

            return BunkrDownloadResult(
                success=False,
                downloaded_files=[],
                url=url,
                error=error_msg,
                output_dir=self.download_dir,
            )

        except Exception as e:
            error_msg = f"Unexpected error during download: {e}"
            self._logger.error(error_msg, exc_info=True)

            # Try to collect any downloaded files
            files_after = self._get_files_before_download(self.download_dir)
            new_files = files_after - files_before
            downloaded_files = sorted(list(new_files))

            return BunkrDownloadResult(
                success=False,
                downloaded_files=downloaded_files,
                url=url,
                error=error_msg,
                output_dir=self.download_dir,
            )

    def __repr__(self) -> str:
        """Return string representation of BunkrDownloaderAdapter.

        Returns:
            String representation showing configuration.
        """
        return (
            f"BunkrDownloaderAdapter("
            f"download_dir={self.download_dir!r}, "
            f"max_retries={self.max_retries}, "
            f"timeout={self.timeout})"
        )
