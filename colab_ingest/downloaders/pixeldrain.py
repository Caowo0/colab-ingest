"""Pixeldrain file downloader with resume support and progress tracking.

This module provides the PixeldrainDownloader class for downloading files
from Pixeldrain using their API with HTTP Basic authentication.
"""

from __future__ import annotations

import base64
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import requests

from colab_ingest.utils.logging import get_logger, mask_sensitive_data


# Pixeldrain API endpoints
PIXELDRAIN_API_BASE = "https://pixeldrain.com/api"
PIXELDRAIN_FILE_INFO_URL = f"{PIXELDRAIN_API_BASE}/file/{{file_id}}/info"
PIXELDRAIN_FILE_DOWNLOAD_URL = f"{PIXELDRAIN_API_BASE}/file/{{file_id}}"

# Default configuration
DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1MB
DEFAULT_TIMEOUT = 30  # seconds
DEFAULT_MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 60.0
BACKOFF_MULTIPLIER = 2.0


@dataclass
class DownloadResult:
    """Result of a download operation.

    Attributes:
        success: Whether the download completed successfully.
        file_path: Path to the downloaded file (None if failed).
        file_name: Name of the downloaded file.
        file_size: Size of the file in bytes.
        error: Error message if download failed (None if successful).
        was_resumed: Whether the download was resumed from a partial file.
    """

    success: bool
    file_path: Optional[Path]
    file_name: str
    file_size: int
    error: Optional[str]
    was_resumed: bool


class PixeldrainError(Exception):
    """Base exception for Pixeldrain-related errors."""

    pass


class PixeldrainAuthError(PixeldrainError):
    """Authentication or authorization error (401/403)."""

    pass


class PixeldrainNotFoundError(PixeldrainError):
    """File not found error (404)."""

    pass


class PixeldrainRateLimitError(PixeldrainError):
    """Rate limit exceeded error (429)."""

    def __init__(self, message: str, retry_after: Optional[int] = None) -> None:
        """Initialize with optional retry-after value.

        Args:
            message: Error message.
            retry_after: Seconds to wait before retrying (from Retry-After header).
        """
        super().__init__(message)
        self.retry_after = retry_after


class PixeldrainDownloader:
    """Downloads files from Pixeldrain with resume support and progress tracking.

    Uses Pixeldrain API with HTTP Basic authentication where the username
    is empty and the password is the API key.

    Attributes:
        api_key: The Pixeldrain API key for authentication.
        download_dir: Directory to save downloaded files.
        max_retries: Maximum number of retry attempts for failed requests.
        chunk_size: Size of chunks for streaming downloads.
        timeout: Request timeout in seconds.
    """

    def __init__(
        self,
        api_key: str,
        download_dir: Path,
        max_retries: int = DEFAULT_MAX_RETRIES,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        timeout: int = DEFAULT_TIMEOUT,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize the Pixeldrain downloader.

        Args:
            api_key: Pixeldrain API key for authentication.
            download_dir: Directory where files will be downloaded.
            max_retries: Maximum retry attempts for failed requests.
            chunk_size: Size of chunks for streaming downloads (default 1MB).
            timeout: Request timeout in seconds.
            logger: Optional logger instance. If None, uses default logger.
        """
        self.api_key = api_key
        self.download_dir = Path(download_dir)
        self.max_retries = max_retries
        self.chunk_size = chunk_size
        self.timeout = timeout
        self._logger = logger or get_logger("downloaders.pixeldrain")

        # Ensure download directory exists
        self.download_dir.mkdir(parents=True, exist_ok=True)

        self._logger.debug(
            f"PixeldrainDownloader initialized with API key: {mask_sensitive_data(api_key)}, "
            f"download_dir: {download_dir}"
        )

    def _build_auth_header(self) -> str:
        """Build HTTP Basic auth header with API key.

        The Pixeldrain API uses HTTP Basic Auth where:
        - Username: empty string
        - Password: API key

        Returns:
            The Authorization header value (e.g., "Basic <base64>").
        """
        # Format: ":api_key" (empty username, colon, password)
        credentials = f":{self.api_key}"
        encoded = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
        return f"Basic {encoded}"

    def _get_headers(self, additional: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Get request headers with authentication.

        Args:
            additional: Optional additional headers to include.

        Returns:
            Dictionary of headers including Authorization.
        """
        headers = {
            "Authorization": self._build_auth_header(),
            "User-Agent": "colab-ingest/1.0",
        }
        if additional:
            headers.update(additional)
        return headers

    def _handle_response_error(self, response: requests.Response, context: str) -> None:
        """Handle HTTP error responses with appropriate exceptions.

        Args:
            response: The HTTP response to check.
            context: Description of the operation for error messages.

        Raises:
            PixeldrainAuthError: For 401/403 responses.
            PixeldrainNotFoundError: For 404 responses.
            PixeldrainRateLimitError: For 429 responses.
            PixeldrainError: For other error responses.
        """
        if response.ok:
            return

        status_code = response.status_code
        try:
            error_data = response.json()
            error_message = error_data.get("message", response.text)
        except (ValueError, KeyError):
            error_message = response.text or f"HTTP {status_code}"

        if status_code == 401:
            self._logger.error(f"Authentication failed for {context}: {error_message}")
            raise PixeldrainAuthError(f"Authentication failed: {error_message}")

        if status_code == 403:
            # 403 can indicate captcha, virus scan, rate limit, or access denied
            self._logger.error(
                f"Access denied for {context}: {error_message}. "
                "This may be due to captcha requirement, virus scan, or rate limiting."
            )
            raise PixeldrainAuthError(f"Access denied: {error_message}")

        if status_code == 404:
            self._logger.error(f"File not found for {context}: {error_message}")
            raise PixeldrainNotFoundError(f"File not found: {error_message}")

        if status_code == 429:
            retry_after = None
            if "Retry-After" in response.headers:
                try:
                    retry_after = int(response.headers["Retry-After"])
                except ValueError:
                    pass
            self._logger.warning(
                f"Rate limited for {context}. Retry-After: {retry_after or 'not specified'}"
            )
            raise PixeldrainRateLimitError(
                f"Rate limited: {error_message}", retry_after=retry_after
            )

        self._logger.error(f"HTTP {status_code} for {context}: {error_message}")
        raise PixeldrainError(f"HTTP {status_code}: {error_message}")

    def _calculate_backoff(self, attempt: int, retry_after: Optional[int] = None) -> float:
        """Calculate backoff time with exponential increase.

        Args:
            attempt: Current attempt number (0-indexed).
            retry_after: Optional Retry-After value from server.

        Returns:
            Seconds to wait before next retry.
        """
        if retry_after is not None:
            return float(retry_after)

        backoff = INITIAL_BACKOFF_SECONDS * (BACKOFF_MULTIPLIER ** attempt)
        return min(backoff, MAX_BACKOFF_SECONDS)

    def get_file_info(self, file_id: str) -> Dict[str, Any]:
        """Fetch file metadata from Pixeldrain API.

        Args:
            file_id: The Pixeldrain file ID.

        Returns:
            Dictionary containing file metadata with keys:
            - name: File name
            - size: File size in bytes
            - mime_type: MIME type of the file

        Raises:
            PixeldrainAuthError: If authentication fails.
            PixeldrainNotFoundError: If file is not found.
            PixeldrainRateLimitError: If rate limited.
            PixeldrainError: For other API errors.
        """
        url = PIXELDRAIN_FILE_INFO_URL.format(file_id=file_id)
        self._logger.debug(f"Fetching file info for ID: {file_id}")

        last_error: Optional[Exception] = None

        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    url,
                    headers=self._get_headers(),
                    timeout=self.timeout,
                )
                self._handle_response_error(response, f"get_file_info({file_id})")

                data = response.json()
                self._logger.debug(
                    f"File info retrieved: name={data.get('name')}, "
                    f"size={data.get('size')}, mime_type={data.get('mime_type')}"
                )
                return {
                    "name": data.get("name", "unknown"),
                    "size": data.get("size", 0),
                    "mime_type": data.get("mime_type", "application/octet-stream"),
                }

            except (PixeldrainAuthError, PixeldrainNotFoundError):
                # Don't retry auth or not found errors
                raise

            except PixeldrainRateLimitError as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    backoff = self._calculate_backoff(attempt, e.retry_after)
                    self._logger.info(f"Rate limited, waiting {backoff:.1f}s before retry...")
                    time.sleep(backoff)
                continue

            except requests.exceptions.RequestException as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    backoff = self._calculate_backoff(attempt)
                    self._logger.warning(
                        f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}. "
                        f"Retrying in {backoff:.1f}s..."
                    )
                    time.sleep(backoff)
                continue

        # All retries exhausted
        error_msg = f"Failed to get file info after {self.max_retries} attempts: {last_error}"
        self._logger.error(error_msg)
        raise PixeldrainError(error_msg) from last_error

    def download(
        self,
        file_id: str,
        progress_callback: Optional[Callable[[int, int, float, float], None]] = None,
    ) -> DownloadResult:
        """Download a file from Pixeldrain with progress tracking and resume support.

        Args:
            file_id: The Pixeldrain file ID to download.
            progress_callback: Optional callback function that receives:
                - downloaded_bytes: Total bytes downloaded so far
                - total_bytes: Total file size
                - speed_bps: Current download speed in bytes per second
                - eta_seconds: Estimated time remaining in seconds

        Returns:
            DownloadResult with download status and file information.
        """
        self._logger.info(f"Starting download for file ID: {file_id}")

        # Get file metadata first
        try:
            file_info = self.get_file_info(file_id)
        except PixeldrainError as e:
            return DownloadResult(
                success=False,
                file_path=None,
                file_name="",
                file_size=0,
                error=str(e),
                was_resumed=False,
            )

        file_name = file_info["name"]
        total_size = file_info["size"]

        # Setup file paths
        final_path = self.download_dir / file_name
        temp_path = self.download_dir / f"{file_name}.tmp"

        # Check for partial download (resume support)
        downloaded_bytes = 0
        was_resumed = False

        if temp_path.exists():
            downloaded_bytes = temp_path.stat().st_size
            if downloaded_bytes > 0 and downloaded_bytes < total_size:
                self._logger.info(
                    f"Found partial download: {downloaded_bytes}/{total_size} bytes. "
                    "Attempting to resume..."
                )
                was_resumed = True
            elif downloaded_bytes >= total_size:
                # File appears complete, rename and return
                self._logger.info("Partial file appears complete, finalizing...")
                temp_path.rename(final_path)
                return DownloadResult(
                    success=True,
                    file_path=final_path,
                    file_name=file_name,
                    file_size=total_size,
                    error=None,
                    was_resumed=True,
                )

        # Download the file
        url = PIXELDRAIN_FILE_DOWNLOAD_URL.format(file_id=file_id)
        last_error: Optional[Exception] = None

        for attempt in range(self.max_retries):
            try:
                result = self._download_with_resume(
                    url=url,
                    temp_path=temp_path,
                    final_path=final_path,
                    total_size=total_size,
                    initial_bytes=downloaded_bytes if was_resumed else 0,
                    progress_callback=progress_callback,
                )

                if result.success:
                    return DownloadResult(
                        success=True,
                        file_path=result.file_path,
                        file_name=file_name,
                        file_size=total_size,
                        error=None,
                        was_resumed=was_resumed,
                    )

                last_error = Exception(result.error)

            except (PixeldrainAuthError, PixeldrainNotFoundError) as e:
                # Don't retry auth or not found errors
                return DownloadResult(
                    success=False,
                    file_path=None,
                    file_name=file_name,
                    file_size=total_size,
                    error=str(e),
                    was_resumed=was_resumed,
                )

            except PixeldrainRateLimitError as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    backoff = self._calculate_backoff(attempt, e.retry_after)
                    self._logger.info(f"Rate limited, waiting {backoff:.1f}s before retry...")
                    time.sleep(backoff)
                    # Update downloaded bytes for resume
                    if temp_path.exists():
                        downloaded_bytes = temp_path.stat().st_size
                        was_resumed = downloaded_bytes > 0
                continue

            except (requests.exceptions.RequestException, PixeldrainError) as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    backoff = self._calculate_backoff(attempt)
                    self._logger.warning(
                        f"Download failed (attempt {attempt + 1}/{self.max_retries}): {e}. "
                        f"Retrying in {backoff:.1f}s..."
                    )
                    time.sleep(backoff)
                    # Update downloaded bytes for resume
                    if temp_path.exists():
                        downloaded_bytes = temp_path.stat().st_size
                        was_resumed = downloaded_bytes > 0
                continue

        # All retries exhausted
        error_msg = f"Download failed after {self.max_retries} attempts: {last_error}"
        self._logger.error(error_msg)
        return DownloadResult(
            success=False,
            file_path=None,
            file_name=file_name,
            file_size=total_size,
            error=error_msg,
            was_resumed=was_resumed,
        )

    def _download_with_resume(
        self,
        url: str,
        temp_path: Path,
        final_path: Path,
        total_size: int,
        initial_bytes: int,
        progress_callback: Optional[Callable[[int, int, float, float], None]] = None,
    ) -> DownloadResult:
        """Execute the actual download with resume support.

        Args:
            url: Download URL.
            temp_path: Path for temporary download file.
            final_path: Final path for completed download.
            total_size: Expected total file size.
            initial_bytes: Bytes already downloaded (for resume).
            progress_callback: Optional progress callback.

        Returns:
            DownloadResult with download status.
        """
        headers = self._get_headers()
        downloaded_bytes = initial_bytes
        server_supports_range = False

        # Add Range header if resuming
        if initial_bytes > 0:
            headers["Range"] = f"bytes={initial_bytes}-"
            self._logger.debug(f"Requesting range: bytes={initial_bytes}-")

        response = requests.get(
            url,
            headers=headers,
            stream=True,
            timeout=self.timeout,
        )

        self._handle_response_error(response, f"download({url})")

        # Check if server supports range requests
        if initial_bytes > 0:
            if response.status_code == 206:
                # Server accepted range request
                server_supports_range = True
                self._logger.debug("Server supports range requests, resuming download")
            elif response.status_code == 200:
                # Server doesn't support range, start from beginning
                self._logger.warning(
                    "Server does not support range requests, restarting download from beginning"
                )
                downloaded_bytes = 0
                initial_bytes = 0

        # Determine write mode
        write_mode = "ab" if server_supports_range and initial_bytes > 0 else "wb"

        # Progress tracking variables
        start_time = time.time()
        last_progress_time = start_time
        bytes_since_last_progress = 0

        try:
            with open(temp_path, write_mode) as f:
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    if chunk:
                        f.write(chunk)
                        chunk_size = len(chunk)
                        downloaded_bytes += chunk_size
                        bytes_since_last_progress += chunk_size

                        # Calculate progress and call callback
                        if progress_callback:
                            current_time = time.time()
                            time_since_last = current_time - last_progress_time

                            if time_since_last >= 0.5 or downloaded_bytes >= total_size:
                                # Calculate speed (bytes per second)
                                if time_since_last > 0:
                                    speed_bps = bytes_since_last_progress / time_since_last
                                else:
                                    speed_bps = 0.0

                                # Calculate ETA
                                remaining_bytes = total_size - downloaded_bytes
                                if speed_bps > 0:
                                    eta_seconds = remaining_bytes / speed_bps
                                else:
                                    eta_seconds = float("inf")

                                progress_callback(
                                    downloaded_bytes, total_size, speed_bps, eta_seconds
                                )

                                last_progress_time = current_time
                                bytes_since_last_progress = 0

            # Verify download size
            actual_size = temp_path.stat().st_size
            if actual_size != total_size:
                error_msg = (
                    f"Size mismatch: expected {total_size} bytes, got {actual_size} bytes"
                )
                self._logger.error(error_msg)
                return DownloadResult(
                    success=False,
                    file_path=None,
                    file_name=final_path.name,
                    file_size=actual_size,
                    error=error_msg,
                    was_resumed=initial_bytes > 0,
                )

            # Atomic rename: move temp file to final location
            temp_path.rename(final_path)
            self._logger.info(f"Download complete: {final_path}")

            return DownloadResult(
                success=True,
                file_path=final_path,
                file_name=final_path.name,
                file_size=total_size,
                error=None,
                was_resumed=initial_bytes > 0,
            )

        except IOError as e:
            error_msg = f"File write error: {e}"
            self._logger.error(error_msg)
            return DownloadResult(
                success=False,
                file_path=None,
                file_name=final_path.name,
                file_size=downloaded_bytes,
                error=error_msg,
                was_resumed=initial_bytes > 0,
            )
