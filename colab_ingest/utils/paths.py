"""Path utilities and working directory management.

This module provides the WorkdirManager class for managing the working
directory structure used by the colab_ingest CLI tool.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Optional


class WorkdirManager:
    """Manages working directory structure for download, extraction, and logging.

    The working directory follows this structure:
        workdir/
        ├── downloads/          # Raw downloaded files
        │   └── <task_id>/      # Task-specific download directory
        ├── extracted/          # Extracted archive contents
        │   └── <task_id>/      # Task-specific extraction directory
        ├── logs/               # Log files
        └── state.db            # SQLite state database

    Attributes:
        workdir: The root working directory path.
    """

    def __init__(self, workdir: Path) -> None:
        """Initialize the WorkdirManager with a root working directory.

        Args:
            workdir: Path to the root working directory. Can be a string
                that will be converted to Path.
        """
        self._workdir = Path(workdir).resolve()

    @property
    def workdir(self) -> Path:
        """Get the root working directory path.

        Returns:
            Absolute path to the working directory.
        """
        return self._workdir

    @property
    def downloads_dir(self) -> Path:
        """Get the downloads directory path.

        Returns:
            Path to the downloads directory.
        """
        return self._workdir / "downloads"

    @property
    def extracted_dir(self) -> Path:
        """Get the extracted files directory path.

        Returns:
            Path to the extracted directory.
        """
        return self._workdir / "extracted"

    @property
    def logs_dir(self) -> Path:
        """Get the logs directory path.

        Returns:
            Path to the logs directory.
        """
        return self._workdir / "logs"

    @property
    def state_db_path(self) -> Path:
        """Get the state database file path.

        Returns:
            Path to the SQLite state database file.
        """
        return self._workdir / "state.db"

    def get_task_download_dir(self, task_id: str) -> Path:
        """Get the download directory for a specific task.

        Args:
            task_id: The unique task identifier.

        Returns:
            Path to the task's download directory.

        Raises:
            ValueError: If task_id is empty or contains invalid characters.
        """
        self._validate_task_id(task_id)
        return self.downloads_dir / task_id

    def get_task_extract_dir(self, task_id: str) -> Path:
        """Get the extraction directory for a specific task.

        Args:
            task_id: The unique task identifier.

        Returns:
            Path to the task's extraction directory.

        Raises:
            ValueError: If task_id is empty or contains invalid characters.
        """
        self._validate_task_id(task_id)
        return self.extracted_dir / task_id

    def ensure_dirs(self) -> None:
        """Create all required directories if they don't exist.

        Creates the following directories:
        - downloads/
        - extracted/
        - logs/
        """
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        self.extracted_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

    def ensure_task_dirs(self, task_id: str) -> tuple[Path, Path]:
        """Create task-specific directories and return their paths.

        Args:
            task_id: The unique task identifier.

        Returns:
            Tuple of (download_dir, extract_dir) paths for the task.

        Raises:
            ValueError: If task_id is empty or contains invalid characters.
        """
        self._validate_task_id(task_id)

        download_dir = self.get_task_download_dir(task_id)
        extract_dir = self.get_task_extract_dir(task_id)

        download_dir.mkdir(parents=True, exist_ok=True)
        extract_dir.mkdir(parents=True, exist_ok=True)

        return download_dir, extract_dir

    def cleanup_task(self, task_id: str) -> None:
        """Remove task-specific directories and their contents.

        Removes both the download and extraction directories for the
        specified task. Silently succeeds if directories don't exist.

        Args:
            task_id: The unique task identifier.

        Raises:
            ValueError: If task_id is empty or contains invalid characters.
            OSError: If directories cannot be removed due to permissions.
        """
        self._validate_task_id(task_id)

        download_dir = self.get_task_download_dir(task_id)
        extract_dir = self.get_task_extract_dir(task_id)

        if download_dir.exists():
            shutil.rmtree(download_dir)

        if extract_dir.exists():
            shutil.rmtree(extract_dir)

    def cleanup_all_tasks(self) -> None:
        """Remove all task directories in downloads and extracted folders.

        This clears all downloaded and extracted files but preserves
        the directory structure and logs.
        """
        # Clear downloads directory contents
        if self.downloads_dir.exists():
            for item in self.downloads_dir.iterdir():
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()

        # Clear extracted directory contents
        if self.extracted_dir.exists():
            for item in self.extracted_dir.iterdir():
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()

    def get_task_files(self, task_id: str, directory: str = "downloads") -> list[Path]:
        """List all files in a task's directory.

        Args:
            task_id: The unique task identifier.
            directory: Which directory to list - "downloads" or "extracted".

        Returns:
            List of file paths in the task directory.

        Raises:
            ValueError: If task_id is invalid or directory is not recognized.
        """
        self._validate_task_id(task_id)

        if directory == "downloads":
            task_dir = self.get_task_download_dir(task_id)
        elif directory == "extracted":
            task_dir = self.get_task_extract_dir(task_id)
        else:
            raise ValueError(f"Unknown directory type: {directory}")

        if not task_dir.exists():
            return []

        return [f for f in task_dir.rglob("*") if f.is_file()]

    def get_disk_usage(self) -> dict[str, int]:
        """Calculate disk usage for each directory.

        Returns:
            Dictionary with directory names as keys and bytes used as values.
        """
        usage = {
            "downloads": 0,
            "extracted": 0,
            "logs": 0,
            "total": 0,
        }

        for name, path in [
            ("downloads", self.downloads_dir),
            ("extracted", self.extracted_dir),
            ("logs", self.logs_dir),
        ]:
            if path.exists():
                size = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
                usage[name] = size
                usage["total"] += size

        return usage

    @staticmethod
    def _validate_task_id(task_id: str) -> None:
        """Validate a task ID for use in file paths.

        Args:
            task_id: The task identifier to validate.

        Raises:
            ValueError: If task_id is empty, contains path separators,
                or contains other invalid characters.
        """
        if not task_id:
            raise ValueError("Task ID cannot be empty")

        if not task_id.strip():
            raise ValueError("Task ID cannot be whitespace only")

        # Check for path traversal attempts and invalid characters
        invalid_chars = ['/', '\\', '..', '\0']
        for char in invalid_chars:
            if char in task_id:
                raise ValueError(
                    f"Task ID contains invalid character or sequence: {repr(char)}"
                )

    def __repr__(self) -> str:
        """Return string representation of WorkdirManager.

        Returns:
            String representation showing the workdir path.
        """
        return f"WorkdirManager(workdir={self._workdir!r})"
