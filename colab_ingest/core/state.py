"""SQLite-based state management for task tracking.

This module provides persistent state management for download tasks using
SQLite, with support for task status tracking, retry counting, and output
path management.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Generator, Optional

from ..utils.url_detect import HostType


class TaskStatus(Enum):
    """Enumeration of possible task states."""

    PENDING = "pending"
    DOWNLOADING = "downloading"
    EXTRACTING = "extracting"
    UPLOADING = "uploading"
    DONE = "done"
    FAILED = "failed"


@dataclass
class Task:
    """Represents a download/processing task.

    Attributes:
        id: Unique task identifier (UUID).
        url: Original URL for the download.
        host: Detected host type (pixeldrain, buzzheavier, bunkr).
        status: Current task status.
        output_paths: List of output file paths (JSON serialized in DB).
        error: Error message if task failed.
        created_at: Timestamp when task was created.
        updated_at: Timestamp of last update.
        retries: Number of retry attempts.
    """

    id: str
    url: str
    host: HostType
    status: TaskStatus
    output_paths: list[str] = field(default_factory=list)
    error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    retries: int = 0

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Task:
        """Create a Task instance from a database row.

        Args:
            row: SQLite row with task data.

        Returns:
            Task instance populated from the row.
        """
        return cls(
            id=row["id"],
            url=row["url"],
            host=HostType(row["host"]),
            status=TaskStatus(row["status"]),
            output_paths=json.loads(row["output_paths"]) if row["output_paths"] else [],
            error=row["error"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            retries=row["retries"],
        )

    def to_dict(self) -> dict:
        """Convert task to dictionary representation.

        Returns:
            Dictionary with task data.
        """
        return {
            "id": self.id,
            "url": self.url,
            "host": self.host.value,
            "status": self.status.value,
            "output_paths": self.output_paths,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "retries": self.retries,
        }


class StateDB:
    """SQLite-based state database for task management.

    Provides persistent storage for task state with support for:
    - Task creation and retrieval
    - Status updates with atomic operations
    - Retry counting
    - Output path tracking

    The database is created automatically if it doesn't exist.

    Attributes:
        db_path: Path to the SQLite database file.
    """

    def __init__(self, db_path: Path) -> None:
        """Initialize the state database.

        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = Path(db_path)
        self._ensure_parent_dir()

    def _ensure_parent_dir(self) -> None:
        """Ensure the parent directory for the database exists."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Get a database connection with row factory configured.

        Yields:
            Configured SQLite connection.
        """
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _transaction(self) -> Generator[tuple[sqlite3.Connection, sqlite3.Cursor], None, None]:
        """Get a connection with automatic transaction management.

        Yields:
            Tuple of (connection, cursor) with auto-commit on success.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield conn, cursor
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def init_db(self) -> None:
        """Initialize the database schema.

        Creates the tasks table if it doesn't exist. Safe to call multiple
        times - will not affect existing data.
        """
        with self._transaction() as (conn, cursor):
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    url TEXT NOT NULL UNIQUE,
                    host TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    output_paths TEXT DEFAULT '[]',
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    retries INTEGER DEFAULT 0
                )
            """)

            # Create indexes for common queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_tasks_status 
                ON tasks(status)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_tasks_url 
                ON tasks(url)
            """)

    def get_task_by_url(self, url: str) -> Optional[Task]:
        """Retrieve a task by its URL.

        Args:
            url: The original URL of the task.

        Returns:
            Task instance if found, None otherwise.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM tasks WHERE url = ?", (url,))
            row = cursor.fetchone()
            return Task.from_row(row) if row else None

    def get_task_by_id(self, task_id: str) -> Optional[Task]:
        """Retrieve a task by its ID.

        Args:
            task_id: The unique task identifier.

        Returns:
            Task instance if found, None otherwise.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
            row = cursor.fetchone()
            return Task.from_row(row) if row else None

    def get_tasks_by_status(self, status: TaskStatus) -> list[Task]:
        """Retrieve all tasks with a specific status.

        Args:
            status: The status to filter by.

        Returns:
            List of matching Task instances.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM tasks WHERE status = ? ORDER BY created_at",
                (status.value,)
            )
            return [Task.from_row(row) for row in cursor.fetchall()]

    def create_task(self, url: str, host: HostType) -> Task:
        """Create a new task for a URL.

        If a task for the URL already exists, returns the existing task.

        Args:
            url: The URL to create a task for.
            host: The detected host type.

        Returns:
            The created or existing Task instance.
        """
        # Check for existing task first
        existing = self.get_task_by_url(url)
        if existing:
            return existing

        now = datetime.now()
        task = Task(
            id=str(uuid.uuid4()),
            url=url,
            host=host,
            status=TaskStatus.PENDING,
            output_paths=[],
            error=None,
            created_at=now,
            updated_at=now,
            retries=0,
        )

        with self._transaction() as (conn, cursor):
            cursor.execute(
                """
                INSERT INTO tasks (id, url, host, status, output_paths, error, created_at, updated_at, retries)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task.id,
                    task.url,
                    task.host.value,
                    task.status.value,
                    json.dumps(task.output_paths),
                    task.error,
                    task.created_at.isoformat(),
                    task.updated_at.isoformat(),
                    task.retries,
                ),
            )

        return task

    def update_status(
        self,
        task_id: str,
        status: TaskStatus,
        error: Optional[str] = None,
    ) -> None:
        """Update a task's status.

        Args:
            task_id: The task ID to update.
            status: The new status.
            error: Optional error message (typically set when status is FAILED).

        Raises:
            ValueError: If task_id doesn't exist.
        """
        now = datetime.now().isoformat()

        with self._transaction() as (conn, cursor):
            if error is not None:
                cursor.execute(
                    """
                    UPDATE tasks 
                    SET status = ?, error = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (status.value, error, now, task_id),
                )
            else:
                cursor.execute(
                    """
                    UPDATE tasks 
                    SET status = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (status.value, now, task_id),
                )

            if cursor.rowcount == 0:
                raise ValueError(f"Task not found: {task_id}")

    def add_output_path(self, task_id: str, path: str) -> None:
        """Add an output path to a task's output_paths list.

        Args:
            task_id: The task ID to update.
            path: The output path to add.

        Raises:
            ValueError: If task_id doesn't exist.
        """
        task = self.get_task_by_id(task_id)
        if not task:
            raise ValueError(f"Task not found: {task_id}")

        output_paths = task.output_paths.copy()
        if path not in output_paths:
            output_paths.append(path)

        now = datetime.now().isoformat()

        with self._transaction() as (conn, cursor):
            cursor.execute(
                """
                UPDATE tasks 
                SET output_paths = ?, updated_at = ?
                WHERE id = ?
                """,
                (json.dumps(output_paths), now, task_id),
            )

    def set_output_paths(self, task_id: str, paths: list[str]) -> None:
        """Set the complete list of output paths for a task.

        Args:
            task_id: The task ID to update.
            paths: List of output paths.

        Raises:
            ValueError: If task_id doesn't exist.
        """
        now = datetime.now().isoformat()

        with self._transaction() as (conn, cursor):
            cursor.execute(
                """
                UPDATE tasks 
                SET output_paths = ?, updated_at = ?
                WHERE id = ?
                """,
                (json.dumps(paths), now, task_id),
            )

            if cursor.rowcount == 0:
                raise ValueError(f"Task not found: {task_id}")

    def increment_retry(self, task_id: str) -> int:
        """Increment the retry counter for a task.

        Args:
            task_id: The task ID to update.

        Returns:
            The new retry count.

        Raises:
            ValueError: If task_id doesn't exist.
        """
        task = self.get_task_by_id(task_id)
        if not task:
            raise ValueError(f"Task not found: {task_id}")

        new_count = task.retries + 1
        now = datetime.now().isoformat()

        with self._transaction() as (conn, cursor):
            cursor.execute(
                """
                UPDATE tasks 
                SET retries = ?, updated_at = ?
                WHERE id = ?
                """,
                (new_count, now, task_id),
            )

        return new_count

    def get_all_tasks(self) -> list[Task]:
        """Retrieve all tasks from the database.

        Returns:
            List of all Task instances, ordered by creation time.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM tasks ORDER BY created_at")
            return [Task.from_row(row) for row in cursor.fetchall()]

    def get_pending_and_failed_tasks(self, retry_failed: bool = False) -> list[Task]:
        """Get tasks that need processing.

        Args:
            retry_failed: If True, include FAILED tasks for retry.

        Returns:
            List of Task instances that need processing.
        """
        statuses = [TaskStatus.PENDING.value]
        if retry_failed:
            statuses.append(TaskStatus.FAILED.value)

        placeholders = ",".join("?" * len(statuses))

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT * FROM tasks WHERE status IN ({placeholders}) ORDER BY created_at",
                statuses,
            )
            return [Task.from_row(row) for row in cursor.fetchall()]

    def get_incomplete_tasks(self) -> list[Task]:
        """Get all tasks that are not yet complete (not DONE or FAILED).

        Returns:
            List of incomplete Task instances.
        """
        complete_statuses = [TaskStatus.DONE.value, TaskStatus.FAILED.value]
        placeholders = ",".join("?" * len(complete_statuses))

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT * FROM tasks WHERE status NOT IN ({placeholders}) ORDER BY created_at",
                complete_statuses,
            )
            return [Task.from_row(row) for row in cursor.fetchall()]

    def reset_task(self, task_id: str) -> None:
        """Reset a task to PENDING status for re-processing.

        Clears error message and resets status, but preserves retry count.

        Args:
            task_id: The task ID to reset.

        Raises:
            ValueError: If task_id doesn't exist.
        """
        now = datetime.now().isoformat()

        with self._transaction() as (conn, cursor):
            cursor.execute(
                """
                UPDATE tasks 
                SET status = ?, error = NULL, updated_at = ?
                WHERE id = ?
                """,
                (TaskStatus.PENDING.value, now, task_id),
            )

            if cursor.rowcount == 0:
                raise ValueError(f"Task not found: {task_id}")

    def delete_task(self, task_id: str) -> bool:
        """Delete a task from the database.

        Args:
            task_id: The task ID to delete.

        Returns:
            True if task was deleted, False if not found.
        """
        with self._transaction() as (conn, cursor):
            cursor.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            return cursor.rowcount > 0

    def get_stats(self) -> dict[str, int]:
        """Get task statistics by status.

        Returns:
            Dictionary mapping status names to counts.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT status, COUNT(*) as count FROM tasks GROUP BY status"
            )
            stats = {row["status"]: row["count"] for row in cursor.fetchall()}

        # Ensure all statuses are represented
        for status in TaskStatus:
            if status.value not in stats:
                stats[status.value] = 0

        stats["total"] = sum(stats.values())
        return stats

    def __repr__(self) -> str:
        """Return string representation of StateDB.

        Returns:
            String representation showing the database path.
        """
        return f"StateDB(db_path={self.db_path!r})"
