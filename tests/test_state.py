"""Tests for state management with SQLite database."""

import pytest
from pathlib import Path
from datetime import datetime

from colab_ingest.core.state import StateDB, Task, TaskStatus
from colab_ingest.utils.url_detect import HostType


class TestStateDBInitialization:
    """Tests for StateDB initialization and setup."""

    def test_init_creates_parent_dir(self, temp_dir):
        """StateDB creates parent directories if needed."""
        nested_path = temp_dir / "nested" / "dirs" / "state.db"
        db = StateDB(nested_path)
        db.init_db()
        
        assert nested_path.parent.exists()
        assert nested_path.exists()

    def test_init_db_creates_tables(self, temp_state_db):
        """init_db() creates the tasks table."""
        # Table should exist - try to query it
        tasks = temp_state_db.get_all_tasks()
        assert tasks == []

    def test_init_db_is_idempotent(self, temp_state_db):
        """Calling init_db() multiple times is safe."""
        # Create a task
        temp_state_db.create_task("https://example.com/file1", HostType.PIXELDRAIN)
        
        # Call init_db again
        temp_state_db.init_db()
        
        # Task should still exist
        tasks = temp_state_db.get_all_tasks()
        assert len(tasks) == 1

    def test_repr(self, temp_state_db):
        """StateDB has a useful string representation."""
        repr_str = repr(temp_state_db)
        assert "StateDB" in repr_str
        assert "db_path" in repr_str


class TestTaskCreation:
    """Tests for task creation."""

    def test_create_task_basic(self, temp_state_db):
        """Create a basic task."""
        url = "https://pixeldrain.com/u/abc12345"
        task = temp_state_db.create_task(url, HostType.PIXELDRAIN)
        
        assert task.url == url
        assert task.host == HostType.PIXELDRAIN
        assert task.status == TaskStatus.PENDING
        assert task.retries == 0
        assert task.error is None
        assert task.output_paths == []
        assert task.id is not None

    def test_create_task_with_different_hosts(self, temp_state_db):
        """Create tasks with different host types."""
        hosts = [
            ("https://pixeldrain.com/u/abc12345", HostType.PIXELDRAIN),
            ("https://buzzheavier.com/f/abc123def456", HostType.BUZZHEAVIER),
            ("https://bunkr.si/a/album-name", HostType.BUNKR),
        ]
        
        for url, host in hosts:
            task = temp_state_db.create_task(url, host)
            assert task.host == host

    def test_create_task_idempotent(self, temp_state_db):
        """Creating same task twice returns existing task."""
        url = "https://pixeldrain.com/u/abc12345"
        
        task1 = temp_state_db.create_task(url, HostType.PIXELDRAIN)
        task2 = temp_state_db.create_task(url, HostType.PIXELDRAIN)
        
        assert task1.id == task2.id
        assert task1.url == task2.url

    def test_create_task_generates_uuid(self, temp_state_db):
        """Each new task gets a unique UUID."""
        task1 = temp_state_db.create_task("https://example1.com", HostType.PIXELDRAIN)
        task2 = temp_state_db.create_task("https://example2.com", HostType.PIXELDRAIN)
        
        assert task1.id != task2.id
        # UUIDs should be 36 characters with hyphens
        assert len(task1.id) == 36
        assert len(task2.id) == 36


class TestTaskRetrieval:
    """Tests for task retrieval methods."""

    def test_get_task_by_url(self, temp_state_db):
        """Retrieve task by URL."""
        url = "https://pixeldrain.com/u/abc12345"
        created = temp_state_db.create_task(url, HostType.PIXELDRAIN)
        
        retrieved = temp_state_db.get_task_by_url(url)
        
        assert retrieved is not None
        assert retrieved.id == created.id
        assert retrieved.url == url

    def test_get_task_by_url_not_found(self, temp_state_db):
        """Return None for nonexistent URL."""
        result = temp_state_db.get_task_by_url("https://nonexistent.com")
        assert result is None

    def test_get_task_by_id(self, temp_state_db):
        """Retrieve task by ID."""
        url = "https://pixeldrain.com/u/abc12345"
        created = temp_state_db.create_task(url, HostType.PIXELDRAIN)
        
        retrieved = temp_state_db.get_task_by_id(created.id)
        
        assert retrieved is not None
        assert retrieved.url == url

    def test_get_task_by_id_not_found(self, temp_state_db):
        """Return None for nonexistent ID."""
        result = temp_state_db.get_task_by_id("nonexistent-id")
        assert result is None

    def test_get_all_tasks(self, temp_state_db):
        """Retrieve all tasks."""
        urls = [
            "https://example1.com",
            "https://example2.com",
            "https://example3.com",
        ]
        
        for url in urls:
            temp_state_db.create_task(url, HostType.PIXELDRAIN)
        
        all_tasks = temp_state_db.get_all_tasks()
        
        assert len(all_tasks) == 3

    def test_get_tasks_by_status(self, temp_state_db):
        """Retrieve tasks filtered by status."""
        # Create tasks with different statuses
        task1 = temp_state_db.create_task("https://example1.com", HostType.PIXELDRAIN)
        task2 = temp_state_db.create_task("https://example2.com", HostType.PIXELDRAIN)
        task3 = temp_state_db.create_task("https://example3.com", HostType.PIXELDRAIN)
        
        temp_state_db.update_status(task1.id, TaskStatus.DONE)
        temp_state_db.update_status(task2.id, TaskStatus.FAILED, error="Test error")
        # task3 stays PENDING
        
        pending = temp_state_db.get_tasks_by_status(TaskStatus.PENDING)
        done = temp_state_db.get_tasks_by_status(TaskStatus.DONE)
        failed = temp_state_db.get_tasks_by_status(TaskStatus.FAILED)
        
        assert len(pending) == 1
        assert len(done) == 1
        assert len(failed) == 1


class TestStatusUpdates:
    """Tests for task status updates."""

    def test_update_status_basic(self, temp_state_db):
        """Update task status."""
        task = temp_state_db.create_task("https://example.com", HostType.PIXELDRAIN)
        
        temp_state_db.update_status(task.id, TaskStatus.DOWNLOADING)
        
        updated = temp_state_db.get_task_by_id(task.id)
        assert updated.status == TaskStatus.DOWNLOADING

    def test_update_status_with_error(self, temp_state_db):
        """Update status to FAILED with error message."""
        task = temp_state_db.create_task("https://example.com", HostType.PIXELDRAIN)
        
        error_msg = "Connection timeout"
        temp_state_db.update_status(task.id, TaskStatus.FAILED, error=error_msg)
        
        updated = temp_state_db.get_task_by_id(task.id)
        assert updated.status == TaskStatus.FAILED
        assert updated.error == error_msg

    def test_update_status_all_transitions(self, temp_state_db):
        """Test all valid status transitions."""
        task = temp_state_db.create_task("https://example.com", HostType.PIXELDRAIN)
        
        statuses = [
            TaskStatus.DOWNLOADING,
            TaskStatus.EXTRACTING,
            TaskStatus.UPLOADING,
            TaskStatus.DONE,
        ]
        
        for status in statuses:
            temp_state_db.update_status(task.id, status)
            updated = temp_state_db.get_task_by_id(task.id)
            assert updated.status == status

    def test_update_status_nonexistent_task(self, temp_state_db):
        """Updating nonexistent task raises ValueError."""
        with pytest.raises(ValueError, match="Task not found"):
            temp_state_db.update_status("nonexistent-id", TaskStatus.DONE)

    def test_update_status_updates_timestamp(self, temp_state_db):
        """Status update modifies updated_at timestamp."""
        task = temp_state_db.create_task("https://example.com", HostType.PIXELDRAIN)
        original_updated = task.updated_at
        
        import time
        time.sleep(0.01)  # Small delay to ensure different timestamp
        
        temp_state_db.update_status(task.id, TaskStatus.DOWNLOADING)
        
        updated = temp_state_db.get_task_by_id(task.id)
        assert updated.updated_at >= original_updated


class TestOutputPaths:
    """Tests for output path management."""

    def test_add_output_path(self, temp_state_db):
        """Add output path to task."""
        task = temp_state_db.create_task("https://example.com", HostType.PIXELDRAIN)
        
        path1 = "/content/drive/MyDrive/file1.txt"
        temp_state_db.add_output_path(task.id, path1)
        
        updated = temp_state_db.get_task_by_id(task.id)
        assert path1 in updated.output_paths

    def test_add_multiple_output_paths(self, temp_state_db):
        """Add multiple output paths."""
        task = temp_state_db.create_task("https://example.com", HostType.PIXELDRAIN)
        
        paths = [
            "/content/drive/MyDrive/file1.txt",
            "/content/drive/MyDrive/file2.txt",
            "/content/drive/MyDrive/file3.txt",
        ]
        
        for path in paths:
            temp_state_db.add_output_path(task.id, path)
        
        updated = temp_state_db.get_task_by_id(task.id)
        assert len(updated.output_paths) == 3
        for path in paths:
            assert path in updated.output_paths

    def test_add_duplicate_output_path(self, temp_state_db):
        """Adding duplicate path doesn't create duplicates."""
        task = temp_state_db.create_task("https://example.com", HostType.PIXELDRAIN)
        
        path = "/content/drive/MyDrive/file.txt"
        temp_state_db.add_output_path(task.id, path)
        temp_state_db.add_output_path(task.id, path)
        
        updated = temp_state_db.get_task_by_id(task.id)
        assert updated.output_paths.count(path) == 1

    def test_set_output_paths(self, temp_state_db):
        """Set complete list of output paths."""
        task = temp_state_db.create_task("https://example.com", HostType.PIXELDRAIN)
        
        paths = ["/path/to/file1.txt", "/path/to/file2.txt"]
        temp_state_db.set_output_paths(task.id, paths)
        
        updated = temp_state_db.get_task_by_id(task.id)
        assert updated.output_paths == paths

    def test_add_output_path_nonexistent_task(self, temp_state_db):
        """Adding path to nonexistent task raises ValueError."""
        with pytest.raises(ValueError, match="Task not found"):
            temp_state_db.add_output_path("nonexistent-id", "/some/path")


class TestRetryManagement:
    """Tests for retry counter management."""

    def test_increment_retry(self, temp_state_db):
        """Increment retry counter."""
        task = temp_state_db.create_task("https://example.com", HostType.PIXELDRAIN)
        assert task.retries == 0
        
        new_count = temp_state_db.increment_retry(task.id)
        
        assert new_count == 1
        updated = temp_state_db.get_task_by_id(task.id)
        assert updated.retries == 1

    def test_increment_retry_multiple(self, temp_state_db):
        """Increment retry counter multiple times."""
        task = temp_state_db.create_task("https://example.com", HostType.PIXELDRAIN)
        
        for expected in [1, 2, 3]:
            new_count = temp_state_db.increment_retry(task.id)
            assert new_count == expected

    def test_increment_retry_nonexistent_task(self, temp_state_db):
        """Incrementing retry on nonexistent task raises ValueError."""
        with pytest.raises(ValueError, match="Task not found"):
            temp_state_db.increment_retry("nonexistent-id")


class TestPendingAndFailedTasks:
    """Tests for get_pending_and_failed_tasks()."""

    def test_get_pending_only(self, temp_state_db):
        """Get only pending tasks when retry_failed=False."""
        task1 = temp_state_db.create_task("https://example1.com", HostType.PIXELDRAIN)
        task2 = temp_state_db.create_task("https://example2.com", HostType.PIXELDRAIN)
        task3 = temp_state_db.create_task("https://example3.com", HostType.PIXELDRAIN)
        
        temp_state_db.update_status(task2.id, TaskStatus.DONE)
        temp_state_db.update_status(task3.id, TaskStatus.FAILED, error="Error")
        
        pending = temp_state_db.get_pending_and_failed_tasks(retry_failed=False)
        
        assert len(pending) == 1
        assert pending[0].id == task1.id

    def test_get_pending_and_failed(self, temp_state_db):
        """Get both pending and failed tasks when retry_failed=True."""
        task1 = temp_state_db.create_task("https://example1.com", HostType.PIXELDRAIN)
        task2 = temp_state_db.create_task("https://example2.com", HostType.PIXELDRAIN)
        task3 = temp_state_db.create_task("https://example3.com", HostType.PIXELDRAIN)
        
        temp_state_db.update_status(task2.id, TaskStatus.DONE)
        temp_state_db.update_status(task3.id, TaskStatus.FAILED, error="Error")
        
        tasks = temp_state_db.get_pending_and_failed_tasks(retry_failed=True)
        
        assert len(tasks) == 2
        task_ids = [t.id for t in tasks]
        assert task1.id in task_ids
        assert task3.id in task_ids


class TestTaskReset:
    """Tests for task reset functionality."""

    def test_reset_task(self, temp_state_db):
        """Reset failed task to pending."""
        task = temp_state_db.create_task("https://example.com", HostType.PIXELDRAIN)
        temp_state_db.update_status(task.id, TaskStatus.FAILED, error="Some error")
        
        temp_state_db.reset_task(task.id)
        
        updated = temp_state_db.get_task_by_id(task.id)
        assert updated.status == TaskStatus.PENDING
        assert updated.error is None

    def test_reset_preserves_retry_count(self, temp_state_db):
        """Reset preserves the retry count."""
        task = temp_state_db.create_task("https://example.com", HostType.PIXELDRAIN)
        temp_state_db.increment_retry(task.id)
        temp_state_db.increment_retry(task.id)
        temp_state_db.update_status(task.id, TaskStatus.FAILED, error="Error")
        
        temp_state_db.reset_task(task.id)
        
        updated = temp_state_db.get_task_by_id(task.id)
        assert updated.retries == 2

    def test_reset_nonexistent_task(self, temp_state_db):
        """Resetting nonexistent task raises ValueError."""
        with pytest.raises(ValueError, match="Task not found"):
            temp_state_db.reset_task("nonexistent-id")


class TestTaskDeletion:
    """Tests for task deletion."""

    def test_delete_task(self, temp_state_db):
        """Delete existing task."""
        task = temp_state_db.create_task("https://example.com", HostType.PIXELDRAIN)
        
        result = temp_state_db.delete_task(task.id)
        
        assert result is True
        assert temp_state_db.get_task_by_id(task.id) is None

    def test_delete_nonexistent_task(self, temp_state_db):
        """Deleting nonexistent task returns False."""
        result = temp_state_db.delete_task("nonexistent-id")
        assert result is False


class TestStatistics:
    """Tests for task statistics."""

    def test_get_stats_empty(self, temp_state_db):
        """Get stats with no tasks."""
        stats = temp_state_db.get_stats()
        
        assert stats["total"] == 0
        assert stats["pending"] == 0
        assert stats["done"] == 0
        assert stats["failed"] == 0

    def test_get_stats_with_tasks(self, temp_state_db):
        """Get stats with various task statuses."""
        # Create tasks with different statuses
        task1 = temp_state_db.create_task("https://example1.com", HostType.PIXELDRAIN)
        task2 = temp_state_db.create_task("https://example2.com", HostType.PIXELDRAIN)
        task3 = temp_state_db.create_task("https://example3.com", HostType.PIXELDRAIN)
        task4 = temp_state_db.create_task("https://example4.com", HostType.PIXELDRAIN)
        
        temp_state_db.update_status(task2.id, TaskStatus.DONE)
        temp_state_db.update_status(task3.id, TaskStatus.DONE)
        temp_state_db.update_status(task4.id, TaskStatus.FAILED, error="Error")
        
        stats = temp_state_db.get_stats()
        
        assert stats["total"] == 4
        assert stats["pending"] == 1
        assert stats["done"] == 2
        assert stats["failed"] == 1


class TestTaskDataclass:
    """Tests for the Task dataclass."""

    def test_task_to_dict(self, temp_state_db):
        """Convert task to dictionary."""
        task = temp_state_db.create_task("https://example.com", HostType.PIXELDRAIN)
        
        task_dict = task.to_dict()
        
        assert task_dict["url"] == "https://example.com"
        assert task_dict["host"] == "pixeldrain"
        assert task_dict["status"] == "pending"
        assert "id" in task_dict
        assert "created_at" in task_dict
        assert "updated_at" in task_dict

    def test_task_from_row(self, temp_state_db):
        """Task is correctly created from database row."""
        task = temp_state_db.create_task("https://example.com", HostType.PIXELDRAIN)
        
        # Retrieve from database (this uses from_row internally)
        retrieved = temp_state_db.get_task_by_id(task.id)
        
        assert isinstance(retrieved, Task)
        assert retrieved.host == HostType.PIXELDRAIN
        assert retrieved.status == TaskStatus.PENDING
        assert isinstance(retrieved.created_at, datetime)
        assert isinstance(retrieved.updated_at, datetime)
