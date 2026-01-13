"""Tests for path utilities and WorkdirManager."""

import pytest
from pathlib import Path

from colab_ingest.utils.paths import WorkdirManager


class TestWorkdirManagerInitialization:
    """Tests for WorkdirManager initialization."""

    def test_init_with_path(self, temp_dir):
        """Initialize with Path object."""
        manager = WorkdirManager(temp_dir)
        assert manager.workdir == temp_dir.resolve()

    def test_init_with_string(self, temp_dir):
        """Initialize with string path."""
        manager = WorkdirManager(str(temp_dir))
        assert manager.workdir == temp_dir.resolve()

    def test_workdir_is_absolute(self, temp_dir):
        """Workdir is converted to absolute path."""
        manager = WorkdirManager(temp_dir)
        assert manager.workdir.is_absolute()

    def test_repr(self, temp_dir):
        """WorkdirManager has useful string representation."""
        manager = WorkdirManager(temp_dir)
        repr_str = repr(manager)
        
        assert "WorkdirManager" in repr_str
        assert "workdir" in repr_str


class TestDirectoryProperties:
    """Tests for directory property methods."""

    def test_downloads_dir(self, workdir_manager):
        """downloads_dir returns correct path."""
        expected = workdir_manager.workdir / "downloads"
        assert workdir_manager.downloads_dir == expected

    def test_extracted_dir(self, workdir_manager):
        """extracted_dir returns correct path."""
        expected = workdir_manager.workdir / "extracted"
        assert workdir_manager.extracted_dir == expected

    def test_logs_dir(self, workdir_manager):
        """logs_dir returns correct path."""
        expected = workdir_manager.workdir / "logs"
        assert workdir_manager.logs_dir == expected

    def test_state_db_path(self, workdir_manager):
        """state_db_path returns correct path."""
        expected = workdir_manager.workdir / "state.db"
        assert workdir_manager.state_db_path == expected


class TestTaskDirectories:
    """Tests for task-specific directory methods."""

    def test_get_task_download_dir(self, workdir_manager):
        """Get task download directory."""
        task_id = "test-task-123"
        expected = workdir_manager.downloads_dir / task_id
        
        assert workdir_manager.get_task_download_dir(task_id) == expected

    def test_get_task_extract_dir(self, workdir_manager):
        """Get task extraction directory."""
        task_id = "test-task-123"
        expected = workdir_manager.extracted_dir / task_id
        
        assert workdir_manager.get_task_extract_dir(task_id) == expected

    def test_task_dir_with_uuid(self, workdir_manager):
        """Task directory with UUID-style task ID."""
        task_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        
        download_dir = workdir_manager.get_task_download_dir(task_id)
        extract_dir = workdir_manager.get_task_extract_dir(task_id)
        
        assert download_dir.name == task_id
        assert extract_dir.name == task_id


class TestEnsureDirs:
    """Tests for directory creation methods."""

    def test_ensure_dirs_creates_all(self, temp_dir):
        """ensure_dirs creates all required directories."""
        manager = WorkdirManager(temp_dir)
        
        # Directories should not exist yet (fresh temp_dir)
        assert not manager.downloads_dir.exists()
        assert not manager.extracted_dir.exists()
        assert not manager.logs_dir.exists()
        
        manager.ensure_dirs()
        
        assert manager.downloads_dir.exists()
        assert manager.extracted_dir.exists()
        assert manager.logs_dir.exists()

    def test_ensure_dirs_idempotent(self, workdir_manager):
        """Calling ensure_dirs multiple times is safe."""
        # workdir_manager fixture already called ensure_dirs
        
        # Call again - should not raise
        workdir_manager.ensure_dirs()
        
        assert workdir_manager.downloads_dir.exists()
        assert workdir_manager.extracted_dir.exists()
        assert workdir_manager.logs_dir.exists()

    def test_ensure_task_dirs(self, workdir_manager):
        """ensure_task_dirs creates task-specific directories."""
        task_id = "test-task-456"
        
        download_dir, extract_dir = workdir_manager.ensure_task_dirs(task_id)
        
        assert download_dir.exists()
        assert extract_dir.exists()
        assert download_dir == workdir_manager.get_task_download_dir(task_id)
        assert extract_dir == workdir_manager.get_task_extract_dir(task_id)

    def test_ensure_task_dirs_returns_correct_paths(self, workdir_manager):
        """ensure_task_dirs returns tuple of paths."""
        task_id = "my-task"
        
        download_dir, extract_dir = workdir_manager.ensure_task_dirs(task_id)
        
        assert isinstance(download_dir, Path)
        assert isinstance(extract_dir, Path)


class TestCleanup:
    """Tests for cleanup methods."""

    def test_cleanup_task(self, workdir_manager):
        """cleanup_task removes task directories."""
        task_id = "cleanup-test"
        
        # Create task directories with some files
        download_dir, extract_dir = workdir_manager.ensure_task_dirs(task_id)
        (download_dir / "test_file.txt").write_text("test content")
        (extract_dir / "extracted_file.txt").write_text("extracted content")
        
        # Verify files exist
        assert download_dir.exists()
        assert extract_dir.exists()
        
        # Cleanup
        workdir_manager.cleanup_task(task_id)
        
        # Directories should be removed
        assert not download_dir.exists()
        assert not extract_dir.exists()

    def test_cleanup_task_nonexistent(self, workdir_manager):
        """cleanup_task for nonexistent task doesn't raise."""
        task_id = "nonexistent-task"
        
        # Should not raise
        workdir_manager.cleanup_task(task_id)

    def test_cleanup_all_tasks(self, workdir_manager):
        """cleanup_all_tasks removes all task directories."""
        # Create multiple task directories
        for i in range(3):
            task_id = f"task-{i}"
            download_dir, extract_dir = workdir_manager.ensure_task_dirs(task_id)
            (download_dir / f"file_{i}.txt").write_text(f"content {i}")
            (extract_dir / f"extracted_{i}.txt").write_text(f"extracted {i}")
        
        # Verify tasks exist
        assert len(list(workdir_manager.downloads_dir.iterdir())) == 3
        assert len(list(workdir_manager.extracted_dir.iterdir())) == 3
        
        # Cleanup all
        workdir_manager.cleanup_all_tasks()
        
        # All task directories should be removed
        assert len(list(workdir_manager.downloads_dir.iterdir())) == 0
        assert len(list(workdir_manager.extracted_dir.iterdir())) == 0
        
        # But the main directories should still exist
        assert workdir_manager.downloads_dir.exists()
        assert workdir_manager.extracted_dir.exists()


class TestGetTaskFiles:
    """Tests for get_task_files method."""

    def test_get_task_files_downloads(self, workdir_manager):
        """Get files from task download directory."""
        task_id = "files-test"
        download_dir, _ = workdir_manager.ensure_task_dirs(task_id)
        
        # Create some files
        (download_dir / "file1.txt").write_text("content 1")
        (download_dir / "file2.txt").write_text("content 2")
        
        files = workdir_manager.get_task_files(task_id, directory="downloads")
        
        assert len(files) == 2
        filenames = [f.name for f in files]
        assert "file1.txt" in filenames
        assert "file2.txt" in filenames

    def test_get_task_files_extracted(self, workdir_manager):
        """Get files from task extracted directory."""
        task_id = "files-test"
        _, extract_dir = workdir_manager.ensure_task_dirs(task_id)
        
        # Create some files
        (extract_dir / "extracted1.txt").write_text("content 1")
        (extract_dir / "extracted2.txt").write_text("content 2")
        
        files = workdir_manager.get_task_files(task_id, directory="extracted")
        
        assert len(files) == 2

    def test_get_task_files_nested(self, workdir_manager):
        """Get files including nested subdirectories."""
        task_id = "nested-test"
        download_dir, _ = workdir_manager.ensure_task_dirs(task_id)
        
        # Create nested structure
        subdir = download_dir / "subdir"
        subdir.mkdir()
        (download_dir / "root.txt").write_text("root")
        (subdir / "nested.txt").write_text("nested")
        
        files = workdir_manager.get_task_files(task_id, directory="downloads")
        
        assert len(files) == 2
        filenames = [f.name for f in files]
        assert "root.txt" in filenames
        assert "nested.txt" in filenames

    def test_get_task_files_empty(self, workdir_manager):
        """Get files from empty task directory."""
        task_id = "empty-task"
        workdir_manager.ensure_task_dirs(task_id)
        
        files = workdir_manager.get_task_files(task_id, directory="downloads")
        
        assert files == []

    def test_get_task_files_nonexistent_task(self, workdir_manager):
        """Get files from nonexistent task directory."""
        files = workdir_manager.get_task_files("nonexistent", directory="downloads")
        
        assert files == []

    def test_get_task_files_invalid_directory(self, workdir_manager):
        """Get files with invalid directory type raises ValueError."""
        task_id = "test-task"
        workdir_manager.ensure_task_dirs(task_id)
        
        with pytest.raises(ValueError, match="Unknown directory type"):
            workdir_manager.get_task_files(task_id, directory="invalid")


class TestDiskUsage:
    """Tests for disk usage calculation."""

    def test_get_disk_usage_empty(self, workdir_manager):
        """Get disk usage with empty directories."""
        usage = workdir_manager.get_disk_usage()
        
        assert "downloads" in usage
        assert "extracted" in usage
        assert "logs" in usage
        assert "total" in usage
        assert usage["total"] == 0

    def test_get_disk_usage_with_files(self, workdir_manager):
        """Get disk usage with files."""
        # Create some files
        task_id = "disk-usage-test"
        download_dir, extract_dir = workdir_manager.ensure_task_dirs(task_id)
        
        content = "x" * 1000  # 1000 bytes
        (download_dir / "file1.txt").write_text(content)
        (extract_dir / "file2.txt").write_text(content)
        
        usage = workdir_manager.get_disk_usage()
        
        assert usage["downloads"] >= 1000
        assert usage["extracted"] >= 1000
        assert usage["total"] >= 2000


class TestTaskIdValidation:
    """Tests for task ID validation."""

    def test_empty_task_id(self, workdir_manager):
        """Empty task ID raises ValueError."""
        with pytest.raises(ValueError, match="cannot be empty"):
            workdir_manager.get_task_download_dir("")

    def test_whitespace_task_id(self, workdir_manager):
        """Whitespace-only task ID raises ValueError."""
        with pytest.raises(ValueError, match="cannot be whitespace"):
            workdir_manager.get_task_download_dir("   ")

    def test_task_id_with_forward_slash(self, workdir_manager):
        """Task ID with forward slash raises ValueError."""
        with pytest.raises(ValueError, match="invalid character"):
            workdir_manager.get_task_download_dir("task/id")

    def test_task_id_with_backslash(self, workdir_manager):
        """Task ID with backslash raises ValueError."""
        with pytest.raises(ValueError, match="invalid character"):
            workdir_manager.get_task_download_dir("task\\id")

    def test_task_id_with_path_traversal(self, workdir_manager):
        """Task ID with path traversal raises ValueError."""
        with pytest.raises(ValueError, match="invalid character"):
            workdir_manager.get_task_download_dir("../etc/passwd")

    def test_valid_task_ids(self, workdir_manager):
        """Valid task IDs are accepted."""
        valid_ids = [
            "simple-task",
            "task_123",
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "UPPERCASE",
            "MixedCase123",
        ]
        
        for task_id in valid_ids:
            # Should not raise
            path = workdir_manager.get_task_download_dir(task_id)
            assert path.name == task_id


class TestEdgeCases:
    """Tests for edge cases."""

    def test_deeply_nested_workdir(self, temp_dir):
        """WorkdirManager with deeply nested workdir."""
        nested = temp_dir / "a" / "b" / "c" / "d" / "e"
        manager = WorkdirManager(nested)
        manager.ensure_dirs()
        
        assert manager.downloads_dir.exists()
        assert manager.extracted_dir.exists()
        assert manager.logs_dir.exists()

    def test_unicode_in_workdir(self, temp_dir):
        """WorkdirManager with unicode in path."""
        unicode_dir = temp_dir / "工作目录"
        unicode_dir.mkdir()
        
        manager = WorkdirManager(unicode_dir)
        manager.ensure_dirs()
        
        assert manager.downloads_dir.exists()

    def test_special_characters_in_task_id(self, workdir_manager):
        """Task ID with special but valid characters."""
        task_id = "task-with_underscore.and.dots"
        
        download_dir, extract_dir = workdir_manager.ensure_task_dirs(task_id)
        
        assert download_dir.exists()
        assert extract_dir.exists()
        assert download_dir.name == task_id

    def test_cleanup_task_validates_id(self, workdir_manager):
        """cleanup_task also validates task ID."""
        with pytest.raises(ValueError):
            workdir_manager.cleanup_task("")

    def test_ensure_task_dirs_validates_id(self, workdir_manager):
        """ensure_task_dirs also validates task ID."""
        with pytest.raises(ValueError):
            workdir_manager.ensure_task_dirs("../invalid")
