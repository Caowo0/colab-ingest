"""Shared pytest fixtures for colab_ingest tests."""

import pytest
import tempfile
from pathlib import Path


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests.
    
    Yields:
        Path to the temporary directory that is automatically
        cleaned up after the test.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_links_file(temp_dir):
    """Create a sample links file with various URL types.
    
    Args:
        temp_dir: Temporary directory fixture.
        
    Returns:
        Path to the created links file.
    """
    links_content = """# Sample links file for testing
# This is a comment line

# Pixeldrain URLs
https://pixeldrain.com/u/abc12345
https://pixeldrain.com/l/listid12

# Buzzheavier URLs  
https://buzzheavier.com/f/abc123def456
https://bzzhr.co/xyz789abc012

# Bunkr URLs
https://bunkr.si/a/album-name-123
https://bunkr.su/f/file-name-456

# Empty lines are ignored


# Unknown host (should be skipped)
https://unknown-host.com/file123
"""
    links_file = temp_dir / "links.txt"
    links_file.write_text(links_content, encoding="utf-8")
    return links_file


@pytest.fixture
def empty_links_file(temp_dir):
    """Create an empty links file.
    
    Args:
        temp_dir: Temporary directory fixture.
        
    Returns:
        Path to the empty links file.
    """
    links_file = temp_dir / "empty_links.txt"
    links_file.write_text("# Only comments\n# No actual URLs\n", encoding="utf-8")
    return links_file


@pytest.fixture
def temp_state_db(temp_dir):
    """Create a temporary state database.
    
    Args:
        temp_dir: Temporary directory fixture.
        
    Returns:
        Initialized StateDB instance.
    """
    from colab_ingest.core.state import StateDB
    
    db_path = temp_dir / "test_state.db"
    state_db = StateDB(db_path)
    state_db.init_db()
    return state_db


@pytest.fixture
def sample_zip_file(temp_dir):
    """Create a sample ZIP file for extraction tests.
    
    Args:
        temp_dir: Temporary directory fixture.
        
    Returns:
        Path to the created ZIP file.
    """
    import zipfile
    
    zip_path = temp_dir / "sample.zip"
    
    # Create a ZIP file with some test content
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("file1.txt", "Content of file 1")
        zf.writestr("file2.txt", "Content of file 2")
        zf.writestr("subdir/file3.txt", "Content of file 3 in subdir")
    
    return zip_path


@pytest.fixture
def sample_text_file(temp_dir):
    """Create a sample text file (non-archive).
    
    Args:
        temp_dir: Temporary directory fixture.
        
    Returns:
        Path to the created text file.
    """
    text_path = temp_dir / "sample.txt"
    text_path.write_text("This is a sample text file content.", encoding="utf-8")
    return text_path


@pytest.fixture
def workdir_manager(temp_dir):
    """Create a WorkdirManager with a temporary directory.
    
    Args:
        temp_dir: Temporary directory fixture.
        
    Returns:
        Initialized WorkdirManager instance.
    """
    from colab_ingest.utils.paths import WorkdirManager
    
    manager = WorkdirManager(temp_dir)
    manager.ensure_dirs()
    return manager
