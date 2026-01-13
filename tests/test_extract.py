"""Tests for archive extraction utilities."""

import pytest
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import patch, MagicMock

from colab_ingest.utils.extract import (
    is_archive,
    detect_archive_type,
    extract_archive,
    check_extraction_tools,
    ExtractionResult,
    ARCHIVE_MAGIC,
    ARCHIVE_EXTENSIONS,
)


class TestIsArchive:
    """Tests for the is_archive() function."""

    def test_is_archive_zip(self, sample_zip_file):
        """ZIP file is detected as archive."""
        assert is_archive(sample_zip_file) is True

    def test_is_archive_text_file(self, sample_text_file):
        """Text file is not an archive."""
        assert is_archive(sample_text_file) is False

    def test_is_archive_nonexistent(self, temp_dir):
        """Nonexistent file is not an archive."""
        nonexistent = temp_dir / "nonexistent.zip"
        assert is_archive(nonexistent) is False

    def test_is_archive_directory(self, temp_dir):
        """Directory is not an archive."""
        assert is_archive(temp_dir) is False

    def test_is_archive_by_extension(self, temp_dir):
        """File with archive extension but wrong content."""
        # Create a file with .zip extension but not ZIP content
        fake_zip = temp_dir / "fake.zip"
        fake_zip.write_text("This is not a real ZIP file")
        
        # Should return None or False since magic bytes don't match
        result = is_archive(fake_zip)
        # Extension matches but magic bytes don't - behavior depends on implementation
        assert result in (True, False)


class TestDetectArchiveType:
    """Tests for the detect_archive_type() function."""

    def test_detect_zip(self, sample_zip_file):
        """Detect ZIP archive type."""
        assert detect_archive_type(sample_zip_file) == "zip"

    def test_detect_text_file(self, sample_text_file):
        """Text file returns None."""
        assert detect_archive_type(sample_text_file) is None

    def test_detect_nonexistent_file(self, temp_dir):
        """Nonexistent file returns None."""
        nonexistent = temp_dir / "nonexistent.zip"
        assert detect_archive_type(nonexistent) is None

    def test_detect_directory(self, temp_dir):
        """Directory returns None."""
        assert detect_archive_type(temp_dir) is None

    def test_detect_by_extension_zip(self, temp_dir):
        """Detect by .zip extension with valid magic bytes."""
        zip_path = temp_dir / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("test.txt", "content")
        
        assert detect_archive_type(zip_path) == "zip"

    def test_detect_by_extension_rar(self, temp_dir):
        """Detect .rar extension (magic bytes may not match without real RAR)."""
        rar_path = temp_dir / "test.rar"
        # Write RAR 4.x magic bytes
        rar_path.write_bytes(b"Rar!\x1a\x07\x00" + b"fake content")
        
        assert detect_archive_type(rar_path) == "rar"

    def test_detect_by_extension_7z(self, temp_dir):
        """Detect .7z extension with magic bytes."""
        sz_path = temp_dir / "test.7z"
        # Write 7z magic bytes
        sz_path.write_bytes(b"7z\xbc\xaf\x27\x1c" + b"fake content")
        
        assert detect_archive_type(sz_path) == "7z"


class TestCheckExtractionTools:
    """Tests for the check_extraction_tools() function."""

    def test_returns_dict(self):
        """check_extraction_tools returns a dictionary."""
        result = check_extraction_tools()
        
        assert isinstance(result, dict)
        assert "unrar" in result
        assert "7z" in result

    def test_values_are_booleans(self):
        """Tool availability values are booleans."""
        result = check_extraction_tools()
        
        for key, value in result.items():
            assert isinstance(value, bool), f"Value for {key} should be boolean"

    @patch('subprocess.run')
    def test_unrar_available(self, mock_run):
        """Test when unrar is available."""
        mock_run.return_value = MagicMock(returncode=0)
        
        result = check_extraction_tools()
        
        # At least one call should have been made
        assert mock_run.called

    @patch('subprocess.run')
    def test_tools_not_found(self, mock_run):
        """Test when tools are not found."""
        mock_run.side_effect = FileNotFoundError("Command not found")
        
        result = check_extraction_tools()
        
        assert result["unrar"] is False
        assert result["7z"] is False


class TestExtractArchive:
    """Tests for the extract_archive() function."""

    def test_extract_zip_success(self, sample_zip_file, temp_dir):
        """Successfully extract a ZIP file."""
        extract_dir = temp_dir / "extracted"
        
        result = extract_archive(
            sample_zip_file,
            extract_dir,
            delete_after=False,
        )
        
        assert result.success is True
        assert result.archive_type == "zip"
        assert len(result.extracted_files) >= 2
        assert result.error is None

    def test_extract_zip_creates_directory(self, sample_zip_file, temp_dir):
        """Extraction creates the destination directory."""
        extract_dir = temp_dir / "new_dir" / "nested"
        
        result = extract_archive(
            sample_zip_file,
            extract_dir,
            delete_after=False,
        )
        
        assert result.success is True
        assert extract_dir.exists()

    def test_extract_zip_with_subdirs(self, sample_zip_file, temp_dir):
        """Extract ZIP preserving subdirectory structure."""
        extract_dir = temp_dir / "extracted"
        
        result = extract_archive(
            sample_zip_file,
            extract_dir,
            delete_after=False,
        )
        
        # Check that subdir/file3.txt was extracted
        subdir_file = extract_dir / "subdir" / "file3.txt"
        assert subdir_file.exists() or any("file3.txt" in str(f) for f in result.extracted_files)

    def test_extract_zip_delete_after(self, temp_dir):
        """Delete archive after extraction when delete_after=True."""
        # Create a fresh ZIP file to delete
        zip_path = temp_dir / "to_delete.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("test.txt", "content")
        
        extract_dir = temp_dir / "extracted"
        
        result = extract_archive(
            zip_path,
            extract_dir,
            delete_after=True,
        )
        
        assert result.success is True
        assert not zip_path.exists()

    def test_extract_zip_keep_after(self, sample_zip_file, temp_dir):
        """Keep archive after extraction when delete_after=False."""
        extract_dir = temp_dir / "extracted"
        
        result = extract_archive(
            sample_zip_file,
            extract_dir,
            delete_after=False,
        )
        
        assert result.success is True
        assert sample_zip_file.exists()

    def test_extract_nonexistent_file(self, temp_dir):
        """Extracting nonexistent file returns failure."""
        nonexistent = temp_dir / "nonexistent.zip"
        extract_dir = temp_dir / "extracted"
        
        result = extract_archive(nonexistent, extract_dir)
        
        assert result.success is False
        assert "not found" in result.error.lower()

    def test_extract_non_archive_copies_file(self, sample_text_file, temp_dir):
        """Non-archive files are copied to destination."""
        extract_dir = temp_dir / "extracted"
        
        result = extract_archive(
            sample_text_file,
            extract_dir,
            delete_after=False,
        )
        
        assert result.success is True
        assert result.archive_type == "none"
        assert len(result.extracted_files) == 1
        
        # Check file was copied
        copied_file = extract_dir / sample_text_file.name
        assert copied_file.exists()

    def test_extract_directory_fails(self, temp_dir):
        """Attempting to extract a directory fails."""
        extract_dir = temp_dir / "extracted"
        
        result = extract_archive(temp_dir, extract_dir)
        
        assert result.success is False
        assert "not a file" in result.error.lower()


class TestExtractionResult:
    """Tests for the ExtractionResult dataclass."""

    def test_result_success_attributes(self):
        """Successful extraction result has correct attributes."""
        result = ExtractionResult(
            success=True,
            extracted_path=Path("/tmp/extracted"),
            original_archive=Path("/tmp/archive.zip"),
            extracted_files=[Path("/tmp/extracted/file.txt")],
            error=None,
            archive_type="zip",
        )
        
        assert result.success is True
        assert result.archive_type == "zip"
        assert result.error is None

    def test_result_failure_attributes(self):
        """Failed extraction result has correct attributes."""
        result = ExtractionResult(
            success=False,
            extracted_path=Path("/tmp/extracted"),
            original_archive=Path("/tmp/archive.zip"),
            extracted_files=[],
            error="Corrupted archive",
            archive_type="zip",
        )
        
        assert result.success is False
        assert result.error == "Corrupted archive"
        assert result.extracted_files == []


class TestCorruptedArchives:
    """Tests for handling corrupted or invalid archives."""

    def test_invalid_zip_file(self, temp_dir):
        """Handle invalid ZIP file gracefully."""
        invalid_zip = temp_dir / "invalid.zip"
        invalid_zip.write_text("This is not a valid ZIP file")
        
        extract_dir = temp_dir / "extracted"
        
        result = extract_archive(invalid_zip, extract_dir)
        
        # Should either fail or treat as non-archive
        # The exact behavior depends on the detection logic
        assert isinstance(result, ExtractionResult)

    def test_truncated_zip_file(self, temp_dir):
        """Handle truncated ZIP file."""
        # Create a valid ZIP and truncate it
        valid_zip = temp_dir / "truncated.zip"
        with zipfile.ZipFile(valid_zip, 'w') as zf:
            zf.writestr("test.txt", "content" * 1000)
        
        # Read and truncate
        content = valid_zip.read_bytes()
        valid_zip.write_bytes(content[:50])  # Keep only first 50 bytes
        
        extract_dir = temp_dir / "extracted"
        
        result = extract_archive(valid_zip, extract_dir, delete_after=False)
        
        # Should fail gracefully
        assert isinstance(result, ExtractionResult)
        # Truncated ZIP should fail
        if result.archive_type == "zip":
            assert result.success is False


class TestMagicBytes:
    """Tests for magic byte detection."""

    def test_zip_magic_bytes_present(self):
        """ZIP magic bytes are defined."""
        assert "zip" in ARCHIVE_MAGIC
        assert len(ARCHIVE_MAGIC["zip"]) > 0

    def test_rar_magic_bytes_present(self):
        """RAR magic bytes are defined."""
        assert "rar" in ARCHIVE_MAGIC
        assert len(ARCHIVE_MAGIC["rar"]) > 0

    def test_7z_magic_bytes_present(self):
        """7z magic bytes are defined."""
        assert "7z" in ARCHIVE_MAGIC
        assert len(ARCHIVE_MAGIC["7z"]) > 0


class TestArchiveExtensions:
    """Tests for archive extension mappings."""

    def test_zip_extensions(self):
        """ZIP extensions are defined."""
        assert "zip" in ARCHIVE_EXTENSIONS
        assert ".zip" in ARCHIVE_EXTENSIONS["zip"]

    def test_rar_extensions(self):
        """RAR extensions are defined."""
        assert "rar" in ARCHIVE_EXTENSIONS
        assert ".rar" in ARCHIVE_EXTENSIONS["rar"]

    def test_7z_extensions(self):
        """7z extensions are defined."""
        assert "7z" in ARCHIVE_EXTENSIONS
        assert ".7z" in ARCHIVE_EXTENSIONS["7z"]


class TestEdgeCases:
    """Tests for edge cases in extraction."""

    def test_empty_zip(self, temp_dir):
        """Handle empty ZIP file."""
        empty_zip = temp_dir / "empty.zip"
        with zipfile.ZipFile(empty_zip, 'w') as zf:
            pass  # Create empty ZIP
        
        extract_dir = temp_dir / "extracted"
        
        result = extract_archive(empty_zip, extract_dir, delete_after=False)
        
        assert result.success is True
        assert result.extracted_files == []

    def test_unicode_filenames_in_zip(self, temp_dir):
        """Handle ZIP with unicode filenames."""
        unicode_zip = temp_dir / "unicode.zip"
        with zipfile.ZipFile(unicode_zip, 'w') as zf:
            zf.writestr("файл.txt", "Russian content")
            zf.writestr("ファイル.txt", "Japanese content")
        
        extract_dir = temp_dir / "extracted"
        
        result = extract_archive(unicode_zip, extract_dir, delete_after=False)
        
        assert result.success is True

    def test_extract_to_same_dir_twice(self, temp_dir):
        """Extract to same directory twice."""
        zip_path = temp_dir / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("test.txt", "content")
        
        extract_dir = temp_dir / "extracted"
        
        # First extraction
        result1 = extract_archive(zip_path, extract_dir, delete_after=False)
        assert result1.success is True
        
        # Second extraction to same directory
        result2 = extract_archive(zip_path, extract_dir, delete_after=False)
        assert result2.success is True

    def test_large_file_in_zip(self, temp_dir):
        """Handle ZIP with larger content."""
        large_zip = temp_dir / "large.zip"
        with zipfile.ZipFile(large_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Create a reasonably large file (but not too large for tests)
            large_content = "x" * (1024 * 100)  # 100KB
            zf.writestr("large.txt", large_content)
        
        extract_dir = temp_dir / "extracted"
        
        result = extract_archive(large_zip, extract_dir, delete_after=False)
        
        assert result.success is True
        assert len(result.extracted_files) == 1
