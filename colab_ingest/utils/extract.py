"""Archive extraction utilities for colab_ingest.

This module provides utilities for extracting various archive formats
including ZIP, RAR, and 7z files. It supports native Python extraction
for ZIP files and subprocess calls for RAR and 7z.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

# Magic bytes for archive detection
ARCHIVE_MAGIC = {
    "zip": [
        b"PK\x03\x04",  # Standard ZIP
        b"PK\x05\x06",  # Empty ZIP
        b"PK\x07\x08",  # Spanned ZIP
    ],
    "rar": [
        b"Rar!\x1a\x07\x00",  # RAR 4.x
        b"Rar!\x1a\x07\x01\x00",  # RAR 5.x
    ],
    "7z": [
        b"7z\xbc\xaf\x27\x1c",  # 7z signature
    ],
}

# File extensions for archive types
ARCHIVE_EXTENSIONS = {
    "zip": [".zip"],
    "rar": [".rar"],
    "7z": [".7z"],
}


@dataclass
class ExtractionResult:
    """Result of an archive extraction operation.

    Attributes:
        success: Whether the extraction completed successfully.
        extracted_path: Directory containing the extracted files.
        original_archive: Path to the original archive file.
        extracted_files: List of paths to all extracted files.
        error: Error message if extraction failed, None otherwise.
        archive_type: Type of archive (zip, rar, 7z, or "none" for non-archives).
    """

    success: bool
    extracted_path: Path
    original_archive: Path
    extracted_files: List[Path] = field(default_factory=list)
    error: Optional[str] = None
    archive_type: str = "none"


def is_archive(file_path: Path) -> bool:
    """Check if file is a supported archive format.

    Checks both file extension and magic bytes to determine if a file
    is a supported archive format.

    Args:
        file_path: Path to the file to check.

    Returns:
        True if the file is a supported archive format, False otherwise.
    """
    return detect_archive_type(file_path) is not None


def detect_archive_type(file_path: Path) -> Optional[str]:
    """Detect archive type by extension and magic bytes.

    Attempts to detect the archive type by first checking the file
    extension, then verifying with magic bytes for more accuracy.

    Args:
        file_path: Path to the file to check.

    Returns:
        Archive type string ("zip", "rar", "7z") if detected, None otherwise.
    """
    if not file_path.exists() or not file_path.is_file():
        return None

    # First check by extension
    suffix = file_path.suffix.lower()
    detected_type = None

    for archive_type, extensions in ARCHIVE_EXTENSIONS.items():
        if suffix in extensions:
            detected_type = archive_type
            break

    # Verify with magic bytes if we have a candidate
    try:
        with open(file_path, "rb") as f:
            header = f.read(16)  # Read enough bytes for all signatures

        # If extension matched, verify with magic bytes
        if detected_type:
            for magic in ARCHIVE_MAGIC[detected_type]:
                if header.startswith(magic):
                    return detected_type

        # If extension didn't match or magic didn't verify, try all types
        for archive_type, magic_list in ARCHIVE_MAGIC.items():
            for magic in magic_list:
                if header.startswith(magic):
                    return archive_type

    except (OSError, IOError):
        # If we can't read the file, fall back to extension only
        return detected_type

    # Return extension-based detection if no magic match
    return detected_type


def check_extraction_tools() -> Dict[str, bool]:
    """Check which extraction tools are available on the system.

    Checks for the presence of unrar and 7z command-line tools.

    Returns:
        Dictionary mapping tool names to availability status.
        Keys: "unrar", "7z"
    """
    tools = {"unrar": False, "7z": False}

    # Check for unrar
    try:
        result = subprocess.run(
            ["unrar", "--version"],
            capture_output=True,
            timeout=10,
        )
        tools["unrar"] = result.returncode == 0
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        tools["unrar"] = False

    # Check for 7z
    try:
        result = subprocess.run(
            ["7z", "--help"],
            capture_output=True,
            timeout=10,
        )
        # 7z returns 0 on --help
        tools["7z"] = result.returncode == 0
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        tools["7z"] = False

    return tools


def _extract_zip(
    archive_path: Path,
    extract_to: Path,
    logger: Optional[logging.Logger] = None,
) -> ExtractionResult:
    """Extract a ZIP archive using Python's zipfile module.

    Args:
        archive_path: Path to the ZIP archive.
        extract_to: Directory to extract files into.
        logger: Optional logger for progress messages.

    Returns:
        ExtractionResult with extraction status and file list.
    """
    log = logger or logging.getLogger(__name__)
    extracted_files: List[Path] = []

    try:
        with zipfile.ZipFile(archive_path, "r") as zf:
            # Check for corrupted archive
            bad_file = zf.testzip()
            if bad_file:
                return ExtractionResult(
                    success=False,
                    extracted_path=extract_to,
                    original_archive=archive_path,
                    extracted_files=[],
                    error=f"Corrupted file in archive: {bad_file}",
                    archive_type="zip",
                )

            # Get list of files
            file_list = zf.namelist()
            log.debug(f"ZIP contains {len(file_list)} entries")

            # Extract all files
            for member in file_list:
                zf.extract(member, extract_to)
                extracted_path = extract_to / member
                if extracted_path.is_file():
                    extracted_files.append(extracted_path)
                    log.debug(f"Extracted: {member}")

        log.info(f"Successfully extracted {len(extracted_files)} files from ZIP")
        return ExtractionResult(
            success=True,
            extracted_path=extract_to,
            original_archive=archive_path,
            extracted_files=extracted_files,
            error=None,
            archive_type="zip",
        )

    except zipfile.BadZipFile as e:
        log.error(f"Invalid or corrupted ZIP file: {e}")
        return ExtractionResult(
            success=False,
            extracted_path=extract_to,
            original_archive=archive_path,
            extracted_files=[],
            error=f"Invalid or corrupted ZIP file: {e}",
            archive_type="zip",
        )
    except Exception as e:
        log.error(f"ZIP extraction failed: {e}")
        return ExtractionResult(
            success=False,
            extracted_path=extract_to,
            original_archive=archive_path,
            extracted_files=[],
            error=f"ZIP extraction failed: {e}",
            archive_type="zip",
        )


def _extract_rar(
    archive_path: Path,
    extract_to: Path,
    logger: Optional[logging.Logger] = None,
) -> ExtractionResult:
    """Extract a RAR archive using unrar command.

    Args:
        archive_path: Path to the RAR archive.
        extract_to: Directory to extract files into.
        logger: Optional logger for progress messages.

    Returns:
        ExtractionResult with extraction status and file list.
    """
    log = logger or logging.getLogger(__name__)

    # Check if unrar is available
    tools = check_extraction_tools()
    if not tools["unrar"]:
        return ExtractionResult(
            success=False,
            extracted_path=extract_to,
            original_archive=archive_path,
            extracted_files=[],
            error="unrar not installed. Install with: apt-get install unrar",
            archive_type="rar",
        )

    try:
        # unrar x <archive> <destination>/
        # The trailing slash on destination is important
        cmd = [
            "unrar",
            "x",  # Extract with full paths
            "-o+",  # Overwrite existing files
            "-y",  # Yes to all queries
            str(archive_path),
            str(extract_to) + "/",
        ]

        log.debug(f"Running: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout for large archives
        )

        if result.returncode != 0:
            log.error(f"unrar failed: {result.stderr}")
            return ExtractionResult(
                success=False,
                extracted_path=extract_to,
                original_archive=archive_path,
                extracted_files=[],
                error=f"unrar failed: {result.stderr}",
                archive_type="rar",
            )

        # Collect extracted files
        extracted_files = [f for f in extract_to.rglob("*") if f.is_file()]
        log.info(f"Successfully extracted {len(extracted_files)} files from RAR")

        return ExtractionResult(
            success=True,
            extracted_path=extract_to,
            original_archive=archive_path,
            extracted_files=extracted_files,
            error=None,
            archive_type="rar",
        )

    except subprocess.TimeoutExpired:
        log.error("RAR extraction timed out")
        return ExtractionResult(
            success=False,
            extracted_path=extract_to,
            original_archive=archive_path,
            extracted_files=[],
            error="RAR extraction timed out after 1 hour",
            archive_type="rar",
        )
    except Exception as e:
        log.error(f"RAR extraction failed: {e}")
        return ExtractionResult(
            success=False,
            extracted_path=extract_to,
            original_archive=archive_path,
            extracted_files=[],
            error=f"RAR extraction failed: {e}",
            archive_type="rar",
        )


def _extract_7z(
    archive_path: Path,
    extract_to: Path,
    logger: Optional[logging.Logger] = None,
) -> ExtractionResult:
    """Extract a 7z archive using 7z command.

    Args:
        archive_path: Path to the 7z archive.
        extract_to: Directory to extract files into.
        logger: Optional logger for progress messages.

    Returns:
        ExtractionResult with extraction status and file list.
    """
    log = logger or logging.getLogger(__name__)

    # Check if 7z is available
    tools = check_extraction_tools()
    if not tools["7z"]:
        return ExtractionResult(
            success=False,
            extracted_path=extract_to,
            original_archive=archive_path,
            extracted_files=[],
            error="7z not installed. Install with: apt-get install p7zip-full",
            archive_type="7z",
        )

    try:
        # 7z x <archive> -o<destination>
        # Note: no space between -o and destination path
        cmd = [
            "7z",
            "x",  # Extract with full paths
            "-y",  # Yes to all queries
            f"-o{extract_to}",
            str(archive_path),
        ]

        log.debug(f"Running: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout for large archives
        )

        if result.returncode != 0:
            log.error(f"7z failed: {result.stderr}")
            return ExtractionResult(
                success=False,
                extracted_path=extract_to,
                original_archive=archive_path,
                extracted_files=[],
                error=f"7z failed: {result.stderr}",
                archive_type="7z",
            )

        # Collect extracted files
        extracted_files = [f for f in extract_to.rglob("*") if f.is_file()]
        log.info(f"Successfully extracted {len(extracted_files)} files from 7z")

        return ExtractionResult(
            success=True,
            extracted_path=extract_to,
            original_archive=archive_path,
            extracted_files=extracted_files,
            error=None,
            archive_type="7z",
        )

    except subprocess.TimeoutExpired:
        log.error("7z extraction timed out")
        return ExtractionResult(
            success=False,
            extracted_path=extract_to,
            original_archive=archive_path,
            extracted_files=[],
            error="7z extraction timed out after 1 hour",
            archive_type="7z",
        )
    except Exception as e:
        log.error(f"7z extraction failed: {e}")
        return ExtractionResult(
            success=False,
            extracted_path=extract_to,
            original_archive=archive_path,
            extracted_files=[],
            error=f"7z extraction failed: {e}",
            archive_type="7z",
        )


def _handle_non_archive(
    file_path: Path,
    extract_to: Path,
    logger: Optional[logging.Logger] = None,
) -> ExtractionResult:
    """Handle a non-archive file by copying/moving it to the destination.

    Args:
        file_path: Path to the file.
        extract_to: Directory to copy the file into.
        logger: Optional logger for progress messages.

    Returns:
        ExtractionResult with the copied file.
    """
    log = logger or logging.getLogger(__name__)

    try:
        # Ensure destination directory exists
        extract_to.mkdir(parents=True, exist_ok=True)

        # Copy the file to destination
        dest_file = extract_to / file_path.name
        shutil.copy2(file_path, dest_file)

        log.info(f"Copied non-archive file to: {dest_file}")

        return ExtractionResult(
            success=True,
            extracted_path=extract_to,
            original_archive=file_path,
            extracted_files=[dest_file],
            error=None,
            archive_type="none",
        )

    except Exception as e:
        log.error(f"Failed to copy file: {e}")
        return ExtractionResult(
            success=False,
            extracted_path=extract_to,
            original_archive=file_path,
            extracted_files=[],
            error=f"Failed to copy file: {e}",
            archive_type="none",
        )


def extract_archive(
    archive_path: Path,
    extract_to: Path,
    delete_after: bool = True,
    logger: Optional[logging.Logger] = None,
) -> ExtractionResult:
    """Extract an archive to the specified destination.

    Supports ZIP, RAR, and 7z archives. For non-archive files, copies
    the file to the destination directory.

    Args:
        archive_path: Path to the archive file.
        extract_to: Directory to extract files into.
        delete_after: If True, delete the original archive after successful
            extraction. Defaults to True.
        logger: Optional logger for progress messages.

    Returns:
        ExtractionResult containing extraction status, file list, and any errors.

    Example:
        >>> result = extract_archive(Path("data.zip"), Path("output/"))
        >>> if result.success:
        ...     print(f"Extracted {len(result.extracted_files)} files")
        ... else:
        ...     print(f"Extraction failed: {result.error}")
    """
    log = logger or logging.getLogger(__name__)

    # Validate input
    if not archive_path.exists():
        return ExtractionResult(
            success=False,
            extracted_path=extract_to,
            original_archive=archive_path,
            extracted_files=[],
            error=f"File not found: {archive_path}",
            archive_type="none",
        )

    if not archive_path.is_file():
        return ExtractionResult(
            success=False,
            extracted_path=extract_to,
            original_archive=archive_path,
            extracted_files=[],
            error=f"Not a file: {archive_path}",
            archive_type="none",
        )

    # Ensure extraction directory exists
    extract_to.mkdir(parents=True, exist_ok=True)

    # Detect archive type
    archive_type = detect_archive_type(archive_path)
    log.info(f"Processing: {archive_path.name} (type: {archive_type or 'non-archive'})")

    # Extract based on type
    if archive_type == "zip":
        result = _extract_zip(archive_path, extract_to, log)
    elif archive_type == "rar":
        result = _extract_rar(archive_path, extract_to, log)
    elif archive_type == "7z":
        result = _extract_7z(archive_path, extract_to, log)
    else:
        # Not an archive - copy the file
        result = _handle_non_archive(archive_path, extract_to, log)

    # Delete original archive after successful extraction
    if result.success and delete_after:
        try:
            archive_path.unlink()
            log.debug(f"Deleted original archive: {archive_path}")
        except OSError as e:
            log.warning(f"Failed to delete archive after extraction: {e}")
            # Don't fail the extraction if cleanup fails

    return result
