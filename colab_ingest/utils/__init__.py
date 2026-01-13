"""
colab_ingest.utils - Utility functions and helpers.

This module contains shared utilities for:
- Logging configuration
- File operations
- Archive extraction
- Path handling
- Configuration management
"""

from colab_ingest.utils.logging import (
    setup_logging,
    get_logger,
    get_task_logger,
    TaskLogAdapter,
    mask_sensitive_data,
    mask_url_sensitive_parts,
)
from colab_ingest.utils.paths import WorkdirManager
from colab_ingest.utils.url_detect import (
    HostType,
    detect_host,
    extract_pixeldrain_id,
    extract_buzzheavier_id,
    extract_bunkr_id,
    normalize_bunkr_url,
    is_pixeldrain_list,
    parse_links_file,
    validate_url,
)
from colab_ingest.utils.extract import (
    extract_archive,
    ExtractionResult,
    is_archive,
    detect_archive_type,
    check_extraction_tools,
)
from colab_ingest.utils.upload import (
    upload_to_drive,
    UploadResult,
    check_rsync_available,
)

__all__ = [
    # Logging
    "setup_logging",
    "get_logger",
    "get_task_logger",
    "TaskLogAdapter",
    "mask_sensitive_data",
    "mask_url_sensitive_parts",
    # Paths
    "WorkdirManager",
    # URL Detection
    "HostType",
    "detect_host",
    "extract_pixeldrain_id",
    "extract_buzzheavier_id",
    "extract_bunkr_id",
    "normalize_bunkr_url",
    "is_pixeldrain_list",
    "parse_links_file",
    "validate_url",
    # Extraction
    "extract_archive",
    "ExtractionResult",
    "is_archive",
    "detect_archive_type",
    "check_extraction_tools",
    # Upload
    "upload_to_drive",
    "UploadResult",
    "check_rsync_available",
]
