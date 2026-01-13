"""URL detection and parsing utilities for supported hosting services.

This module provides functions to detect and parse URLs from various file
hosting services: pixeldrain, buzzheavier, and bunkr.
"""

from __future__ import annotations

import re
from enum import Enum
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse


class HostType(Enum):
    """Enumeration of supported file hosting services."""

    PIXELDRAIN = "pixeldrain"
    BUZZHEAVIER = "buzzheavier"
    BUNKR = "bunkr"
    UNKNOWN = "unknown"


# Regex patterns for URL matching
PIXELDRAIN_PATTERNS = [
    re.compile(r"pixeldrain\.com/[ul]/([a-zA-Z0-9]+)", re.IGNORECASE),
    re.compile(r"^([a-zA-Z0-9]{8})$"),  # Raw 8-char ID
]

BUZZHEAVIER_PATTERNS = [
    re.compile(r"buzzheavier\.com/f?/?([a-zA-Z0-9]+)", re.IGNORECASE),
    re.compile(r"bzzhr\.co/([a-zA-Z0-9]+)", re.IGNORECASE),
    re.compile(r"^([a-zA-Z0-9]{12})$"),  # Raw 12-char ID
]

BUNKR_PATTERNS = [
    re.compile(r"bunkr+\.(?:si|su|la|ru|is|to|sk|ac|black|red|cat|ws|fi|ph)/[af]/([a-zA-Z0-9-]+)", re.IGNORECASE),
    re.compile(r"bunkr+\.(?:si|su|la|ru|is|to|sk|ac|black|red|cat|ws|fi|ph)/v/([a-zA-Z0-9-]+)", re.IGNORECASE),
    re.compile(r"bunkr+\.(?:si|su|la|ru|is|to|sk|ac|black|red|cat|ws|fi|ph)/d/([a-zA-Z0-9-]+)", re.IGNORECASE),
    re.compile(r"bunkr+\.(?:si|su|la|ru|is|to|sk|ac|black|red|cat|ws|fi|ph)/i/([a-zA-Z0-9-]+)", re.IGNORECASE),
]


def detect_host(url: str) -> HostType:
    """Detect the hosting service from a URL.

    Args:
        url: URL string to analyze.

    Returns:
        HostType enum value indicating the detected service.

    Examples:
        >>> detect_host("https://pixeldrain.com/u/abc12345")
        HostType.PIXELDRAIN
        >>> detect_host("https://buzzheavier.com/f/abc123def456")
        HostType.BUZZHEAVIER
        >>> detect_host("https://bunkr.si/a/album-name")
        HostType.BUNKR
        >>> detect_host("https://unknown-site.com/file")
        HostType.UNKNOWN
    """
    if not url or not url.strip():
        return HostType.UNKNOWN

    url = url.strip()
    url_lower = url.lower()

    # Check for pixeldrain
    if "pixeldrain" in url_lower:
        return HostType.PIXELDRAIN

    # Check for buzzheavier or bzzhr.co
    if "buzzheavier" in url_lower or "bzzhr.co" in url_lower:
        return HostType.BUZZHEAVIER

    # Check for bunkr (various TLDs)
    if re.search(r"bunkr+\.(?:si|su|la|ru|is|to|sk|ac|black|red|cat|ws|fi|ph)", url_lower):
        return HostType.BUNKR

    # Try to detect by ID pattern matching
    # Pixeldrain IDs are typically 8 characters
    if re.match(r"^[a-zA-Z0-9]{8}$", url):
        return HostType.PIXELDRAIN

    # Buzzheavier IDs are typically 12 characters
    if re.match(r"^[a-zA-Z0-9]{12}$", url):
        return HostType.BUZZHEAVIER

    return HostType.UNKNOWN


def extract_pixeldrain_id(url: str) -> Optional[str]:
    """Extract the file or list ID from a pixeldrain URL.

    Handles the following formats:
    - https://pixeldrain.com/u/<id> (single file)
    - https://pixeldrain.com/l/<id> (file list)
    - Raw 8-character ID

    Args:
        url: Pixeldrain URL or raw ID.

    Returns:
        Extracted ID string, or None if not a valid pixeldrain URL.

    Examples:
        >>> extract_pixeldrain_id("https://pixeldrain.com/u/abc12345")
        'abc12345'
        >>> extract_pixeldrain_id("https://pixeldrain.com/l/listid12")
        'listid12'
        >>> extract_pixeldrain_id("abc12345")
        'abc12345'
    """
    if not url or not url.strip():
        return None

    url = url.strip()

    # Try URL patterns first
    for pattern in PIXELDRAIN_PATTERNS:
        match = pattern.search(url)
        if match:
            return match.group(1)

    return None


def is_pixeldrain_list(url: str) -> bool:
    """Check if a pixeldrain URL is a list (multiple files).

    Args:
        url: Pixeldrain URL to check.

    Returns:
        True if the URL is a list (/l/), False otherwise.
    """
    if not url:
        return False

    return bool(re.search(r"pixeldrain\.com/l/", url, re.IGNORECASE))


def extract_buzzheavier_id(url: str) -> Optional[str]:
    """Extract the file ID from a buzzheavier URL.

    Handles the following formats:
    - https://buzzheavier.com/<id>
    - https://buzzheavier.com/f/<id>
    - https://bzzhr.co/<id>
    - Raw 12-character ID

    Args:
        url: Buzzheavier URL or raw ID.

    Returns:
        Extracted ID string, or None if not a valid buzzheavier URL.

    Examples:
        >>> extract_buzzheavier_id("https://buzzheavier.com/abc123def456")
        'abc123def456'
        >>> extract_buzzheavier_id("https://bzzhr.co/abc123def456")
        'abc123def456'
        >>> extract_buzzheavier_id("abc123def456")
        'abc123def456'
    """
    if not url or not url.strip():
        return None

    url = url.strip()

    # Try URL patterns
    for pattern in BUZZHEAVIER_PATTERNS:
        match = pattern.search(url)
        if match:
            return match.group(1)

    return None


def normalize_bunkr_url(url: str) -> str:
    """Normalize a bunkr URL to a standard format.

    Ensures the URL has proper format for albums (/a/) and files (/f/).
    Handles various bunkr domain TLDs.

    Args:
        url: Bunkr URL to normalize.

    Returns:
        Normalized bunkr URL string.

    Examples:
        >>> normalize_bunkr_url("https://bunkr.si/a/album-name")
        'https://bunkr.si/a/album-name'
        >>> normalize_bunkr_url("bunkr.si/a/album-name")
        'https://bunkr.si/a/album-name'
    """
    if not url or not url.strip():
        return url

    url = url.strip()

    # Add https:// if missing
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    # Parse and validate
    try:
        parsed = urlparse(url)

        # Check if it's a bunkr domain
        if not re.match(r"bunkr+\.(?:si|su|la|ru|is|to|sk|ac|black|red|cat|ws|fi|ph)", parsed.netloc, re.IGNORECASE):
            return url

        # Ensure path starts with /a/, /f/, /v/, /d/, or /i/
        path = parsed.path
        if path and not path.startswith(("/a/", "/f/", "/v/", "/d/", "/i/")):
            # Try to extract album/file identifier and default to album
            path_parts = path.strip("/").split("/")
            if path_parts and path_parts[0]:
                # Assume it's an album if no type specified
                path = "/a/" + path_parts[0]

        # Reconstruct URL
        return f"https://{parsed.netloc}{path}"

    except Exception:
        return url


def extract_bunkr_id(url: str) -> Optional[str]:
    """Extract the album or file ID from a bunkr URL.

    Args:
        url: Bunkr URL to parse.

    Returns:
        Extracted ID/slug string, or None if not a valid bunkr URL.
    """
    if not url or not url.strip():
        return None

    url = url.strip()

    # Try all bunkr patterns
    for pattern in BUNKR_PATTERNS:
        match = pattern.search(url)
        if match:
            return match.group(1)

    return None


def parse_links_file(filepath: Path) -> list[tuple[str, HostType, str]]:
    """Parse a file containing URLs, one per line.

    Reads a text file with URLs (one per line), detects the host type
    for each, and extracts the relevant ID or normalized URL.

    Args:
        filepath: Path to the links file.

    Returns:
        List of tuples: (original_url, host_type, extracted_id_or_url).
        For unknown hosts, the third element is the original URL.
        Empty lines and lines starting with # are skipped.

    Raises:
        FileNotFoundError: If the file doesn't exist.
        PermissionError: If the file can't be read.

    Examples:
        >>> parse_links_file(Path("links.txt"))
        [
            ('https://pixeldrain.com/u/abc123', HostType.PIXELDRAIN, 'abc123'),
            ('https://bunkr.si/a/album', HostType.BUNKR, 'https://bunkr.si/a/album'),
        ]
    """
    if not filepath.exists():
        raise FileNotFoundError(f"Links file not found: {filepath}")

    results: list[tuple[str, HostType, str]] = []

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue

            host_type = detect_host(line)

            if host_type == HostType.PIXELDRAIN:
                extracted = extract_pixeldrain_id(line)
                results.append((line, host_type, extracted or line))

            elif host_type == HostType.BUZZHEAVIER:
                extracted = extract_buzzheavier_id(line)
                results.append((line, host_type, extracted or line))

            elif host_type == HostType.BUNKR:
                normalized = normalize_bunkr_url(line)
                results.append((line, host_type, normalized))

            else:
                # Unknown host - keep original URL
                results.append((line, host_type, line))

    return results


def validate_url(url: str) -> tuple[bool, Optional[str]]:
    """Validate a URL and return validation result with error message.

    Args:
        url: URL to validate.

    Returns:
        Tuple of (is_valid, error_message). error_message is None if valid.
    """
    if not url or not url.strip():
        return False, "URL is empty"

    url = url.strip()
    host_type = detect_host(url)

    if host_type == HostType.UNKNOWN:
        return False, f"Unsupported host for URL: {url}"

    # Validate extracted ID based on host
    if host_type == HostType.PIXELDRAIN:
        if not extract_pixeldrain_id(url):
            return False, f"Could not extract pixeldrain ID from: {url}"

    elif host_type == HostType.BUZZHEAVIER:
        if not extract_buzzheavier_id(url):
            return False, f"Could not extract buzzheavier ID from: {url}"

    elif host_type == HostType.BUNKR:
        if not extract_bunkr_id(url):
            return False, f"Could not extract bunkr ID from: {url}"

    return True, None
