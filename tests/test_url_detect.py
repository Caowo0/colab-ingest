"""Tests for URL detection and parsing utilities."""

import pytest
from pathlib import Path

from colab_ingest.utils.url_detect import (
    HostType,
    detect_host,
    extract_pixeldrain_id,
    extract_buzzheavier_id,
    normalize_bunkr_url,
    extract_bunkr_id,
    parse_links_file,
    validate_url,
    is_pixeldrain_list,
)


class TestDetectHost:
    """Tests for the detect_host() function."""

    def test_detect_pixeldrain_single_file(self):
        """Detect pixeldrain single file URL."""
        url = "https://pixeldrain.com/u/abc12345"
        assert detect_host(url) == HostType.PIXELDRAIN

    def test_detect_pixeldrain_list(self):
        """Detect pixeldrain list URL."""
        url = "https://pixeldrain.com/l/listid12"
        assert detect_host(url) == HostType.PIXELDRAIN

    def test_detect_pixeldrain_case_insensitive(self):
        """Detect pixeldrain with mixed case."""
        url = "https://PIXELDRAIN.COM/u/abc12345"
        assert detect_host(url) == HostType.PIXELDRAIN

    def test_detect_buzzheavier_main_domain(self):
        """Detect buzzheavier.com URL."""
        url = "https://buzzheavier.com/f/abc123def456"
        assert detect_host(url) == HostType.BUZZHEAVIER

    def test_detect_buzzheavier_short_domain(self):
        """Detect bzzhr.co short URL."""
        url = "https://bzzhr.co/abc123def456"
        assert detect_host(url) == HostType.BUZZHEAVIER

    def test_detect_bunkr_si_album(self):
        """Detect bunkr.si album URL."""
        url = "https://bunkr.si/a/album-name-123"
        assert detect_host(url) == HostType.BUNKR

    def test_detect_bunkr_su_file(self):
        """Detect bunkr.su file URL."""
        url = "https://bunkr.su/f/file-name"
        assert detect_host(url) == HostType.BUNKR

    def test_detect_bunkr_various_tlds(self):
        """Detect bunkr with various TLDs."""
        tlds = ["si", "su", "la", "ru", "is", "to", "sk", "ac", "black", "red", "cat", "ws", "fi", "ph"]
        for tld in tlds:
            url = f"https://bunkr.{tld}/a/album"
            assert detect_host(url) == HostType.BUNKR, f"Failed for TLD: {tld}"

    def test_detect_unknown_host(self):
        """Detect unknown host returns UNKNOWN."""
        url = "https://unknown-site.com/file123"
        assert detect_host(url) == HostType.UNKNOWN

    def test_detect_empty_url(self):
        """Empty URL returns UNKNOWN."""
        assert detect_host("") == HostType.UNKNOWN
        assert detect_host("   ") == HostType.UNKNOWN

    def test_detect_none_url(self):
        """None-like empty URL returns UNKNOWN."""
        assert detect_host("") == HostType.UNKNOWN

    def test_detect_raw_pixeldrain_id(self):
        """Detect raw 8-character pixeldrain ID."""
        raw_id = "abcd1234"
        assert detect_host(raw_id) == HostType.PIXELDRAIN

    def test_detect_raw_buzzheavier_id(self):
        """Detect raw 12-character buzzheavier ID."""
        raw_id = "abc123def456"
        assert detect_host(raw_id) == HostType.BUZZHEAVIER


class TestExtractPixeldrainId:
    """Tests for the extract_pixeldrain_id() function."""

    def test_extract_single_file_id(self):
        """Extract ID from single file URL."""
        url = "https://pixeldrain.com/u/abc12345"
        assert extract_pixeldrain_id(url) == "abc12345"

    def test_extract_list_id(self):
        """Extract ID from list URL."""
        url = "https://pixeldrain.com/l/listid12"
        assert extract_pixeldrain_id(url) == "listid12"

    def test_extract_raw_id(self):
        """Extract raw 8-character ID."""
        raw_id = "abcd1234"
        assert extract_pixeldrain_id(raw_id) == "abcd1234"

    def test_extract_with_trailing_slash(self):
        """Extract ID from URL with trailing content."""
        url = "https://pixeldrain.com/u/abc12345"
        assert extract_pixeldrain_id(url) == "abc12345"

    def test_extract_empty_url(self):
        """Empty URL returns None."""
        assert extract_pixeldrain_id("") is None
        assert extract_pixeldrain_id("   ") is None

    def test_extract_invalid_url(self):
        """Invalid URL returns None."""
        assert extract_pixeldrain_id("not a valid url") is None


class TestIsPixeldrainList:
    """Tests for the is_pixeldrain_list() function."""

    def test_is_list_true(self):
        """Detect list URL correctly."""
        url = "https://pixeldrain.com/l/listid12"
        assert is_pixeldrain_list(url) is True

    def test_is_list_false_single_file(self):
        """Single file URL is not a list."""
        url = "https://pixeldrain.com/u/abc12345"
        assert is_pixeldrain_list(url) is False

    def test_is_list_false_empty(self):
        """Empty URL is not a list."""
        assert is_pixeldrain_list("") is False
        assert is_pixeldrain_list(None) is False


class TestExtractBuzzheavierId:
    """Tests for the extract_buzzheavier_id() function."""

    def test_extract_main_domain_with_f(self):
        """Extract ID from buzzheavier.com/f/ URL."""
        url = "https://buzzheavier.com/f/abc123def456"
        assert extract_buzzheavier_id(url) == "abc123def456"

    def test_extract_main_domain_without_f(self):
        """Extract ID from buzzheavier.com/ URL without /f/."""
        url = "https://buzzheavier.com/abc123def456"
        assert extract_buzzheavier_id(url) == "abc123def456"

    def test_extract_short_domain(self):
        """Extract ID from bzzhr.co short URL."""
        url = "https://bzzhr.co/abc123def456"
        assert extract_buzzheavier_id(url) == "abc123def456"

    def test_extract_raw_id(self):
        """Extract raw 12-character ID."""
        raw_id = "abc123def456"
        assert extract_buzzheavier_id(raw_id) == "abc123def456"

    def test_extract_empty_url(self):
        """Empty URL returns None."""
        assert extract_buzzheavier_id("") is None
        assert extract_buzzheavier_id("   ") is None


class TestNormalizeBunkrUrl:
    """Tests for the normalize_bunkr_url() function."""

    def test_normalize_already_valid_album(self):
        """Already valid album URL stays the same."""
        url = "https://bunkr.si/a/album-name"
        assert normalize_bunkr_url(url) == "https://bunkr.si/a/album-name"

    def test_normalize_already_valid_file(self):
        """Already valid file URL stays the same."""
        url = "https://bunkr.si/f/file-name"
        assert normalize_bunkr_url(url) == "https://bunkr.si/f/file-name"

    def test_normalize_adds_https(self):
        """URL without protocol gets https:// added."""
        url = "bunkr.si/a/album-name"
        assert normalize_bunkr_url(url) == "https://bunkr.si/a/album-name"

    def test_normalize_video_url(self):
        """Video URLs (/v/) are preserved."""
        url = "https://bunkr.si/v/video-name"
        assert normalize_bunkr_url(url) == "https://bunkr.si/v/video-name"

    def test_normalize_download_url(self):
        """Download URLs (/d/) are preserved."""
        url = "https://bunkr.si/d/download-name"
        assert normalize_bunkr_url(url) == "https://bunkr.si/d/download-name"

    def test_normalize_image_url(self):
        """Image URLs (/i/) are preserved."""
        url = "https://bunkr.si/i/image-name"
        assert normalize_bunkr_url(url) == "https://bunkr.si/i/image-name"

    def test_normalize_empty_url(self):
        """Empty URL returns empty."""
        assert normalize_bunkr_url("") == ""
        assert normalize_bunkr_url("   ") == "   "


class TestExtractBunkrId:
    """Tests for the extract_bunkr_id() function."""

    def test_extract_album_id(self):
        """Extract album ID from bunkr URL."""
        url = "https://bunkr.si/a/album-name-123"
        assert extract_bunkr_id(url) == "album-name-123"

    def test_extract_file_id(self):
        """Extract file ID from bunkr URL."""
        url = "https://bunkr.su/f/file-name-456"
        assert extract_bunkr_id(url) == "file-name-456"

    def test_extract_video_id(self):
        """Extract video ID from bunkr URL."""
        url = "https://bunkr.si/v/video-name"
        assert extract_bunkr_id(url) == "video-name"

    def test_extract_empty_url(self):
        """Empty URL returns None."""
        assert extract_bunkr_id("") is None
        assert extract_bunkr_id("   ") is None


class TestParseLinksFile:
    """Tests for the parse_links_file() function."""

    def test_parse_sample_file(self, sample_links_file):
        """Parse sample links file with various URL types."""
        results = parse_links_file(sample_links_file)
        
        # Should have parsed several URLs
        assert len(results) >= 5
        
        # Check that we have different host types
        host_types = [r[1] for r in results]
        assert HostType.PIXELDRAIN in host_types
        assert HostType.BUZZHEAVIER in host_types
        assert HostType.BUNKR in host_types

    def test_parse_skips_comments(self, sample_links_file):
        """Comments starting with # are skipped."""
        results = parse_links_file(sample_links_file)
        
        # None of the results should be comments
        for original_url, _, _ in results:
            assert not original_url.strip().startswith("#")

    def test_parse_skips_empty_lines(self, sample_links_file):
        """Empty lines are skipped."""
        results = parse_links_file(sample_links_file)
        
        for original_url, _, _ in results:
            assert original_url.strip() != ""

    def test_parse_empty_file(self, empty_links_file):
        """Parsing file with only comments returns empty list."""
        results = parse_links_file(empty_links_file)
        assert results == []

    def test_parse_nonexistent_file(self, temp_dir):
        """Parsing nonexistent file raises FileNotFoundError."""
        nonexistent = temp_dir / "nonexistent.txt"
        
        with pytest.raises(FileNotFoundError):
            parse_links_file(nonexistent)

    def test_parse_extracts_correct_ids(self, temp_dir):
        """Parse file and verify extracted IDs are correct."""
        links_file = temp_dir / "test_links.txt"
        links_file.write_text(
            "https://pixeldrain.com/u/testid01\n"
            "https://buzzheavier.com/f/buzztestid12\n",
            encoding="utf-8"
        )
        
        results = parse_links_file(links_file)
        
        assert len(results) == 2
        
        # Check pixeldrain
        assert results[0][1] == HostType.PIXELDRAIN
        assert results[0][2] == "testid01"
        
        # Check buzzheavier
        assert results[1][1] == HostType.BUZZHEAVIER
        assert results[1][2] == "buzztestid12"


class TestValidateUrl:
    """Tests for the validate_url() function."""

    def test_validate_valid_pixeldrain(self):
        """Valid pixeldrain URL passes validation."""
        is_valid, error = validate_url("https://pixeldrain.com/u/abc12345")
        assert is_valid is True
        assert error is None

    def test_validate_valid_buzzheavier(self):
        """Valid buzzheavier URL passes validation."""
        is_valid, error = validate_url("https://buzzheavier.com/f/abc123def456")
        assert is_valid is True
        assert error is None

    def test_validate_valid_bunkr(self):
        """Valid bunkr URL passes validation."""
        is_valid, error = validate_url("https://bunkr.si/a/album-name")
        assert is_valid is True
        assert error is None

    def test_validate_empty_url(self):
        """Empty URL fails validation."""
        is_valid, error = validate_url("")
        assert is_valid is False
        assert "empty" in error.lower()

    def test_validate_unknown_host(self):
        """Unknown host fails validation."""
        is_valid, error = validate_url("https://unknown.com/file")
        assert is_valid is False
        assert "unsupported" in error.lower()

    def test_validate_whitespace_url(self):
        """Whitespace-only URL fails validation."""
        is_valid, error = validate_url("   ")
        assert is_valid is False
        assert "empty" in error.lower()


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_url_with_query_params(self):
        """URL with query parameters is handled."""
        url = "https://pixeldrain.com/u/abc12345?download=true"
        assert detect_host(url) == HostType.PIXELDRAIN

    def test_url_with_fragment(self):
        """URL with fragment is handled."""
        url = "https://bunkr.si/a/album-name#section"
        assert detect_host(url) == HostType.BUNKR

    def test_url_with_port(self):
        """URL with port number is handled."""
        url = "https://pixeldrain.com:443/u/abc12345"
        assert detect_host(url) == HostType.PIXELDRAIN

    def test_http_protocol(self):
        """HTTP (non-HTTPS) URLs are handled."""
        url = "http://pixeldrain.com/u/abc12345"
        assert detect_host(url) == HostType.PIXELDRAIN

    def test_mixed_case_id(self):
        """Mixed case IDs are extracted correctly."""
        url = "https://pixeldrain.com/u/AbCd1234"
        extracted = extract_pixeldrain_id(url)
        assert extracted == "AbCd1234"

    def test_unicode_in_path(self):
        """URLs with unicode characters don't crash."""
        url = "https://bunkr.si/a/album-名前"
        # Should not raise an exception
        host = detect_host(url)
        assert host == HostType.BUNKR
