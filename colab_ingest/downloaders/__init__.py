"""
colab_ingest.downloaders - Host-specific download implementations.

This module contains downloaders for various file hosting services:
- Pixeldrain
- Buzzheavier
- Bunkr

Each downloader implements a common interface for consistent usage.
"""

from colab_ingest.downloaders.pixeldrain import (
    PixeldrainDownloader,
    DownloadResult,
    PixeldrainError,
    PixeldrainAuthError,
    PixeldrainNotFoundError,
    PixeldrainRateLimitError,
)
from colab_ingest.downloaders.bunkr_adapter import (
    BunkrDownloaderAdapter,
    BunkrDownloadResult,
    BunkrDownloaderError,
    BunkrScriptNotFoundError,
    BunkrDownloadTimeoutError,
)
from colab_ingest.downloaders.buzzheavier_adapter import (
    BuzzHeavierDownloaderAdapter,
    BuzzHeavierDownloadResult,
    BuzzHeavierDownloaderError,
    BuzzHeavierScriptNotFoundError,
    BuzzHeavierDownloadTimeoutError,
)

__all__ = [
    # Pixeldrain
    "PixeldrainDownloader",
    "DownloadResult",
    "PixeldrainError",
    "PixeldrainAuthError",
    "PixeldrainNotFoundError",
    "PixeldrainRateLimitError",
    # Bunkr
    "BunkrDownloaderAdapter",
    "BunkrDownloadResult",
    "BunkrDownloaderError",
    "BunkrScriptNotFoundError",
    "BunkrDownloadTimeoutError",
    # BuzzHeavier
    "BuzzHeavierDownloaderAdapter",
    "BuzzHeavierDownloadResult",
    "BuzzHeavierDownloaderError",
    "BuzzHeavierScriptNotFoundError",
    "BuzzHeavierDownloadTimeoutError",
]
