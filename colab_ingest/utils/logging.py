"""Logging configuration with rich console output and file logging.

This module provides logging setup for the colab_ingest CLI tool with:
- Rich console output with timestamps
- File logging to timestamped log files
- Task prefix support for tracking operations
- Sensitive data masking for API keys
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from rich.logging import RichHandler


def setup_logging(workdir: Path, verbose: bool = True) -> logging.Logger:
    """Set up logging with rich console output and file handler.

    Creates a logger with:
    - Console output using RichHandler with timestamps
    - File output to workdir/logs/run_YYYYMMDD_HHMMSS.log
    - DEBUG level for file, INFO for console (DEBUG if verbose)

    Args:
        workdir: Working directory path where logs directory will be created.
        verbose: If True, console shows DEBUG level; otherwise INFO.

    Returns:
        Configured logger instance for the application.
    """
    # Ensure logs directory exists
    logs_dir = workdir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Create timestamped log filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"run_{timestamp}.log"

    # Get or create the main logger
    logger = logging.getLogger("colab_ingest")
    logger.setLevel(logging.DEBUG)

    # Clear any existing handlers to avoid duplicates
    logger.handlers.clear()

    # Console handler with Rich
    console_level = logging.DEBUG if verbose else logging.INFO
    console_handler = RichHandler(
        show_time=True,
        show_path=False,
        rich_tracebacks=True,
        tracebacks_show_locals=verbose,
        markup=True,
    )
    console_handler.setLevel(console_level)
    console_handler.setFormatter(logging.Formatter("%(message)s"))

    # File handler with detailed format
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.debug(f"Logging initialized. Log file: {log_file}")

    return logger


def get_task_logger(task_id: str) -> logging.Logger:
    """Get a logger with task prefix for tracking specific operations.

    Args:
        task_id: The unique task identifier to prefix log messages with.

    Returns:
        Logger instance that prefixes messages with [TASK <id>].
    """
    return logging.getLogger(f"colab_ingest.task.{task_id}")


class TaskLogAdapter(logging.LoggerAdapter):
    """Logger adapter that prefixes messages with task ID.

    Usage:
        logger = TaskLogAdapter(base_logger, task_id="abc123")
        logger.info("Starting download")  # Logs: [TASK abc123] Starting download
    """

    def __init__(self, logger: logging.Logger, task_id: str) -> None:
        """Initialize the task log adapter.

        Args:
            logger: Base logger to wrap.
            task_id: Task identifier for prefixing messages.
        """
        super().__init__(logger, {"task_id": task_id})
        self.task_id = task_id

    def process(
        self, msg: str, kwargs: dict
    ) -> tuple[str, dict]:
        """Process the log message to add task prefix.

        Args:
            msg: Original log message.
            kwargs: Keyword arguments for the log call.

        Returns:
            Tuple of (prefixed_message, kwargs).
        """
        return f"[TASK {self.task_id}] {msg}", kwargs


def mask_sensitive_data(value: str, visible_chars: int = 4) -> str:
    """Mask sensitive data, showing only the last few characters.

    Args:
        value: The sensitive string to mask (e.g., API key).
        visible_chars: Number of characters to show at the end.

    Returns:
        Masked string with asterisks and visible suffix.

    Examples:
        >>> mask_sensitive_data("sk-1234567890abcdef")
        '**************cdef'
        >>> mask_sensitive_data("short", visible_chars=4)
        '*hort'
        >>> mask_sensitive_data("")
        ''
    """
    if not value:
        return ""

    if len(value) <= visible_chars:
        # For very short strings, mask all but last char
        if len(value) <= 1:
            return "*" * len(value)
        return "*" * (len(value) - 1) + value[-1]

    masked_length = len(value) - visible_chars
    return "*" * masked_length + value[-visible_chars:]


def mask_url_sensitive_parts(url: str) -> str:
    """Mask sensitive parts of a URL (API keys, tokens in query params).

    Args:
        url: URL that may contain sensitive query parameters.

    Returns:
        URL with sensitive query parameter values masked.
    """
    # Common sensitive parameter names
    sensitive_params = re.compile(
        r"((?:api[_-]?key|token|secret|password|auth|key|access[_-]?token)"
        r"=)([^&\s]+)",
        re.IGNORECASE,
    )

    def mask_match(match: re.Match) -> str:
        param_name = match.group(1)
        param_value = match.group(2)
        return param_name + mask_sensitive_data(param_value)

    return sensitive_params.sub(mask_match, url)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get a logger instance for the colab_ingest package.

    Args:
        name: Optional sub-logger name. If None, returns the main logger.

    Returns:
        Logger instance.
    """
    if name:
        return logging.getLogger(f"colab_ingest.{name}")
    return logging.getLogger("colab_ingest")
