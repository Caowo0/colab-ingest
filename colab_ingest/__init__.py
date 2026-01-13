"""
colab-ingest: CLI tool for downloading files from multiple hosts and uploading to Google Drive.

Supports downloading from:
- Pixeldrain
- Buzzheavier
- Bunkr

And automatically extracts archives and uploads to Google Drive.
"""

from colab_ingest.core.pipeline import Pipeline, PipelineConfig
from colab_ingest.core.state import StateDB, Task, TaskStatus

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

__all__ = [
    "__version__",
    "__author__",
    "__email__",
    "Pipeline",
    "PipelineConfig",
    "StateDB",
    "Task",
    "TaskStatus",
]
