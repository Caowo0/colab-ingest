"""
colab_ingest.core - Core functionality and business logic.

This module contains the main orchestration logic for:
- Download queue management
- Extraction handling
- Upload coordination
"""

from colab_ingest.core.pipeline import Pipeline, PipelineConfig, PipelineStats
from colab_ingest.core.state import StateDB, Task, TaskStatus

__all__ = [
    "Pipeline",
    "PipelineConfig",
    "PipelineStats",
    "StateDB",
    "Task",
    "TaskStatus",
]
