"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Dict, List
from loguru import logger


class PipelineStatusFormatter:
    """Formatter for pipeline status output."""

    @staticmethod
    def format_single_pipeline(status_info: Dict[str, Any]) -> None:
        """Format and print status for a single pipeline."""
        logger.info(f"Pipeline Status: {status_info}")

    @staticmethod
    def format_multiple_pipelines(status_list: List[Dict[str, Any]]) -> None:
        """Format and print status for multiple pipelines."""
        for status in status_list:
            logger.info(f"Pipeline Status: {status}")
