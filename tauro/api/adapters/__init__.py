"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root

API adapters for orchestration services.

These adapters provide API-specific interfaces to the centralized
orchestration services, ensuring complete independence from CLI.
"""

from .orchestration_adapter import APIOrchestrationAdapter

__all__ = ["APIOrchestrationAdapter"]
