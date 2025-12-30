"""
Feature schema definitions and validation.

Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum

from loguru import logger  # type: ignore


class DataType(str, Enum):
    """Supported feature data types."""

    INT = "int"
    BIGINT = "bigint"
    FLOAT = "float"
    DOUBLE = "double"
    STRING = "string"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"
    DECIMAL = "decimal"
    ARRAY = "array"
    MAP = "map"
    STRUCT = "struct"


class FeatureType(str, Enum):
    """Types of features in the store."""

    NUMERICAL = "numerical"
    CATEGORICAL = "categorical"
    TEXT = "text"
    VECTOR = "vector"
    TIMESERIES = "timeseries"
    COMPLEX = "complex"


@dataclass
class FeatureSchema:
    """Schema definition for a feature."""

    name: str
    """Feature name, must be unique within feature group"""

    data_type: DataType
    """Data type of the feature"""

    feature_type: FeatureType
    """Semantic type of the feature"""

    description: str = ""
    """Human-readable feature description"""

    nullable: bool = True
    """Whether the feature can be null"""

    metadata: Dict[str, Any] = field(default_factory=dict)
    """Additional metadata (units, ranges, distributions)"""

    def to_dict(self) -> Dict[str, Any]:
        """Convert schema to dictionary representation."""
        return {
            "name": self.name,
            "data_type": self.data_type.value,
            "feature_type": self.feature_type.value,
            "description": self.description,
            "nullable": self.nullable,
            "metadata": self.metadata,
        }


@dataclass
class FeatureGroupSchema:
    """Schema definition for a feature group."""

    name: str
    """Feature group name"""

    version: int = 1
    """Schema version for evolution tracking"""

    entity_keys: List[str] = field(default_factory=list)
    """Primary key columns for the entity"""

    features: List[FeatureSchema] = field(default_factory=list)
    """List of feature schemas in this group"""

    timestamp_key: Optional[str] = None
    """Optional timestamp column for point-in-time queries"""

    description: str = ""
    """Feature group description"""

    tags: Dict[str, str] = field(default_factory=dict)
    """Tags for organization and discovery"""

    owner: str = "unknown"
    """Owner of the feature group"""

    created_at: datetime = field(default_factory=datetime.utcnow)
    """Creation timestamp"""

    def add_feature(self, feature: FeatureSchema) -> None:
        """Add a feature to the group."""
        if any(f.name == feature.name for f in self.features):
            raise ValueError(f"Feature '{feature.name}' already exists in group '{self.name}'")
        self.features.append(feature)
        logger.debug(f"Added feature '{feature.name}' to group '{self.name}'")

    def get_feature(self, name: str) -> Optional[FeatureSchema]:
        """Retrieve a feature by name."""
        return next((f for f in self.features if f.name == name), None)

    def to_dict(self) -> Dict[str, Any]:
        """Convert schema to dictionary representation."""
        return {
            "name": self.name,
            "version": self.version,
            "entity_keys": self.entity_keys,
            "features": [f.to_dict() for f in self.features],
            "timestamp_key": self.timestamp_key,
            "description": self.description,
            "tags": self.tags,
            "owner": self.owner,
            "created_at": self.created_at.isoformat(),
        }

    def validate(self) -> bool:
        """Validate schema integrity."""
        if not self.name:
            raise ValueError("Feature group name is required")
        if not self.entity_keys:
            logger.warning(f"Feature group '{self.name}' has no entity keys")
        if not self.features:
            raise ValueError(f"Feature group '{self.name}' has no features")
        return True
