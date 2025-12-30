"""
Feature Store exceptions module.

Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""


class FeatureStoreException(Exception):
    """Base exception for Feature Store operations."""

    pass


class FeatureNotFoundError(FeatureStoreException):
    """Raised when a requested feature is not found in the store."""

    pass


class FeatureGroupNotFoundError(FeatureStoreException):
    """Raised when a requested feature group is not found."""

    pass


class SchemaValidationError(FeatureStoreException):
    """Raised when feature schema validation fails."""

    pass


class FeatureMaterializationError(FeatureStoreException):
    """Raised when materialization of features fails."""

    pass


class VirtualizationQueryError(FeatureStoreException):
    """Raised when on-demand query execution fails."""

    pass


class MetadataError(FeatureStoreException):
    """Raised when metadata operations fail."""

    pass


class FeatureRegistryError(FeatureStoreException):
    """Raised when feature registry operations fail."""

    pass
