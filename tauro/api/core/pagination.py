"""
Pagination utilities for API endpoints.

Provides common pagination logic to avoid code duplication.
"""

from typing import TypeVar, Generic, List, Dict, Any, Optional

T = TypeVar("T")


class PaginationParams:
    """Parameters for pagination"""

    def __init__(self, limit: int = 50, offset: int = 0):
        """
        Initialize pagination parameters.

        Args:
            limit: Maximum items per page (1-500)
            offset: Number of items to skip
        """
        self.limit = max(1, min(limit, 500))  # Enforce bounds
        self.offset = max(0, offset)

    @property
    def page(self) -> int:
        """Calculate page number (1-indexed)"""
        return (self.offset // self.limit) + 1

    @property
    def skip(self) -> int:
        """Items to skip in first page"""
        return self.offset % self.limit

    def apply(self, items: List[T]) -> List[T]:
        """
        Apply pagination to a list of items.

        Args:
            items: List to paginate

        Returns:
            Paginated slice of items
        """
        start = self.offset
        end = self.offset + self.limit
        return items[start:end]


class PaginatedResponse(Generic[T]):
    """Generic paginated response"""

    def __init__(
        self,
        items: List[T],
        total: int,
        limit: int,
        offset: int,
    ):
        self.items = items
        self.total = total
        self.limit = limit
        self.offset = offset

    @property
    def page(self) -> int:
        """Current page number"""
        return (self.offset // self.limit) + 1

    @property
    def pages(self) -> int:
        """Total number of pages"""
        return (self.total + self.limit - 1) // self.limit

    @property
    def has_next(self) -> bool:
        """Check if there are more pages"""
        return (self.offset + self.limit) < self.total

    @property
    def has_prev(self) -> bool:
        """Check if there are previous pages"""
        return self.offset > 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "items": self.items,
            "pagination": {
                "total": self.total,
                "limit": self.limit,
                "offset": self.offset,
                "page": self.page,
                "pages": self.pages,
                "has_next": self.has_next,
                "has_prev": self.has_prev,
            },
        }


def paginate_list(
    items: List[T],
    limit: int = 50,
    offset: int = 0,
) -> PaginatedResponse[T]:
    """
    Paginate a list of items.

    Args:
        items: Full list to paginate
        limit: Items per page
        offset: Starting offset

    Returns:
        PaginatedResponse with paginated items and metadata
    """
    params = PaginationParams(limit, offset)
    total = len(items)
    paginated_items = params.apply(items)

    return PaginatedResponse(
        items=paginated_items,
        total=total,
        limit=params.limit,
        offset=params.offset,
    )


__all__ = [
    "PaginationParams",
    "PaginatedResponse",
    "paginate_list",
]
