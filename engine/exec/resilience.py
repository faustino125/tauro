"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import time
from typing import Any, Callable, TypeVar

from loguru import logger  # type: ignore

T = TypeVar("T")


class RetryPolicy:
    """
    Generic retry policy with exponential backoff.
    """

    def __init__(self, max_retries: int = 3, delay: int = 5, backoff_factor: float = 1.0):
        self.max_retries = max_retries
        self.delay = delay
        self.backoff_factor = backoff_factor

    def execute(self, func: Callable[..., T], *args, **kwargs) -> T:
        """
        Execute a function with retries.
        """
        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < self.max_retries:
                    sleep_time = self.delay * (self.backoff_factor**attempt)
                    logger.warning(
                        f"Action failed: {str(e)}. "
                        f"Retrying (attempt {attempt + 1}/{self.max_retries}) in {sleep_time:.2f}s..."
                    )
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Action failed after {self.max_retries + 1} attempts.")

        if last_exception:
            raise last_exception
        raise RuntimeError("Retry loop finished without result or exception")
