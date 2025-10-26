"""
Retry logic utilities for handling transient failures.

Provides decorators and utilities for exponential backoff retry.
"""

import asyncio
import time
from functools import wraps
from typing import Callable, Any, Type, Tuple
from loguru import logger


class RetryConfig:
    """Configuration for retry behavior"""

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
    ):
        """
        Initialize retry configuration.

        Args:
            max_retries: Maximum number of retries
            base_delay: Initial delay in seconds
            max_delay: Maximum delay in seconds
            exponential_base: Base for exponential backoff
            jitter: Add random jitter to delays
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter

    def get_delay(self, attempt: int) -> float:
        """
        Calculate delay for a given attempt number.

        Args:
            attempt: Attempt number (0-indexed)

        Returns:
            Delay in seconds
        """
        delay = self.base_delay * (self.exponential_base**attempt)
        delay = min(delay, self.max_delay)

        if self.jitter:
            import random

            delay = delay * (0.5 + random.random())

        return delay


# Default retry configuration
DEFAULT_RETRY_CONFIG = RetryConfig(
    max_retries=3,
    base_delay=1.0,
    max_delay=60.0,
)


def retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retry_on: Tuple[Type[Exception], ...] = (Exception,),
    name: str = None,
):
    """
    Decorator for retrying functions with exponential backoff.

    Usage:
        @retry(max_retries=3, retry_on=(ConnectionError, TimeoutError))
        async def call_api():
            ...
    """

    def decorator(func: Callable) -> Callable:
        config = RetryConfig(
            max_retries=max_retries,
            base_delay=base_delay,
            max_delay=max_delay,
        )
        func_name = name or func.__name__

        async def async_wrapper(*args, **kwargs):
            return await _retry_async_impl(
                func, config, func_name, retry_on, *args, **kwargs
            )

        def sync_wrapper(*args, **kwargs):
            return _retry_sync_impl(func, config, func_name, retry_on, *args, **kwargs)

        # Return async wrapper if original is async
        if asyncio.iscoroutinefunction(func):
            return wraps(func)(async_wrapper)
        else:
            return wraps(func)(sync_wrapper)

    return decorator


async def _retry_async_impl(
    func: Callable,
    config: RetryConfig,
    func_name: str,
    retry_on: Tuple[Type[Exception], ...],
    *args,
    **kwargs,
) -> Any:
    """Helper for async retry implementation"""
    last_exception = None

    for attempt in range(config.max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except retry_on as e:
            last_exception = e
            if attempt < config.max_retries:
                delay = config.get_delay(attempt)
                logger.warning(
                    f"[{func_name}] Attempt {attempt + 1}/{config.max_retries + 1} failed: {e}. "
                    f"Retrying in {delay:.2f}s..."
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    f"[{func_name}] All {config.max_retries + 1} attempts failed"
                )
        except Exception as e:
            logger.error(f"[{func_name}] Non-retryable error: {e}")
            raise

    raise last_exception


def _retry_sync_impl(
    func: Callable,
    config: RetryConfig,
    func_name: str,
    retry_on: Tuple[Type[Exception], ...],
    *args,
    **kwargs,
) -> Any:
    """Helper for sync retry implementation"""
    last_exception = None

    for attempt in range(config.max_retries + 1):
        try:
            return func(*args, **kwargs)
        except retry_on as e:
            last_exception = e
            if attempt < config.max_retries:
                delay = config.get_delay(attempt)
                logger.warning(
                    f"[{func_name}] Attempt {attempt + 1}/{config.max_retries + 1} failed: {e}. "
                    f"Retrying in {delay:.2f}s..."
                )
                time.sleep(delay)
            else:
                logger.error(
                    f"[{func_name}] All {config.max_retries + 1} attempts failed"
                )
        except Exception as e:
            logger.error(f"[{func_name}] Non-retryable error: {e}")
            raise

    raise last_exception


async def retry_async(
    func: Callable,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retry_on: Tuple[Type[Exception], ...] = (Exception,),
) -> Any:
    """
    Retry an async function with exponential backoff.

    Args:
        func: Async function to retry
        max_retries: Maximum retries
        base_delay: Initial delay
        max_delay: Max delay
        retry_on: Exception types to retry on

    Returns:
        Result of successful function call
    """
    config = RetryConfig(
        max_retries=max_retries,
        base_delay=base_delay,
        max_delay=max_delay,
    )
    last_exception = None

    for attempt in range(config.max_retries + 1):
        try:
            return await func()
        except retry_on as e:
            last_exception = e
            if attempt < config.max_retries:
                delay = config.get_delay(attempt)
                logger.warning(
                    f"Attempt {attempt + 1}/{config.max_retries + 1} failed: {e}. "
                    f"Retrying in {delay:.2f}s..."
                )
                await asyncio.sleep(delay)
        except Exception as e:
            logger.error(f"Non-retryable error: {e}")
            raise

    raise last_exception


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "RetryConfig",
    "DEFAULT_RETRY_CONFIG",
    "retry",
    "retry_async",
]
