from __future__ import annotations
from enum import Enum
from typing import Callable, Optional, Any, Dict, TypeVar
from datetime import datetime, timezone
from threading import RLock
from dataclasses import dataclass, field
import time
import threading
from functools import wraps

from loguru import logger


T = TypeVar("T")


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "CLOSED"  # Operating normally
    OPEN = "OPEN"  # Circuit open, rejecting requests
    HALF_OPEN = "HALF_OPEN"  # Testing whether the service recovered


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""

    failure_threshold: int = 5  # Consecutive failures before opening
    success_threshold: int = 2  # Successes in half-open before closing
    timeout_seconds: float = 60.0  # Time in OPEN before trying HALF_OPEN
    half_open_max_calls: int = 3  # Calls allowed in HALF_OPEN


@dataclass
class CircuitBreakerMetrics:
    """Circuit breaker metrics."""

    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None
    last_state_change: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    total_calls: int = 0
    failed_calls: int = 0
    successful_calls: int = 0
    rejected_calls: int = 0


class CircuitBreakerOpenError(Exception):
    """Exception raised when the circuit is open."""

    pass


class CircuitBreaker:
    """
    Circuit Breaker pattern implementation.

    Prevents failure cascades by "opening the circuit" when too many consecutive
    errors are detected.
    """

    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._metrics = CircuitBreakerMetrics()
        self._lock = RLock()
        self._half_open_calls = 0

    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute a function with circuit breaker protection."""
        with self._lock:
            self._metrics.total_calls += 1

            # Check if the circuit is open
            if self._metrics.state == CircuitState.OPEN:
                # Check if it's time to attempt HALF_OPEN
                if self._should_attempt_reset():
                    self._transition_to_half_open()
                else:
                    self._metrics.rejected_calls += 1
                    logger.warning(
                        f"Circuit breaker '{self.name}' is OPEN, rejecting call",
                        extra={
                            "circuit": self.name,
                            "state": self._metrics.state.value,
                            "failure_count": self._metrics.failure_count,
                        },
                    )
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker '{self.name}' is OPEN"
                    )

            # In HALF_OPEN, limit calls
            if self._metrics.state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self.config.half_open_max_calls:
                    self._metrics.rejected_calls += 1
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker '{self.name}' is HALF_OPEN and at max capacity"
                    )
                self._half_open_calls += 1

        # Ejecutar función
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure(e)
            raise
        finally:
            if self._metrics.state == CircuitState.HALF_OPEN:
                with self._lock:
                    self._half_open_calls -= 1

    def _should_attempt_reset(self) -> bool:
        """Check if it's time to attempt a service recovery check."""
        if self._metrics.last_failure_time is None:
            return False

        elapsed = datetime.now(timezone.utc) - self._metrics.last_failure_time
        return elapsed.total_seconds() >= self.config.timeout_seconds

    def _transition_to_half_open(self):
        """Transition to HALF_OPEN state."""
        logger.info(
            f"Circuit breaker '{self.name}' transitioning to HALF_OPEN",
            extra={"circuit": self.name},
        )
        self._metrics.state = CircuitState.HALF_OPEN
        self._metrics.failure_count = 0
        self._metrics.success_count = 0
        self._metrics.last_state_change = datetime.now(timezone.utc)
        self._half_open_calls = 0

    def _on_success(self):
        """Handle successful execution."""
        with self._lock:
            self._metrics.successful_calls += 1

            if self._metrics.state == CircuitState.HALF_OPEN:
                self._metrics.success_count += 1

                # If we reach the success threshold, close the circuit
                if self._metrics.success_count >= self.config.success_threshold:
                    logger.info(
                        f"Circuit breaker '{self.name}' closing after successful recovery",
                        extra={
                            "circuit": self.name,
                            "success_count": self._metrics.success_count,
                        },
                    )
                    self._metrics.state = CircuitState.CLOSED
                    self._metrics.failure_count = 0
                    self._metrics.success_count = 0
                    self._metrics.last_state_change = datetime.now(timezone.utc)

            elif self._metrics.state == CircuitState.CLOSED:
                # Reset failure count in CLOSED
                self._metrics.failure_count = 0

    def _on_failure(self, error: Exception):
        """Handle failed execution."""
        with self._lock:
            self._metrics.failed_calls += 1
            self._metrics.failure_count += 1
            self._metrics.last_failure_time = datetime.now(timezone.utc)

            if self._metrics.state == CircuitState.HALF_OPEN:
                # In HALF_OPEN, any failure opens the circuit
                logger.warning(
                    f"Circuit breaker '{self.name}' opening after failure in HALF_OPEN",
                    extra={"circuit": self.name, "error": str(error)},
                )
                self._metrics.state = CircuitState.OPEN
                self._metrics.last_state_change = datetime.now(timezone.utc)
                self._metrics.success_count = 0

            elif self._metrics.state == CircuitState.CLOSED:
                # In CLOSED, open if we reach the threshold
                if self._metrics.failure_count >= self.config.failure_threshold:
                    logger.error(
                        f"Circuit breaker '{self.name}' opening after {self._metrics.failure_count} failures",
                        extra={
                            "circuit": self.name,
                            "failure_count": self._metrics.failure_count,
                            "threshold": self.config.failure_threshold,
                        },
                    )
                    self._metrics.state = CircuitState.OPEN
                    self._metrics.last_state_change = datetime.now(timezone.utc)

    def get_metrics(self) -> CircuitBreakerMetrics:
        """Obtener métricas actuales"""
        with self._lock:
            return CircuitBreakerMetrics(
                state=self._metrics.state,
                failure_count=self._metrics.failure_count,
                success_count=self._metrics.success_count,
                last_failure_time=self._metrics.last_failure_time,
                last_state_change=self._metrics.last_state_change,
                total_calls=self._metrics.total_calls,
                failed_calls=self._metrics.failed_calls,
                successful_calls=self._metrics.successful_calls,
                rejected_calls=self._metrics.rejected_calls,
            )

    def reset(self):
        """Resetear manualmente el circuit breaker"""
        with self._lock:
            logger.info(f"Manually resetting circuit breaker '{self.name}'")
            self._metrics.state = CircuitState.CLOSED
            self._metrics.failure_count = 0
            self._metrics.success_count = 0
            self._metrics.last_state_change = datetime.now(timezone.utc)


class Bulkhead:
    """
    Implementación de Bulkhead pattern.

    Aísla recursos para prevenir que un componente fallido afecte
    a otros componentes del sistema.
    """

    def __init__(
        self, name: str, max_concurrent_calls: int, max_wait_duration: float = 30.0
    ):
        self.name = name
        self.max_concurrent_calls = max_concurrent_calls
        self.max_wait_duration = max_wait_duration
        self._semaphore = threading.Semaphore(max_concurrent_calls)
        self._lock = RLock()
        self._metrics = {
            "total_calls": 0,
            "concurrent_calls": 0,
            "rejected_calls": 0,
            "successful_calls": 0,
            "failed_calls": 0,
        }

    def execute(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Ejecutar función con protección de bulkhead"""
        with self._lock:
            self._metrics["total_calls"] += 1

        # Intentar adquirir semáforo con timeout
        acquired = self._semaphore.acquire(timeout=self.max_wait_duration)

        if not acquired:
            with self._lock:
                self._metrics["rejected_calls"] += 1
            logger.warning(
                f"Bulkhead '{self.name}' rejected call - max capacity reached",
                extra={
                    "bulkhead": self.name,
                    "max_concurrent": self.max_concurrent_calls,
                },
            )
            raise RuntimeError(f"Bulkhead '{self.name}' is at max capacity")

        try:
            with self._lock:
                self._metrics["concurrent_calls"] += 1

            result = func(*args, **kwargs)

            with self._lock:
                self._metrics["successful_calls"] += 1

            return result

        except Exception:
            with self._lock:
                self._metrics["failed_calls"] += 1
            raise

        finally:
            with self._lock:
                self._metrics["concurrent_calls"] -= 1
            self._semaphore.release()

    def get_metrics(self) -> Dict[str, Any]:
        """Obtener métricas del bulkhead"""
        with self._lock:
            return {
                "name": self.name,
                "max_concurrent_calls": self.max_concurrent_calls,
                **self._metrics.copy(),
            }


class RetryPolicy:
    """
    Política de reintentos con backoff exponencial.
    """

    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
    ):
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter

    def execute(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Ejecutar función con reintentos"""
        last_exception = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e

                if attempt == self.max_attempts:
                    logger.error(
                        "All retry attempts exhausted",
                        extra={
                            "attempt": attempt,
                            "max_attempts": self.max_attempts,
                            "error": str(e),
                        },
                    )
                    raise

                # Calcular delay con backoff exponencial
                delay = min(
                    self.initial_delay * (self.exponential_base ** (attempt - 1)),
                    self.max_delay,
                )

                # Añadir jitter si está habilitado
                if self.jitter:
                    import random

                    delay = delay * (0.5 + random.random())

                logger.warning(
                    f"Retry attempt {attempt}/{self.max_attempts} after {delay:.2f}s",
                    extra={
                        "attempt": attempt,
                        "max_attempts": self.max_attempts,
                        "delay": delay,
                        "error": str(e),
                    },
                )

                time.sleep(delay)

        # Esto no debería alcanzarse, pero por seguridad
        if last_exception:
            raise last_exception
        raise RuntimeError("Unexpected retry loop exit")


def with_circuit_breaker(name: str, config: Optional[CircuitBreakerConfig] = None):
    """Decorador para aplicar circuit breaker a una función"""
    breaker = CircuitBreaker(name, config)

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return breaker.call(func, *args, **kwargs)

        # Adjuntar breaker para acceso a métricas
        wrapper.circuit_breaker = breaker
        return wrapper

    return decorator


def with_bulkhead(
    name: str, max_concurrent_calls: int, max_wait_duration: float = 30.0
):
    """Decorador para aplicar bulkhead a una función"""
    bulkhead = Bulkhead(name, max_concurrent_calls, max_wait_duration)

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return bulkhead.execute(func, *args, **kwargs)

        # Adjuntar bulkhead para acceso a métricas
        wrapper.bulkhead = bulkhead
        return wrapper

    return decorator


def with_retry(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
):
    """Decorador para aplicar política de reintentos a una función"""
    policy = RetryPolicy(
        max_attempts, initial_delay, max_delay, exponential_base, jitter
    )

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return policy.execute(func, *args, **kwargs)

        return wrapper

    return decorator


class ResilienceManager:
    """
    Gestor centralizado de patrones de resiliencia.
    Permite registrar y obtener circuit breakers y bulkheads por nombre.
    """

    def __init__(self):
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._bulkheads: Dict[str, Bulkhead] = {}
        self._lock = RLock()

    def get_or_create_circuit_breaker(
        self, name: str, config: Optional[CircuitBreakerConfig] = None
    ) -> CircuitBreaker:
        """Obtener o crear un circuit breaker"""
        with self._lock:
            if name not in self._circuit_breakers:
                self._circuit_breakers[name] = CircuitBreaker(name, config)
            return self._circuit_breakers[name]

    def get_or_create_bulkhead(
        self, name: str, max_concurrent_calls: int, max_wait_duration: float = 30.0
    ) -> Bulkhead:
        """Obtener o crear un bulkhead"""
        with self._lock:
            if name not in self._bulkheads:
                self._bulkheads[name] = Bulkhead(
                    name, max_concurrent_calls, max_wait_duration
                )
            return self._bulkheads[name]

    def get_all_metrics(self) -> Dict[str, Any]:
        """Obtener métricas de todos los componentes"""
        with self._lock:
            return {
                "circuit_breakers": {
                    name: cb.get_metrics()
                    for name, cb in self._circuit_breakers.items()
                },
                "bulkheads": {
                    name: bh.get_metrics() for name, bh in self._bulkheads.items()
                },
            }

    def reset_all(self):
        """Resetear todos los circuit breakers"""
        with self._lock:
            for cb in self._circuit_breakers.values():
                cb.reset()


# Instancia global del gestor de resiliencia
_resilience_manager = ResilienceManager()


def get_resilience_manager() -> ResilienceManager:
    """Obtener instancia global del gestor de resiliencia"""
    return _resilience_manager
