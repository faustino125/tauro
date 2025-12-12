"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import sys
import os
import atexit
import time
from typing import Any, Dict, List, Literal, Optional
import threading

from loguru import logger  # type: ignore


class SparkSessionManager:
    """
    Manager for Spark sessions with lifecycle management, cleanup, and memory leak prevention.
    """

    _sessions: Dict[str, Any] = {}
    _session_metadata: Dict[str, Dict[str, Any]] = {}
    _lock = threading.RLock()
    _cleanup_registered = False
    _session_timeout = 3600  # 1 hour default

    @classmethod
    def _register_cleanup(cls) -> None:
        """Register cleanup handler to run at program exit."""
        if not cls._cleanup_registered:
            atexit.register(cls.cleanup_all)
            cls._cleanup_registered = True
            logger.debug("Registered SparkSessionManager cleanup handler")

    @classmethod
    def _generate_session_key(
        cls,
        mode: str,
        ml_config: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Generate a unique key for session caching based on configuration."""
        config_hash = hash(frozenset((ml_config or {}).items()))
        return f"{mode}_{config_hash}"

    @classmethod
    def get_or_create_session(
        cls,
        mode: Literal["local", "databricks", "distributed"] = "databricks",
        ml_config: Optional[Dict[str, Any]] = None,
        force_new: bool = False,
    ):
        """Get existing session or create new one with thread-safe caching."""
        cls._register_cleanup()

        session_key = cls._generate_session_key(mode, ml_config)

        with cls._lock:
            # Check if session exists and is valid
            if not force_new and session_key in cls._sessions:
                session = cls._sessions[session_key]
                if cls._is_session_valid(session, session_key):
                    logger.debug(f"Reusing existing Spark session: {session_key}")
                    cls._update_session_access_time(session_key)
                    return session
                else:
                    logger.info(f"Existing session invalid, creating new one: {session_key}")
                    cls._cleanup_session(session_key)

            # Create new session
            logger.info(f"Creating new Spark session: {session_key} (mode={mode})")
            session = SparkSessionFactory.create_session(mode, ml_config)

            cls._sessions[session_key] = session
            cls._session_metadata[session_key] = {
                "created_at": time.time(),
                "last_accessed": time.time(),
                "mode": mode,
                "ml_config": ml_config,
            }

            return session

    @classmethod
    def _is_session_valid(cls, session: Any, session_key: str) -> bool:
        """Check if a session is still valid and active."""
        try:
            # Check if session object is not None
            if session is None:
                return False

            # Check metadata exists
            if session_key not in cls._session_metadata:
                return False

            # Check timeout
            metadata = cls._session_metadata[session_key]
            elapsed = time.time() - metadata.get("last_accessed", 0)
            if elapsed > cls._session_timeout:
                logger.warning(
                    f"Session {session_key} timed out ({elapsed:.0f}s > {cls._session_timeout}s)"
                )
                return False

            # Try to access session to verify it's active
            _ = session.version
            return True

        except Exception as e:
            logger.debug(f"Session validation failed for {session_key}: {e}")
            return False

    @classmethod
    def _update_session_access_time(cls, session_key: str) -> None:
        """Update last access time for a session."""
        if session_key in cls._session_metadata:
            cls._session_metadata[session_key]["last_accessed"] = time.time()

    @classmethod
    def _cleanup_session(cls, session_key: str) -> None:
        """Cleanup a specific session."""
        if session_key in cls._sessions:
            session = cls._sessions.pop(session_key)
            cls._session_metadata.pop(session_key, None)
            try:
                session.stop()
                logger.debug(f"Stopped Spark session: {session_key}")
            except Exception as e:
                logger.warning(f"Error stopping session {session_key}: {e}")

    @classmethod
    def cleanup_all(cls) -> None:
        """Cleanup all managed sessions."""
        with cls._lock:
            logger.info(f"Cleaning up {len(cls._sessions)} Spark session(s)")
            for session_key in tuple(cls._sessions):
                cls._cleanup_session(session_key)
            cls._sessions.clear()
            cls._session_metadata.clear()

    @classmethod
    def cleanup_stale_sessions(cls, max_age: int = None) -> int:
        """Cleanup sessions that haven't been accessed recently."""
        max_age = max_age or cls._session_timeout
        cleaned = 0

        with cls._lock:
            current_time = time.time()
            for session_key in tuple(cls._sessions):
                metadata = cls._session_metadata.get(session_key, {})
                last_accessed = metadata.get("last_accessed", 0)
                age = current_time - last_accessed

                if age > max_age:
                    logger.info(f"Cleaning up stale session {session_key} (age: {age:.0f}s)")
                    cls._cleanup_session(session_key)
                    cleaned += 1

        return cleaned

    @classmethod
    def get_session_info(cls) -> Dict[str, Dict[str, Any]]:
        """Get information about all active sessions."""
        with cls._lock:
            info = {}
            current_time = time.time()
            for key, metadata in cls._session_metadata.items():
                age = current_time - metadata.get("created_at", current_time)
                last_access_age = current_time - metadata.get("last_accessed", current_time)
                info[key] = {
                    "mode": metadata.get("mode"),
                    "age_seconds": age,
                    "last_access_seconds_ago": last_access_age,
                    "is_valid": cls._is_session_valid(cls._sessions.get(key), key),
                }
            return info


class SparkSessionFactory:
    """
    Factory for creating Spark sessions based on the execution mode with ML optimizations.
    """

    _session = None
    _lock = threading.Lock()

    @classmethod
    def get_session(
        cls,
        mode: Literal["local", "databricks", "distributed"] = "databricks",
        ml_config: Optional[Dict[str, Any]] = None,
    ):
        """Singleton Spark session with thread-safe initialization."""
        with cls._lock:
            if cls._session is None:
                cls._session = SparkSessionFactory.create_session(mode, ml_config)
        return cls._session

    @classmethod
    def reset_session(cls):
        """Reset session for testing or reconfiguration."""
        with cls._lock:
            if cls._session:
                try:
                    cls._session.stop()
                except Exception:
                    logger.warning("Error stopping Spark session during reset", exc_info=True)
            cls._session = None

    PROTECTED_CONFIGS = [
        "spark.sql.shuffle.partitions",
        "spark.executor.memory",
        "spark.driver.memory",
        "spark.master",
        "spark.submit.deployMode",
        "spark.dynamicAllocation.enabled",
        "spark.executor.instances",
    ]

    @classmethod
    def set_protected_configs(cls, configs: List[str]) -> None:
        """Set custom protected configurations"""
        cls.PROTECTED_CONFIGS = configs

    @staticmethod
    def create_session(
        mode: Literal["local", "databricks", "distributed"] = "databricks",
        ml_config: Optional[Dict[str, Any]] = None,
    ):
        """Create a Spark session based on the specified mode with ML configurations."""
        logger.info(f"Attempting to create Spark session in {mode} mode")

        if ml_config:
            logger.info("Applying ML-specific Spark configurations")

        normalized = str(mode).lower()
        if normalized in ("databricks", "distributed"):
            return SparkSessionFactory._create_databricks_session(ml_config)
        elif normalized == "local":
            return SparkSessionFactory._create_local_session(ml_config)
        else:
            raise ValueError(
                f"Invalid execution mode: {mode}. Use 'local', 'databricks' or 'distributed'."
            )

    @staticmethod
    def _create_databricks_session(ml_config: Optional[Dict[str, Any]] = None):
        """
        Create a Databricks Connect session for remote execution with ML configs.
        """
        try:
            from databricks.connect import DatabricksSession  # type: ignore
            from databricks.sdk.core import Config  # type: ignore

            config = Config()

            SparkSessionFactory._validate_databricks_config(config)

            logger.info("Creating remote session with Databricks Connect")
            builder = DatabricksSession.builder.remote(
                host=config.host, token=config.token, cluster_id=config.cluster_id
            )

            if ml_config:
                builder = SparkSessionFactory._apply_ml_configs(builder, ml_config)

            return builder.getOrCreate()

        except ImportError as e:
            logger.error(f"Databricks Connect not installed: {str(e)}")
            raise
        except ValueError as e:
            logger.error(f"Invalid configuration: {str(e)}")
            raise
        except RuntimeError as e:
            logger.error(f"Connection failed: {str(e)}")
            raise
        except Exception as e:
            logger.critical(f"Unhandled exception: {str(e)}")
            raise RuntimeError("Critical error creating session") from e

    @staticmethod
    def _create_local_session(ml_config: Optional[Dict[str, Any]] = None):
        """Create a local Spark session with ML optimizations."""
        try:
            from pyspark.sql import SparkSession  # type: ignore

            os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
            builder = SparkSession.builder.appName("TauroLocal").master("local[*]")

            if ml_config and isinstance(ml_config, dict):
                apply_fn = getattr(SparkSessionFactory, "_apply_ml_configs", None)
                if callable(apply_fn):
                    builder = apply_fn(builder, ml_config)

            return builder.getOrCreate()
        except Exception:
            logger.error("Session creation failed", exc_info=True)
            raise

    @staticmethod
    def _validate_databricks_config(config) -> None:
        """Validate required Databricks configuration parameters."""
        required = ["host", "token", "cluster_id"]
        missing = [k for k in required if not getattr(config, k, None)]
        if missing:
            raise ValueError(f"Missing Databricks config values: {', '.join(missing)}")

    @staticmethod
    def _apply_ml_configs(builder: Any, ml_config: Dict[str, Any]) -> Any:
        """Apply ML-related configurations to the Spark builder."""
        protected = set(SparkSessionFactory.PROTECTED_CONFIGS or [])
        for k, v in (ml_config or {}).items():
            if k in protected:
                logger.warning(f"Skipping ML config '{k}' because it's in PROTECTED_CONFIGS")
                continue
            try:
                builder = builder.config(k, v)
            except Exception:
                logger.debug(f"Failed to apply Spark config {k}={v}", exc_info=True)
        return builder
