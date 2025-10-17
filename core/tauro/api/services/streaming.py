from typing import Any, Dict, Optional
import asyncio
import time

from loguru import logger

from tauro.api.services.base import BaseService, ServiceConfig, ServicePriority


class StreamingService(BaseService):
    """Service for streaming pipeline management."""

    def __init__(self, store, runner=None, config: Optional[ServiceConfig] = None):
        super().__init__(config, "StreamingService")
        self.store = store
        self.runner = runner

        # Active streams tracking
        self._active_streams: Dict[str, Dict[str, Any]] = {}
        self._streams_lock = asyncio.Lock()

    async def _initialize(self) -> None:
        """Initialize streaming service"""
        logger.info("Streaming service initialized")

    async def _cleanup(self) -> None:
        """Cleanup streaming service"""
        # Stop all active streams
        async with self._streams_lock:
            stream_ids = list(self._active_streams.keys())

        for stream_id in stream_ids:
            try:
                await self.stop_stream(stream_id)
            except Exception as e:
                logger.error(f"Error stopping stream {stream_id}: {e}")

        logger.info("Streaming service cleaned up")

    async def start_stream(
        self,
        pipeline_id: str,
        config: Dict[str, Any],
        priority: ServicePriority = ServicePriority.NORMAL,
    ) -> str:
        """
        Start a streaming pipeline.

        Args:
            pipeline_id: Pipeline identifier
            config: Stream configuration
            priority: Execution priority

        Returns:
            Stream ID
        """
        async with self._acquire_slot(priority):
            try:
                # Generate stream ID
                stream_id = f"stream_{pipeline_id}_{int(time.time())}"

                # Register stream
                async with self._streams_lock:
                    self._active_streams[stream_id] = {
                        "pipeline_id": pipeline_id,
                        "config": config,
                        "started_at": time.time(),
                        "status": "starting",
                        "messages_processed": 0,
                        "errors": 0,
                    }

                # Start stream processing task
                task = asyncio.create_task(self._process_stream(stream_id))

                async with self._streams_lock:
                    self._active_streams[stream_id]["task"] = task

                async with self._lock:
                    self.metrics.successful_requests += 1

                logger.info(f"Started stream {stream_id} for pipeline {pipeline_id}")

                return stream_id

            except Exception as e:
                async with self._lock:
                    self.metrics.failed_requests += 1
                logger.exception(f"Failed to start stream for {pipeline_id}: {e}")
                raise

    async def stop_stream(self, stream_id: str) -> bool:
        """
        Stop a streaming pipeline.

        Args:
            stream_id: Stream identifier

        Returns:
            True if stopped, False if stream not found
        """
        async with self._acquire_slot(ServicePriority.HIGH):
            try:
                async with self._streams_lock:
                    stream_info = self._active_streams.get(stream_id)

                    if not stream_info:
                        logger.warning(f"Stream {stream_id} not found")
                        return False

                    # Update status
                    stream_info["status"] = "stopping"

                    # Cancel task
                    task = stream_info.get("task")
                    if task and not task.done():
                        task.cancel()
                        await task

                    # Remove stream
                    del self._active_streams[stream_id]

                logger.info(f"Stopped stream {stream_id}")
                return True

            except Exception as e:
                logger.exception(f"Failed to stop stream {stream_id}: {e}")
                raise

    async def get_stream(self, stream_id: str) -> Optional[Dict[str, Any]]:
        """Get stream information"""
        async with self._streams_lock:
            stream_info = self._active_streams.get(stream_id)

            if not stream_info:
                return None

            return {
                "stream_id": stream_id,
                "pipeline_id": stream_info["pipeline_id"],
                "status": stream_info["status"],
                "started_at": stream_info["started_at"],
                "uptime_seconds": time.time() - stream_info["started_at"],
                "messages_processed": stream_info["messages_processed"],
                "errors": stream_info["errors"],
            }

    async def list_streams(self) -> list[Dict[str, Any]]:
        """List all active streams"""
        async with self._streams_lock:
            return [
                {
                    "stream_id": stream_id,
                    "pipeline_id": info["pipeline_id"],
                    "status": info["status"],
                    "started_at": info["started_at"],
                    "messages_processed": info["messages_processed"],
                }
                for stream_id, info in self._active_streams.items()
            ]

    async def _process_stream(self, stream_id: str) -> None:
        """
        Process streaming data.
        Main streaming loop for a single stream.
        """
        try:
            # Update status to running
            async with self._streams_lock:
                if stream_id in self._active_streams:
                    self._active_streams[stream_id]["status"] = "running"

            logger.info(f"Stream {stream_id} processing started")

            # Main streaming loop
            while self.state.value == "running":
                # Check if stream still exists
                async with self._streams_lock:
                    if stream_id not in self._active_streams:
                        break
                    stream_info = self._active_streams[stream_id]

                if not await self._process_iteration(stream_id, stream_info):
                    break

        except asyncio.CancelledError:
            async with self._streams_lock:
                if stream_id in self._active_streams:
                    self._active_streams[stream_id]["status"] = "stopped"
            raise

        except Exception as e:
            logger.exception(f"Stream {stream_id} fatal error: {e}")
            async with self._streams_lock:
                if stream_id in self._active_streams:
                    self._active_streams[stream_id]["status"] = "error"
            logger.info(f"Stream {stream_id} processing stopped")

    async def _perform_health_check(self) -> Dict[str, Any]:
        """Streaming service health check"""
        base_health = await super()._perform_health_check()

        async with self._streams_lock:
            total_messages = sum(
                s["messages_processed"] for s in self._active_streams.values()
            )
            total_errors = sum(s["errors"] for s in self._active_streams.values())

            base_health.update(
                {
                    "active_streams": len(self._active_streams),
                    "streams": list(self._active_streams.keys()),
                    "total_messages_processed": total_messages,
                    "total_errors": total_errors,
                }
            )

        return base_health
