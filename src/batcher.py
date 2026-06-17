from __future__ import annotations
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class PendingRequest:
    inputs: Any
    future: asyncio.Future
    enqueued_at: float = field(default_factory=time.perf_counter)


class DynamicBatcher:
    """
    Groups prediction requests into batches for GPU efficiency.

    A batch of 16 requests uses the same GPU wall-clock time as a batch of 1
    for most transformer architectures — this is the highest-impact optimization
    for inference throughput.

    Flush triggers:
    1. Queue reaches max_batch_size
    2. batch_timeout_ms elapses since first request in window

    Each caller awaits a Future — resolved when their batch completes.
    Keeps the FastAPI event loop non-blocking between batch submits.
    """

    def __init__(self, predict_fn, max_batch_size: int = 16, batch_timeout_ms: float = 10.0):
        self._predict_fn = predict_fn
        self._max_batch_size = max_batch_size
        self._batch_timeout_s = batch_timeout_ms / 1000.0
        self._queue: asyncio.Queue[PendingRequest] = asyncio.Queue()
        self._flush_task: Optional[asyncio.Task] = None

    async def start(self):
        """Start background flush loop. Call once on app startup."""
        self._flush_task = asyncio.create_task(self._flush_loop())
        logger.info("DynamicBatcher started: max_batch=%d timeout=%.1fms",
                    self._max_batch_size, self._batch_timeout_s * 1000)

    async def stop(self):
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

    async def predict(self, inputs: Any) -> Any:
        """Submit a single prediction. Blocks until batch containing it is processed."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        await self._queue.put(PendingRequest(inputs=inputs, future=future))
        return await future

    async def _flush_loop(self):
        while True:
            first = await self._queue.get()
            batch = [first]
            deadline = first.enqueued_at + self._batch_timeout_s

            while len(batch) < self._max_batch_size:
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    break
                try:
                    req = await asyncio.wait_for(self._queue.get(), timeout=remaining)
                    batch.append(req)
                except asyncio.TimeoutError:
                    break

            await self._dispatch(batch)

    async def _dispatch(self, batch: list[PendingRequest]):
        """Run the model on the batch and resolve each caller's future."""
        inputs = [req.inputs for req in batch]
        t0 = time.perf_counter()
        try:
            outputs = await self._predict_fn(inputs)
            latency_ms = (time.perf_counter() - t0) * 1000
            logger.debug("Batch size=%d latency=%.1fms", len(batch), latency_ms)
            for req, output in zip(batch, outputs):
                if not req.future.done():
                    req.future.set_result(output)
        except Exception as exc:
            logger.exception("Batch prediction failed: %s", exc)
            for req in batch:
                if not req.future.done():
                    req.future.set_exception(exc)
