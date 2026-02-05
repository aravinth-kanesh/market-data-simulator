"""
Subscriber implementation for market data streams.

Subscribers are the consumers of market data. Each subscriber:
- Has its own asyncio queue for message delivery
- Can subscribe to specific instruments or all instruments
- Handles backpressure through configurable policies
- Tracks delivery statistics for monitoring

Design Decisions:
1. Per-subscriber queues: Isolates slow consumers from fast ones.
2. Instrument filtering: Reduces unnecessary data transfer.
3. Callback-based processing: Allows flexible integration.
4. Async context manager: Ensures proper cleanup.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol

from .generator import MarketTick


logger = logging.getLogger(__name__)


class TickHandler(Protocol):
    """Protocol for tick handler callbacks."""

    async def __call__(self, tick: MarketTick) -> None:
        """Process a received tick."""
        ...


@dataclass
class SubscriberStats:
    """Statistics for a subscriber's message handling."""

    messages_received: int = 0
    messages_dropped: int = 0
    total_latency_ns: int = 0
    max_latency_ns: int = 0
    last_sequence: dict[str, int] = field(default_factory=dict)
    gaps_detected: int = 0

    @property
    def avg_latency_ns(self) -> float:
        """Average latency in nanoseconds."""
        if self.messages_received == 0:
            return 0.0
        return self.total_latency_ns / self.messages_received

    @property
    def drop_rate(self) -> float:
        """Percentage of messages dropped."""
        total = self.messages_received + self.messages_dropped
        if total == 0:
            return 0.0
        return (self.messages_dropped / total) * 100


class Subscriber:
    """
    A subscriber to market data streams.

    Subscribers receive market data through an asyncio queue and process
    it either through callbacks or by reading from the queue directly.

    Features:
    - Instrument filtering (subscribe to specific symbols)
    - Backpressure handling (drop or block when queue full)
    - Statistics tracking (latency, drops, gaps)
    - Async context manager support

    Usage:
        # With callback
        async def on_tick(tick):
            print(f"Received: {tick}")

        subscriber = Subscriber("sub-1", handler=on_tick)

        # Or read directly
        subscriber = Subscriber("sub-2")
        tick = await subscriber.receive()

    Thread Safety:
        Subscribers are NOT thread-safe. Each subscriber should be used
        from a single asyncio task. Multiple tasks can have their own
        subscribers.
    """

    __slots__ = (
        "_id",
        "_queue",
        "_instruments",
        "_handler",
        "_backpressure_policy",
        "_stats",
        "_running",
        "_process_task",
    )

    def __init__(
        self,
        subscriber_id: str,
        queue_size: int = 10_000,
        instruments: set[str] | None = None,
        handler: TickHandler | None = None,
        backpressure_policy: str = "drop",
    ) -> None:
        """
        Initialise a subscriber.

        Args:
            subscriber_id: Unique identifier for this subscriber.
            queue_size: Maximum queue depth before backpressure kicks in.
            instruments: Set of instruments to subscribe to, or None for all.
            handler: Async callback to invoke for each tick.
            backpressure_policy: 'drop' to drop messages, 'block' to wait.
        """
        self._id = subscriber_id
        self._queue: asyncio.Queue[MarketTick] = asyncio.Queue(maxsize=queue_size)
        self._instruments = instruments  # None means subscribe to all
        self._handler = handler
        self._backpressure_policy = backpressure_policy
        self._stats = SubscriberStats()
        self._running = False
        self._process_task: asyncio.Task | None = None

    @property
    def id(self) -> str:
        """Subscriber's unique identifier."""
        return self._id

    @property
    def stats(self) -> SubscriberStats:
        """Current statistics."""
        return self._stats

    @property
    def queue_size(self) -> int:
        """Current number of messages in queue."""
        return self._queue.qsize()

    @property
    def is_running(self) -> bool:
        """Whether the subscriber is actively processing."""
        return self._running

    def wants_instrument(self, instrument: str) -> bool:
        """
        Check if this subscriber wants data for an instrument.

        Args:
            instrument: Instrument symbol.

        Returns:
            True if subscribed to this instrument.
        """
        return self._instruments is None or instrument in self._instruments

    async def deliver(self, tick: MarketTick) -> bool:
        """
        Deliver a tick to this subscriber.

        Called by the streamer to push data to the subscriber's queue.
        Handles backpressure according to the configured policy.

        Args:
            tick: The market tick to deliver.

        Returns:
            True if delivered, False if dropped.
        """
        # Check instrument filter
        if not self.wants_instrument(tick.instrument):
            return True  # Not interested, but not a drop

        try:
            if self._backpressure_policy == "drop":
                # Non-blocking put with immediate failure if full
                self._queue.put_nowait(tick)
            else:
                # Blocking put (will wait for space)
                await self._queue.put(tick)

            return True

        except asyncio.QueueFull:
            self._stats.messages_dropped += 1
            if self._stats.messages_dropped % 1000 == 1:
                logger.warning(
                    f"Subscriber {self._id}: queue full, dropped message "
                    f"(total drops: {self._stats.messages_dropped})"
                )
            return False

    async def receive(self, timeout: float | None = None) -> MarketTick | None:
        """
        Receive the next tick from the queue.

        Args:
            timeout: Maximum seconds to wait, or None for indefinite.

        Returns:
            The next MarketTick, or None if timeout.
        """
        try:
            if timeout is not None:
                tick = await asyncio.wait_for(self._queue.get(), timeout=timeout)
            else:
                tick = await self._queue.get()

            self._record_receipt(tick)
            return tick

        except asyncio.TimeoutError:
            return None

    def _record_receipt(self, tick: MarketTick) -> None:
        """Record statistics for a received tick."""
        # Calculate latency
        now = time.time()
        latency_ns = int((now - tick.timestamp) * 1_000_000_000)
        self._stats.messages_received += 1
        self._stats.total_latency_ns += latency_ns
        self._stats.max_latency_ns = max(self._stats.max_latency_ns, latency_ns)

        # Check for sequence gaps
        last_seq = self._stats.last_sequence.get(tick.instrument)
        if last_seq is not None and tick.sequence != last_seq + 1:
            self._stats.gaps_detected += 1
            gap_size = tick.sequence - last_seq - 1
            if gap_size > 0:
                logger.debug(
                    f"Subscriber {self._id}: sequence gap detected for "
                    f"{tick.instrument}: {last_seq} -> {tick.sequence}"
                )
        self._stats.last_sequence[tick.instrument] = tick.sequence

    async def start(self) -> None:
        """
        Start the subscriber's processing loop.

        Only needed if a handler was provided. The loop will continuously
        receive ticks and invoke the handler.
        """
        if self._handler is None:
            raise RuntimeError("Cannot start without a handler")

        if self._running:
            return

        self._running = True
        self._process_task = asyncio.create_task(self._process_loop())
        logger.info(f"Subscriber {self._id} started")

    async def stop(self) -> None:
        """Stop the subscriber's processing loop."""
        self._running = False

        if self._process_task is not None:
            self._process_task.cancel()
            try:
                await self._process_task
            except asyncio.CancelledError:
                pass
            self._process_task = None

        logger.info(f"Subscriber {self._id} stopped")

    async def _process_loop(self) -> None:
        """Main processing loop - receives and handles ticks."""
        while self._running:
            try:
                tick = await self._queue.get()
                self._record_receipt(tick)

                if self._handler is not None:
                    await self._handler(tick)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Subscriber {self._id} handler error: {e}")

    async def drain(self) -> list[MarketTick]:
        """
        Drain all pending ticks from the queue.

        Useful for cleanup or batch processing.

        Returns:
            List of all pending ticks.
        """
        ticks = []
        while not self._queue.empty():
            try:
                tick = self._queue.get_nowait()
                self._record_receipt(tick)
                ticks.append(tick)
            except asyncio.QueueEmpty:
                break
        return ticks

    async def __aenter__(self) -> "Subscriber":
        """Async context manager entry."""
        if self._handler is not None:
            await self.start()
        return self

    async def __aexit__(self, *args) -> None:
        """Async context manager exit."""
        await self.stop()

    def __repr__(self) -> str:
        instruments = "all" if self._instruments is None else len(self._instruments)
        return (
            f"Subscriber(id={self._id!r}, instruments={instruments}, "
            f"queue={self.queue_size}, received={self._stats.messages_received})"
        )
