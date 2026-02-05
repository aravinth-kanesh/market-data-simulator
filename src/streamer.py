"""
Asyncio-based market data streaming infrastructure.

The streamer is the central coordinator that:
- Generates market data at configurable rates
- Distributes ticks to all registered subscribers
- Handles backpressure across multiple subscribers
- Provides graceful shutdown

Design Decisions:
1. Asyncio over threading: Better for I/O-bound workloads, easier to reason about,
   no GIL contention, and more efficient for many concurrent connections.
2. Pull-based generation with push-based delivery: Generator produces on demand,
   but pushes to subscriber queues immediately.
3. Single-threaded event loop: Avoids synchronisation overhead. For CPU-bound
   workloads, could use ProcessPoolExecutor for generation.
4. Configurable tick timing: Can run at real-time pace or as fast as possible.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from collections.abc import Callable
from contextlib import suppress
from typing import Any

from .config import SimulatorConfig
from .generator import MarketDataGenerator, MarketTick
from .subscriber import Subscriber
from .monitor import PerformanceMonitor


logger = logging.getLogger(__name__)


class MarketDataStreamer:
    """
    High-performance market data streaming server.

    Coordinates generation and distribution of market data to multiple
    subscribers with configurable timing and backpressure handling.

    Architecture:
    - Main loop generates ticks at target rate
    - Each tick is pushed to all interested subscribers
    - Subscribers filter by instrument subscription
    - Monitor tracks performance metrics

    Usage:
        config = SimulatorConfig(tick_rate_per_instrument=100)
        streamer = MarketDataStreamer(config)

        # Add subscribers
        sub = Subscriber("client-1")
        streamer.add_subscriber(sub)

        # Run the streamer
        await streamer.start()

    Performance Notes:
    - Target 100k+ messages/second total throughput
    - Sub-millisecond latency for local subscribers
    - Memory-efficient with bounded queues
    """

    __slots__ = (
        "_config",
        "_generator",
        "_subscribers",
        "_monitor",
        "_running",
        "_paused",
        "_main_task",
        "_monitor_task",
        "_total_published",
        "_total_dropped",
        "_start_time",
        "_shutdown_event",
    )

    def __init__(
        self,
        config: SimulatorConfig,
        generator: MarketDataGenerator | None = None,
        seed: int | None = None,
    ) -> None:
        """
        Initialise the streamer.

        Args:
            config: Simulator configuration.
            generator: Optional custom generator. If None, creates default.
            seed: Random seed for reproducibility (passed to generator).
        """
        self._config = config
        self._generator = generator or MarketDataGenerator(config, seed=seed)
        self._subscribers: dict[str, Subscriber] = {}
        self._monitor = PerformanceMonitor(config) if config.enable_monitoring else None

        self._running = False
        self._paused = False
        self._main_task: asyncio.Task | None = None
        self._monitor_task: asyncio.Task | None = None

        self._total_published = 0
        self._total_dropped = 0
        self._start_time: float = 0
        self._shutdown_event = asyncio.Event()

    @property
    def is_running(self) -> bool:
        """Whether the streamer is currently running."""
        return self._running

    @property
    def subscriber_count(self) -> int:
        """Number of registered subscribers."""
        return len(self._subscribers)

    @property
    def total_published(self) -> int:
        """Total ticks published since start."""
        return self._total_published

    @property
    def total_dropped(self) -> int:
        """Total ticks dropped due to backpressure."""
        return self._total_dropped

    @property
    def throughput(self) -> float:
        """Current throughput in messages per second."""
        if self._start_time == 0:
            return 0.0
        elapsed = time.time() - self._start_time
        if elapsed <= 0:
            return 0.0
        return self._total_published / elapsed

    def add_subscriber(self, subscriber: Subscriber) -> None:
        """
        Register a subscriber to receive market data.

        Args:
            subscriber: The subscriber to add.

        Raises:
            ValueError: If subscriber ID already exists.
        """
        if subscriber.id in self._subscribers:
            raise ValueError(f"Subscriber {subscriber.id} already registered")

        self._subscribers[subscriber.id] = subscriber
        logger.info(f"Added subscriber: {subscriber}")

    def remove_subscriber(self, subscriber_id: str) -> Subscriber | None:
        """
        Unregister a subscriber.

        Args:
            subscriber_id: ID of subscriber to remove.

        Returns:
            The removed subscriber, or None if not found.
        """
        subscriber = self._subscribers.pop(subscriber_id, None)
        if subscriber:
            logger.info(f"Removed subscriber: {subscriber_id}")
        return subscriber

    def get_subscriber(self, subscriber_id: str) -> Subscriber | None:
        """Get a subscriber by ID."""
        return self._subscribers.get(subscriber_id)

    async def publish(self, tick: MarketTick) -> int:
        """
        Publish a tick to all interested subscribers.

        Args:
            tick: The tick to publish.

        Returns:
            Number of subscribers that received the tick.
        """
        delivered = 0
        dropped = 0

        for subscriber in self._subscribers.values():
            if subscriber.wants_instrument(tick.instrument):
                success = await subscriber.deliver(tick)
                if success:
                    delivered += 1
                else:
                    dropped += 1

        self._total_published += 1
        self._total_dropped += dropped

        # Record in monitor
        if self._monitor:
            self._monitor.record_publish(tick, delivered, dropped)

        return delivered

    async def start(self, realtime: bool = True) -> None:
        """
        Start the streaming server.

        Args:
            realtime: If True, generate at configured rate. If False,
                     generate as fast as possible (for benchmarking).
        """
        if self._running:
            logger.warning("Streamer already running")
            return

        self._running = True
        self._paused = False
        self._start_time = time.time()
        self._shutdown_event.clear()

        logger.info(
            f"Starting streamer: {len(self._generator.instruments)} instruments, "
            f"{self._config.tick_rate_per_instrument} ticks/s/instrument, "
            f"{len(self._subscribers)} subscribers"
        )

        # Start all subscriber handlers
        for subscriber in self._subscribers.values():
            if subscriber._handler is not None:
                await subscriber.start()

        # Start monitoring if enabled
        if self._monitor:
            self._monitor_task = asyncio.create_task(self._monitoring_loop())

        # Start main generation loop
        if realtime:
            self._main_task = asyncio.create_task(self._realtime_loop())
        else:
            self._main_task = asyncio.create_task(self._fast_loop())

    async def stop(self) -> None:
        """Stop the streaming server gracefully."""
        if not self._running:
            return

        logger.info("Stopping streamer...")
        self._running = False
        self._shutdown_event.set()

        # Cancel main task
        if self._main_task:
            self._main_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._main_task
            self._main_task = None

        # Cancel monitor task
        if self._monitor_task:
            self._monitor_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._monitor_task
            self._monitor_task = None

        # Stop all subscribers
        for subscriber in self._subscribers.values():
            await subscriber.stop()

        # Final metrics
        elapsed = time.time() - self._start_time
        logger.info(
            f"Streamer stopped. Published {self._total_published:,} ticks "
            f"in {elapsed:.2f}s ({self._total_published/elapsed:,.0f} msg/s)"
        )

    def pause(self) -> None:
        """Pause tick generation (subscribers stay connected)."""
        self._paused = True
        logger.info("Streamer paused")

    def resume(self) -> None:
        """Resume tick generation."""
        self._paused = False
        logger.info("Streamer resumed")

    async def _realtime_loop(self) -> None:
        """
        Main loop for real-time tick generation.

        Generates ticks at the configured rate, sleeping between ticks
        to maintain proper timing.
        """
        # Calculate interval between ticks
        total_rate = self._config.total_tick_rate
        tick_interval = 1.0 / total_rate

        # Use monotonic clock for accurate timing
        next_tick_time = time.monotonic()

        while self._running:
            if self._paused:
                await asyncio.sleep(0.1)
                next_tick_time = time.monotonic()
                continue

            # Generate and publish tick
            tick = self._generator.next_tick()
            await self.publish(tick)

            # Calculate sleep time to maintain rate
            next_tick_time += tick_interval
            sleep_time = next_tick_time - time.monotonic()

            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            else:
                # Falling behind - skip sleep and catch up
                # Reset timing if we're too far behind
                if sleep_time < -0.1:
                    next_tick_time = time.monotonic()

    async def _fast_loop(self) -> None:
        """
        Main loop for maximum throughput (benchmarking mode).

        Generates ticks as fast as possible without timing constraints.
        Yields to event loop periodically to prevent starvation.
        """
        batch_size = 100  # Yield every N ticks

        while self._running:
            if self._paused:
                await asyncio.sleep(0.1)
                continue

            for _ in range(batch_size):
                if not self._running:
                    return
                tick = self._generator.next_tick()
                await self.publish(tick)

            # Yield to event loop
            await asyncio.sleep(0)

    async def _monitoring_loop(self) -> None:
        """Periodic monitoring and reporting loop."""
        while self._running:
            await asyncio.sleep(self._config.monitoring_interval_seconds)

            if self._monitor:
                self._monitor.report()

                # Also report subscriber stats
                for sub in self._subscribers.values():
                    stats = sub.stats
                    logger.info(
                        f"Subscriber {sub.id}: received={stats.messages_received:,}, "
                        f"dropped={stats.messages_dropped:,}, "
                        f"queue={sub.queue_size}, "
                        f"avg_latency={stats.avg_latency_ns/1000:.1f}μs"
                    )

    async def run_until_shutdown(self, realtime: bool = True) -> None:
        """
        Start and run until interrupted.

        Handles SIGINT and SIGTERM for graceful shutdown.

        Args:
            realtime: If True, run at configured rate.
        """
        loop = asyncio.get_running_loop()

        # Setup signal handlers
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._handle_signal)

        try:
            await self.start(realtime=realtime)
            await self._shutdown_event.wait()
        finally:
            await self.stop()

    def _handle_signal(self) -> None:
        """Handle shutdown signal."""
        logger.info("Received shutdown signal")
        self._running = False
        self._shutdown_event.set()

    async def __aenter__(self) -> "MarketDataStreamer":
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args) -> None:
        """Async context manager exit."""
        await self.stop()

    def __repr__(self) -> str:
        status = "running" if self._running else "stopped"
        return (
            f"MarketDataStreamer(status={status}, "
            f"subscribers={len(self._subscribers)}, "
            f"published={self._total_published:,})"
        )
