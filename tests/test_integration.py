"""
Integration tests for the full market data simulator system.

These tests verify end-to-end behaviour including:
- Full system startup and shutdown
- Performance characteristics
- Error handling and recovery
"""

import pytest
import asyncio
import time

from src.config import SimulatorConfig
from src.generator import MarketDataGenerator
from src.streamer import MarketDataStreamer
from src.subscriber import Subscriber
from src.monitor import PerformanceMonitor


class TestSystemIntegration:
    """Full system integration tests."""

    @pytest.mark.asyncio
    async def test_full_system_startup_shutdown(self):
        """Test complete system lifecycle."""
        config = SimulatorConfig(
            instruments=("AAPL", "GOOGL", "MSFT"),
            tick_rate_per_instrument=100,
            num_subscribers=5,
            enable_monitoring=True,
            monitoring_interval_seconds=0.1,
        )

        # Track received messages with handlers
        received_counts = [0] * 5

        def make_handler(idx):
            async def handler(tick):
                received_counts[idx] += 1
            return handler

        # Create subscribers with handlers
        subscribers = [
            Subscriber(f"sub-{i}", queue_size=1000, handler=make_handler(i))
            for i in range(5)
        ]

        # Create streamer
        streamer = MarketDataStreamer(config, seed=42)
        for sub in subscribers:
            streamer.add_subscriber(sub)

        # Start
        await streamer.start(realtime=True)
        assert streamer.is_running

        # Run for a bit
        await asyncio.sleep(0.5)

        # Verify ticks are flowing
        assert streamer.total_published > 0
        for count in received_counts:
            assert count > 0

        # Stop
        await streamer.stop()
        assert not streamer.is_running

    @pytest.mark.asyncio
    async def test_benchmark_mode_throughput(self):
        """Test that benchmark mode achieves high throughput."""
        config = SimulatorConfig(
            instruments=("AAPL",),
            tick_rate_per_instrument=1000,
            enable_monitoring=False,
        )

        sub = Subscriber("bench", queue_size=100_000)
        streamer = MarketDataStreamer(config, seed=42)
        streamer.add_subscriber(sub)

        start = time.time()
        await streamer.start(realtime=False)
        await asyncio.sleep(1.0)
        await streamer.stop()
        elapsed = time.time() - start

        # Should achieve at least 10k msg/s in benchmark mode
        rate = streamer.total_published / elapsed
        assert rate > 10_000, f"Rate too low: {rate:.0f} msg/s"

    @pytest.mark.asyncio
    async def test_latency_within_bounds(self):
        """Test that latency stays within acceptable bounds."""
        config = SimulatorConfig(
            instruments=("AAPL",),
            tick_rate_per_instrument=100,
            enable_monitoring=True,
        )

        latencies = []

        async def handler(tick):
            latency_us = (time.time() - tick.timestamp) * 1_000_000
            latencies.append(latency_us)

        sub = Subscriber("latency-test", handler=handler)
        streamer = MarketDataStreamer(config, seed=42)
        streamer.add_subscriber(sub)

        await streamer.start(realtime=True)
        await asyncio.sleep(0.5)
        await streamer.stop()

        import numpy as np
        latencies_arr = np.array(latencies)
        p99 = np.percentile(latencies_arr, 99)

        # p99 latency should be under 10ms for local processing
        assert p99 < 10_000, f"p99 latency too high: {p99:.0f}μs"

    @pytest.mark.asyncio
    async def test_graceful_handling_of_slow_subscriber(self):
        """Test that slow subscribers don't crash the system."""
        config = SimulatorConfig(
            instruments=("AAPL",),
            tick_rate_per_instrument=500,
            queue_size=100,  # Minimum allowed
            backpressure_policy="drop",
            enable_monitoring=False,
        )

        slow_count = 0
        fast_count = 0

        async def very_slow_handler(tick):
            nonlocal slow_count
            await asyncio.sleep(0.1)
            slow_count += 1

        async def fast_handler(tick):
            nonlocal fast_count
            fast_count += 1

        slow_sub = Subscriber("slow", handler=very_slow_handler, queue_size=100)
        fast_sub = Subscriber("fast", handler=fast_handler, queue_size=10000)

        streamer = MarketDataStreamer(config, seed=42)
        streamer.add_subscriber(slow_sub)
        streamer.add_subscriber(fast_sub)

        await streamer.start(realtime=True)
        await asyncio.sleep(0.5)
        await streamer.stop()

        # System should still be functional
        assert streamer.total_published > 0

        # Slow subscriber should have drops
        assert slow_sub.stats.messages_dropped > 0

        # Fast subscriber should receive more messages
        assert fast_count > slow_count

    @pytest.mark.asyncio
    async def test_multi_instrument_filtering(self):
        """Test instrument filtering with multiple subscribers."""
        config = SimulatorConfig(
            instruments=("AAPL", "GOOGL", "MSFT"),
            tick_rate_per_instrument=100,
            enable_monitoring=False,
        )

        aapl_ticks = []
        googl_ticks = []
        all_ticks = []

        async def aapl_handler(tick):
            aapl_ticks.append(tick)

        async def googl_handler(tick):
            googl_ticks.append(tick)

        async def all_handler(tick):
            all_ticks.append(tick)

        sub_aapl = Subscriber("aapl", instruments={"AAPL"}, handler=aapl_handler)
        sub_googl = Subscriber("googl", instruments={"GOOGL"}, handler=googl_handler)
        sub_all = Subscriber("all", handler=all_handler)

        streamer = MarketDataStreamer(config, seed=42)
        streamer.add_subscriber(sub_aapl)
        streamer.add_subscriber(sub_googl)
        streamer.add_subscriber(sub_all)

        await streamer.start(realtime=True)
        await asyncio.sleep(0.3)
        await streamer.stop()

        # AAPL subscriber should only get AAPL
        assert all(t.instrument == "AAPL" for t in aapl_ticks)
        assert len(aapl_ticks) > 0

        # GOOGL subscriber should only get GOOGL
        assert all(t.instrument == "GOOGL" for t in googl_ticks)
        assert len(googl_ticks) > 0

        # All subscriber should get everything
        instruments = {t.instrument for t in all_ticks}
        assert instruments == {"AAPL", "GOOGL", "MSFT"}


class TestPerformanceMonitor:
    """Tests for performance monitoring."""

    def test_latency_percentiles(self):
        """Test latency percentile calculation."""
        config = SimulatorConfig(enable_monitoring=True)
        monitor = PerformanceMonitor(config, window_size=1000)

        # Simulate some publishes with known latencies
        from src.generator import MarketTick

        base_time = time.time()
        for i in range(100):
            tick = MarketTick(
                instrument="AAPL",
                timestamp=base_time - (i * 0.001),  # Varying latencies
                price=175.0,
                volume=100,
                bid=174.98,
                ask=175.02,
                sequence=i,
            )
            monitor.record_publish(tick, delivered=1, dropped=0)

        stats = monitor.calculate_latency_stats()

        assert stats.count == 100
        assert stats.min_ns > 0
        assert stats.p50_ns > 0
        assert stats.p95_ns >= stats.p50_ns
        assert stats.p99_ns >= stats.p95_ns

    def test_throughput_calculation(self):
        """Test throughput calculation."""
        config = SimulatorConfig(enable_monitoring=True)
        monitor = PerformanceMonitor(config)

        from src.generator import MarketTick

        # Publish many ticks
        for i in range(1000):
            tick = MarketTick(
                instrument="AAPL",
                timestamp=time.time(),
                price=175.0,
                volume=100,
                bid=174.98,
                ask=175.02,
                sequence=i,
            )
            monitor.record_publish(tick, delivered=1, dropped=0)

        stats = monitor.calculate_throughput_stats()

        assert stats.total_messages == 1000
        assert stats.messages_per_second > 0

    def test_drop_rate_tracking(self):
        """Test drop rate tracking."""
        config = SimulatorConfig(enable_monitoring=True)
        monitor = PerformanceMonitor(config)

        from src.generator import MarketTick

        # 80% delivered, 20% dropped
        for i in range(100):
            tick = MarketTick(
                instrument="AAPL",
                timestamp=time.time(),
                price=175.0,
                volume=100,
                bid=174.98,
                ask=175.02,
                sequence=i,
            )
            if i < 80:
                monitor.record_publish(tick, delivered=1, dropped=0)
            else:
                monitor.record_publish(tick, delivered=0, dropped=1)

        stats = monitor.calculate_throughput_stats()

        # 20 out of 100 dropped = 20%
        assert 19 < stats.drop_rate_percent < 21


class TestReproducibility:
    """Tests for deterministic behaviour with seeds."""

    @pytest.mark.asyncio
    async def test_same_seed_same_results(self):
        """Test that same seed produces identical results."""
        config = SimulatorConfig(
            instruments=("AAPL",),
            tick_rate_per_instrument=10,
            enable_monitoring=False,
        )

        async def collect_prices(seed):
            prices = []

            async def handler(tick):
                prices.append(tick.price)

            sub = Subscriber("test", handler=handler)
            streamer = MarketDataStreamer(config, seed=seed)
            streamer.add_subscriber(sub)

            await streamer.start(realtime=True)
            await asyncio.sleep(0.1)
            await streamer.stop()

            return prices

        prices1 = await collect_prices(12345)
        prices2 = await collect_prices(12345)

        # Should be identical
        assert prices1 == prices2
