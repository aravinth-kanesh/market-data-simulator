"""
Unit tests for the streaming infrastructure.

Tests verify:
- Subscriber registration and management
- Message delivery to subscribers
- Backpressure handling
- Graceful shutdown
"""

import pytest
import asyncio
import time

from src.config import SimulatorConfig
from src.generator import MarketDataGenerator, MarketTick
from src.streamer import MarketDataStreamer
from src.subscriber import Subscriber, SubscriberStats


class TestSubscriber:
    """Tests for Subscriber class."""

    @pytest.fixture
    def subscriber(self):
        """Create a basic subscriber."""
        return Subscriber(
            subscriber_id="test-sub",
            queue_size=100,
            backpressure_policy="drop",
        )

    def test_subscriber_creation(self, subscriber):
        """Test subscriber initialisation."""
        assert subscriber.id == "test-sub"
        assert subscriber.queue_size == 0
        assert not subscriber.is_running

    def test_wants_instrument_all(self, subscriber):
        """Test that subscriber with no filter wants all instruments."""
        assert subscriber.wants_instrument("AAPL")
        assert subscriber.wants_instrument("GOOGL")
        assert subscriber.wants_instrument("ANY")

    def test_wants_instrument_filtered(self):
        """Test subscriber with instrument filter."""
        sub = Subscriber(
            subscriber_id="filtered",
            instruments={"AAPL", "GOOGL"},
        )

        assert sub.wants_instrument("AAPL")
        assert sub.wants_instrument("GOOGL")
        assert not sub.wants_instrument("MSFT")

    @pytest.mark.asyncio
    async def test_deliver_tick(self, subscriber):
        """Test delivering tick to subscriber."""
        tick = MarketTick(
            instrument="AAPL",
            timestamp=time.time(),
            price=175.0,
            volume=100,
            bid=174.98,
            ask=175.02,
            sequence=1,
        )

        success = await subscriber.deliver(tick)

        assert success
        assert subscriber.queue_size == 1

    @pytest.mark.asyncio
    async def test_receive_tick(self, subscriber):
        """Test receiving tick from subscriber queue."""
        tick = MarketTick(
            instrument="AAPL",
            timestamp=time.time(),
            price=175.0,
            volume=100,
            bid=174.98,
            ask=175.02,
            sequence=1,
        )

        await subscriber.deliver(tick)
        received = await subscriber.receive(timeout=1.0)

        assert received is not None
        assert received.instrument == "AAPL"
        assert subscriber.stats.messages_received == 1

    @pytest.mark.asyncio
    async def test_receive_timeout(self, subscriber):
        """Test receive timeout when queue is empty."""
        received = await subscriber.receive(timeout=0.01)
        assert received is None

    @pytest.mark.asyncio
    async def test_backpressure_drop(self):
        """Test drop policy when queue is full."""
        sub = Subscriber(
            subscriber_id="small-queue",
            queue_size=5,
            backpressure_policy="drop",
        )

        tick = MarketTick(
            instrument="AAPL",
            timestamp=time.time(),
            price=175.0,
            volume=100,
            bid=174.98,
            ask=175.02,
            sequence=1,
        )

        # Fill the queue
        for _ in range(5):
            await sub.deliver(tick)

        assert sub.queue_size == 5

        # Next delivery should drop
        success = await sub.deliver(tick)
        assert not success
        assert sub.stats.messages_dropped == 1

    @pytest.mark.asyncio
    async def test_sequence_gap_detection(self, subscriber):
        """Test detection of sequence gaps."""
        tick1 = MarketTick(
            instrument="AAPL",
            timestamp=time.time(),
            price=175.0,
            volume=100,
            bid=174.98,
            ask=175.02,
            sequence=1,
        )

        tick3 = MarketTick(
            instrument="AAPL",
            timestamp=time.time(),
            price=175.0,
            volume=100,
            bid=174.98,
            ask=175.02,
            sequence=3,  # Skip sequence 2
        )

        await subscriber.deliver(tick1)
        await subscriber.receive(timeout=0.1)

        await subscriber.deliver(tick3)
        await subscriber.receive(timeout=0.1)

        assert subscriber.stats.gaps_detected == 1

    @pytest.mark.asyncio
    async def test_latency_tracking(self, subscriber):
        """Test latency statistics are tracked."""
        tick = MarketTick(
            instrument="AAPL",
            timestamp=time.time() - 0.001,  # 1ms ago
            price=175.0,
            volume=100,
            bid=174.98,
            ask=175.02,
            sequence=1,
        )

        await subscriber.deliver(tick)
        await subscriber.receive(timeout=0.1)

        # Should have recorded some latency
        assert subscriber.stats.total_latency_ns > 0
        assert subscriber.stats.max_latency_ns > 0

    @pytest.mark.asyncio
    async def test_drain(self, subscriber):
        """Test draining all pending ticks."""
        for i in range(5):
            tick = MarketTick(
                instrument="AAPL",
                timestamp=time.time(),
                price=175.0 + i,
                volume=100,
                bid=174.98,
                ask=175.02,
                sequence=i + 1,
            )
            await subscriber.deliver(tick)

        drained = await subscriber.drain()

        assert len(drained) == 5
        assert subscriber.queue_size == 0

    @pytest.mark.asyncio
    async def test_handler_callback(self):
        """Test subscriber with handler callback."""
        received_ticks = []

        async def handler(tick: MarketTick) -> None:
            received_ticks.append(tick)

        sub = Subscriber(
            subscriber_id="with-handler",
            handler=handler,
        )

        tick = MarketTick(
            instrument="AAPL",
            timestamp=time.time(),
            price=175.0,
            volume=100,
            bid=174.98,
            ask=175.02,
            sequence=1,
        )

        # Start subscriber
        await sub.start()

        # Deliver tick
        await sub.deliver(tick)

        # Give handler time to process
        await asyncio.sleep(0.1)

        # Stop subscriber
        await sub.stop()

        assert len(received_ticks) == 1
        assert received_ticks[0].instrument == "AAPL"


class TestMarketDataStreamer:
    """Tests for MarketDataStreamer class."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return SimulatorConfig(
            instruments=("AAPL", "GOOGL"),
            tick_rate_per_instrument=100,
            num_subscribers=2,
            queue_size=1000,
            enable_monitoring=False,  # Disable for tests
        )

    @pytest.fixture
    def streamer(self, config):
        """Create test streamer."""
        return MarketDataStreamer(config, seed=42)

    def test_streamer_creation(self, streamer, config):
        """Test streamer initialisation."""
        assert streamer.subscriber_count == 0
        assert not streamer.is_running

    def test_add_subscriber(self, streamer):
        """Test adding subscriber."""
        sub = Subscriber("test-sub")
        streamer.add_subscriber(sub)

        assert streamer.subscriber_count == 1
        assert streamer.get_subscriber("test-sub") is sub

    def test_add_duplicate_subscriber(self, streamer):
        """Test error on duplicate subscriber ID."""
        sub1 = Subscriber("test-sub")
        sub2 = Subscriber("test-sub")

        streamer.add_subscriber(sub1)

        with pytest.raises(ValueError):
            streamer.add_subscriber(sub2)

    def test_remove_subscriber(self, streamer):
        """Test removing subscriber."""
        sub = Subscriber("test-sub")
        streamer.add_subscriber(sub)

        removed = streamer.remove_subscriber("test-sub")

        assert removed is sub
        assert streamer.subscriber_count == 0

    def test_remove_nonexistent_subscriber(self, streamer):
        """Test removing non-existent subscriber."""
        removed = streamer.remove_subscriber("nonexistent")
        assert removed is None

    @pytest.mark.asyncio
    async def test_publish(self, streamer):
        """Test publishing tick to subscribers."""
        sub1 = Subscriber("sub-1")
        sub2 = Subscriber("sub-2")

        streamer.add_subscriber(sub1)
        streamer.add_subscriber(sub2)

        tick = MarketTick(
            instrument="AAPL",
            timestamp=time.time(),
            price=175.0,
            volume=100,
            bid=174.98,
            ask=175.02,
            sequence=1,
        )

        delivered = await streamer.publish(tick)

        assert delivered == 2
        assert sub1.queue_size == 1
        assert sub2.queue_size == 1

    @pytest.mark.asyncio
    async def test_publish_with_filter(self, streamer):
        """Test publishing respects instrument filters."""
        sub_aapl = Subscriber("sub-aapl", instruments={"AAPL"})
        sub_googl = Subscriber("sub-googl", instruments={"GOOGL"})

        streamer.add_subscriber(sub_aapl)
        streamer.add_subscriber(sub_googl)

        tick_aapl = MarketTick(
            instrument="AAPL",
            timestamp=time.time(),
            price=175.0,
            volume=100,
            bid=174.98,
            ask=175.02,
            sequence=1,
        )

        delivered = await streamer.publish(tick_aapl)

        assert delivered == 1
        assert sub_aapl.queue_size == 1
        assert sub_googl.queue_size == 0

    @pytest.mark.asyncio
    async def test_start_stop(self, streamer):
        """Test starting and stopping streamer."""
        sub = Subscriber("test-sub")
        streamer.add_subscriber(sub)

        # Start and let it run briefly
        await streamer.start(realtime=True)
        assert streamer.is_running

        await asyncio.sleep(0.1)

        # Stop
        await streamer.stop()
        assert not streamer.is_running

        # Should have published some ticks
        assert streamer.total_published > 0

    @pytest.mark.asyncio
    async def test_fast_mode(self, streamer):
        """Test benchmark/fast mode."""
        sub = Subscriber("test-sub")
        streamer.add_subscriber(sub)

        await streamer.start(realtime=False)

        # Let it run briefly
        await asyncio.sleep(0.1)

        await streamer.stop()

        # Should publish many ticks quickly
        assert streamer.total_published > 100

    @pytest.mark.asyncio
    async def test_pause_resume(self, streamer):
        """Test pausing and resuming streamer."""
        sub = Subscriber("test-sub")
        streamer.add_subscriber(sub)

        await streamer.start(realtime=True)
        await asyncio.sleep(0.05)

        # Pause
        streamer.pause()
        count_at_pause = streamer.total_published

        await asyncio.sleep(0.05)

        # Should not have published while paused
        assert streamer.total_published == count_at_pause

        # Resume
        streamer.resume()
        await asyncio.sleep(0.05)

        # Should have published more
        assert streamer.total_published > count_at_pause

        await streamer.stop()

    @pytest.mark.asyncio
    async def test_context_manager(self, config):
        """Test streamer as async context manager."""
        async with MarketDataStreamer(config) as streamer:
            sub = Subscriber("test-sub")
            streamer.add_subscriber(sub)

            await streamer.start(realtime=True)
            await asyncio.sleep(0.05)

        # Should be stopped after context exit
        assert not streamer.is_running


class TestIntegration:
    """Integration tests for the full system."""

    @pytest.mark.asyncio
    async def test_end_to_end_flow(self):
        """Test complete flow from generator to subscriber."""
        config = SimulatorConfig(
            instruments=("AAPL",),
            tick_rate_per_instrument=100,
            enable_monitoring=False,
        )

        received = []

        async def handler(tick: MarketTick) -> None:
            received.append(tick)

        sub = Subscriber("test", handler=handler)
        streamer = MarketDataStreamer(config, seed=42)
        streamer.add_subscriber(sub)

        await streamer.start(realtime=True)
        await asyncio.sleep(0.5)
        await streamer.stop()

        # Should have received ticks
        assert len(received) > 0

        # All should be AAPL
        assert all(t.instrument == "AAPL" for t in received)

        # Prices should be positive
        assert all(t.price > 0 for t in received)

        # Sequences should be incrementing
        sequences = [t.sequence for t in received]
        assert sequences == sorted(sequences)

    @pytest.mark.asyncio
    async def test_multiple_subscribers_isolation(self):
        """Test that slow subscribers don't affect fast ones."""
        config = SimulatorConfig(
            instruments=("AAPL",),
            tick_rate_per_instrument=1000,  # High rate
            queue_size=100,  # Small queue
            backpressure_policy="drop",
            enable_monitoring=False,
        )

        fast_received = []
        slow_received = []

        async def fast_handler(tick: MarketTick) -> None:
            fast_received.append(tick)

        async def slow_handler(tick: MarketTick) -> None:
            await asyncio.sleep(0.01)  # Simulate slow processing
            slow_received.append(tick)

        fast_sub = Subscriber("fast", handler=fast_handler, queue_size=10000)
        slow_sub = Subscriber("slow", handler=slow_handler, queue_size=100)

        streamer = MarketDataStreamer(config, seed=42)
        streamer.add_subscriber(fast_sub)
        streamer.add_subscriber(slow_sub)

        await streamer.start(realtime=False)
        await asyncio.sleep(0.2)
        await streamer.stop()

        # Fast subscriber should receive more
        assert len(fast_received) > len(slow_received)

        # Slow subscriber should have drops
        assert slow_sub.stats.messages_dropped > 0

        # Fast subscriber should have no drops
        assert fast_sub.stats.messages_dropped == 0
