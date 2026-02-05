#!/usr/bin/env python3
"""
Demo script showing basic usage of the market data simulator.

This demonstrates:
1. Creating and configuring the simulator
2. Setting up subscribers with filters
3. Processing market data in real-time
4. Monitoring performance metrics
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import SimulatorConfig
from src.generator import MarketTick
from src.streamer import MarketDataStreamer
from src.subscriber import Subscriber


async def simple_demo():
    """Simple demonstration of the simulator."""
    print("=" * 60)
    print("Market Data Simulator - Simple Demo")
    print("=" * 60)

    # Create configuration
    config = SimulatorConfig(
        instruments=("AAPL", "GOOGL", "MSFT"),
        tick_rate_per_instrument=10,  # Low rate for demo visibility
        enable_monitoring=True,
        monitoring_interval_seconds=2.0,
    )

    # Create a subscriber that prints ticks
    tick_count = 0

    async def print_handler(tick: MarketTick) -> None:
        nonlocal tick_count
        tick_count += 1
        if tick_count <= 20:  # Only print first 20
            print(
                f"  [{tick.instrument}] "
                f"Price: ${tick.price:>8.2f} | "
                f"Bid: ${tick.bid:>8.2f} | "
                f"Ask: ${tick.ask:>8.2f} | "
                f"Vol: {tick.volume:>5}"
            )
        elif tick_count == 21:
            print("  ... (suppressing further output)")

    subscriber = Subscriber(
        subscriber_id="demo-subscriber",
        handler=print_handler,
        queue_size=1000,
    )

    # Create and run streamer
    streamer = MarketDataStreamer(config, seed=42)
    streamer.add_subscriber(subscriber)

    print("\nStarting market data stream...\n")

    await streamer.start(realtime=True)
    await asyncio.sleep(5)  # Run for 5 seconds
    await streamer.stop()

    # Print summary
    stats = subscriber.stats
    print(f"\n{'='*60}")
    print("Demo Complete!")
    print(f"  Total ticks received: {stats.messages_received:,}")
    print(f"  Average latency: {stats.avg_latency_ns/1000:.1f} μs")
    print(f"  Dropped messages: {stats.messages_dropped}")
    print(f"{'='*60}")


async def filtered_subscribers_demo():
    """Demo with multiple filtered subscribers."""
    print("\n" + "=" * 60)
    print("Market Data Simulator - Filtered Subscribers Demo")
    print("=" * 60)

    config = SimulatorConfig(
        instruments=("AAPL", "GOOGL", "MSFT", "AMZN", "META"),
        tick_rate_per_instrument=50,
        enable_monitoring=False,
    )

    # Track ticks per subscriber
    tech_ticks = []
    faang_ticks = []

    async def tech_handler(tick: MarketTick) -> None:
        tech_ticks.append(tick)

    async def faang_handler(tick: MarketTick) -> None:
        faang_ticks.append(tick)

    # Subscriber for specific tech stocks
    tech_subscriber = Subscriber(
        subscriber_id="tech-only",
        instruments={"AAPL", "MSFT"},
        handler=tech_handler,
    )

    # Subscriber for all FAANG stocks
    faang_subscriber = Subscriber(
        subscriber_id="faang-all",
        handler=faang_handler,
    )

    streamer = MarketDataStreamer(config, seed=42)
    streamer.add_subscriber(tech_subscriber)
    streamer.add_subscriber(faang_subscriber)

    print("\nRunning with filtered subscribers...")
    print("  - tech-only: subscribes to AAPL, MSFT")
    print("  - faang-all: subscribes to all instruments")

    await streamer.start(realtime=True)
    await asyncio.sleep(2)
    await streamer.stop()

    # Analyze results
    tech_instruments = set(t.instrument for t in tech_ticks)
    faang_instruments = set(t.instrument for t in faang_ticks)

    print(f"\nResults:")
    print(f"  tech-only received: {len(tech_ticks):,} ticks")
    print(f"    Instruments: {tech_instruments}")
    print(f"  faang-all received: {len(faang_ticks):,} ticks")
    print(f"    Instruments: {faang_instruments}")


async def price_tracking_demo():
    """Demo showing price movement tracking."""
    print("\n" + "=" * 60)
    print("Market Data Simulator - Price Tracking Demo")
    print("=" * 60)

    config = SimulatorConfig(
        instruments=("AAPL",),
        tick_rate_per_instrument=100,
        volatility=0.30,  # Higher volatility for visible movement
        enable_monitoring=False,
    )

    prices = []
    timestamps = []

    async def track_handler(tick: MarketTick) -> None:
        prices.append(tick.price)
        timestamps.append(tick.timestamp)

    subscriber = Subscriber("tracker", handler=track_handler)
    streamer = MarketDataStreamer(config, seed=42)
    streamer.add_subscriber(subscriber)

    print("\nTracking AAPL price for 3 seconds...")

    await streamer.start(realtime=True)
    await asyncio.sleep(3)
    await streamer.stop()

    # Calculate statistics
    import numpy as np

    prices_arr = np.array(prices)
    returns = np.diff(np.log(prices_arr))

    print(f"\nPrice Statistics:")
    print(f"  Ticks collected: {len(prices):,}")
    print(f"  Starting price: ${prices[0]:.2f}")
    print(f"  Ending price: ${prices[-1]:.2f}")
    print(f"  Min price: ${np.min(prices_arr):.2f}")
    print(f"  Max price: ${np.max(prices_arr):.2f}")
    print(f"  Price change: {((prices[-1]/prices[0])-1)*100:+.2f}%")
    print(f"\nReturn Statistics:")
    print(f"  Mean return: {np.mean(returns)*100:.4f}%")
    print(f"  Volatility (std): {np.std(returns)*100:.4f}%")


async def main():
    """Run all demos."""
    await simple_demo()
    await filtered_subscribers_demo()
    await price_tracking_demo()

    print("\n" + "=" * 60)
    print("All demos completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
