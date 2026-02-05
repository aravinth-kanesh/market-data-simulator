#!/usr/bin/env python3
"""
Performance benchmarking script for the market data simulator.

Runs various benchmarks to measure:
- Maximum throughput (messages/second)
- Latency distribution (p50, p95, p99, p99.9)
- Scalability with number of subscribers
- Memory efficiency
- Generator performance

Usage:
    python benchmarks/benchmark.py                    # Run all benchmarks
    python benchmarks/benchmark.py --quick            # Quick benchmark
    python benchmarks/benchmark.py --throughput-only  # Only throughput test
    python benchmarks/benchmark.py --latency-only     # Only latency test
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import SimulatorConfig
from src.generator import MarketDataGenerator, MarketTick
from src.streamer import MarketDataStreamer
from src.subscriber import Subscriber
from src.monitor import PerformanceMonitor


@dataclass
class BenchmarkResult:
    """Results from a benchmark run."""

    name: str
    messages_per_second: float
    latency_p50_us: float
    latency_p95_us: float
    latency_p99_us: float
    latency_p999_us: float
    total_messages: int
    dropped_messages: int
    duration_seconds: float
    subscribers: int

    def __str__(self) -> str:
        drop_rate = (
            (self.dropped_messages / (self.total_messages + self.dropped_messages) * 100)
            if self.total_messages + self.dropped_messages > 0
            else 0
        )
        return (
            f"\n{'='*60}\n"
            f"Benchmark: {self.name}\n"
            f"{'='*60}\n"
            f"Throughput:     {self.messages_per_second:>12,.0f} msg/s\n"
            f"Total Messages: {self.total_messages:>12,}\n"
            f"Dropped:        {self.dropped_messages:>12,} ({drop_rate:.2f}%)\n"
            f"Duration:       {self.duration_seconds:>12.2f} seconds\n"
            f"Subscribers:    {self.subscribers:>12}\n"
            f"\nLatency Distribution:\n"
            f"  p50:          {self.latency_p50_us:>12.1f} μs\n"
            f"  p95:          {self.latency_p95_us:>12.1f} μs\n"
            f"  p99:          {self.latency_p99_us:>12.1f} μs\n"
            f"  p99.9:        {self.latency_p999_us:>12.1f} μs\n"
            f"{'='*60}"
        )


def benchmark_generator_raw(iterations: int = 100_000) -> None:
    """Benchmark raw generator performance without streaming."""
    print(f"\n📊 Generator Benchmark ({iterations:,} iterations)")
    print("-" * 40)

    config = SimulatorConfig(
        instruments=("AAPL", "GOOGL", "MSFT", "AMZN", "META"),
        tick_rate_per_instrument=1000,
    )
    generator = MarketDataGenerator(config, seed=42)

    # Warm up
    for _ in range(1000):
        generator.next_tick()

    # Single tick benchmark
    gc.disable()
    start = time.perf_counter()
    for _ in range(iterations):
        generator.next_tick()
    elapsed = time.perf_counter() - start
    gc.enable()

    rate = iterations / elapsed
    ns_per_tick = (elapsed / iterations) * 1e9

    print(f"Single tick generation:")
    print(f"  Rate:         {rate:>12,.0f} ticks/s")
    print(f"  Time/tick:    {ns_per_tick:>12.0f} ns")

    # Batch benchmark
    batch_sizes = [100, 1000, 10000]
    for batch_size in batch_sizes:
        batches = iterations // batch_size

        gc.disable()
        start = time.perf_counter()
        for _ in range(batches):
            generator.generate_batch(batch_size)
        elapsed = time.perf_counter() - start
        gc.enable()

        total = batches * batch_size
        rate = total / elapsed
        print(f"Batch size {batch_size:>5}: {rate:>12,.0f} ticks/s")


async def benchmark_throughput(
    duration: float = 5.0,
    num_subscribers: int = 10,
) -> BenchmarkResult:
    """Benchmark maximum throughput."""
    print(f"\n📊 Throughput Benchmark (duration={duration}s, subscribers={num_subscribers})")
    print("-" * 40)

    config = SimulatorConfig(
        instruments=("AAPL", "GOOGL", "MSFT", "AMZN", "META"),
        tick_rate_per_instrument=10000,  # High rate
        num_subscribers=num_subscribers,
        queue_size=100_000,
        backpressure_policy="drop",
        enable_monitoring=True,
    )

    # Track latencies
    latencies: list[float] = []
    lock = asyncio.Lock()

    async def handler(tick: MarketTick) -> None:
        latency_us = (time.time() - tick.timestamp) * 1_000_000
        async with lock:
            if len(latencies) < 100_000:  # Limit memory
                latencies.append(latency_us)

    # Create subscribers
    subscribers = [
        Subscriber(f"sub-{i}", handler=handler, queue_size=config.queue_size)
        for i in range(num_subscribers)
    ]

    streamer = MarketDataStreamer(config, seed=42)
    for sub in subscribers:
        streamer.add_subscriber(sub)

    # Run benchmark
    start = time.time()
    await streamer.start(realtime=False)  # Max speed
    await asyncio.sleep(duration)
    await streamer.stop()
    elapsed = time.time() - start

    # Calculate results
    total_received = sum(s.stats.messages_received for s in subscribers)
    total_dropped = sum(s.stats.messages_dropped for s in subscribers)

    if latencies:
        lat_array = np.array(latencies)
        p50 = float(np.percentile(lat_array, 50))
        p95 = float(np.percentile(lat_array, 95))
        p99 = float(np.percentile(lat_array, 99))
        p999 = float(np.percentile(lat_array, 99.9))
    else:
        p50 = p95 = p99 = p999 = 0.0

    result = BenchmarkResult(
        name="Maximum Throughput",
        messages_per_second=streamer.total_published / elapsed,
        latency_p50_us=p50,
        latency_p95_us=p95,
        latency_p99_us=p99,
        latency_p999_us=p999,
        total_messages=streamer.total_published,
        dropped_messages=total_dropped,
        duration_seconds=elapsed,
        subscribers=num_subscribers,
    )

    print(result)
    return result


async def benchmark_latency(
    duration: float = 5.0,
    tick_rate: int = 100,
) -> BenchmarkResult:
    """Benchmark latency at sustainable rate."""
    print(f"\n📊 Latency Benchmark (duration={duration}s, rate={tick_rate}/s/instrument)")
    print("-" * 40)

    config = SimulatorConfig(
        instruments=("AAPL", "GOOGL", "MSFT"),
        tick_rate_per_instrument=tick_rate,
        num_subscribers=1,
        queue_size=10_000,
        enable_monitoring=True,
    )

    latencies: list[float] = []

    async def handler(tick: MarketTick) -> None:
        latency_us = (time.time() - tick.timestamp) * 1_000_000
        latencies.append(latency_us)

    subscriber = Subscriber("latency-test", handler=handler)
    streamer = MarketDataStreamer(config, seed=42)
    streamer.add_subscriber(subscriber)

    # Run benchmark
    start = time.time()
    await streamer.start(realtime=True)
    await asyncio.sleep(duration)
    await streamer.stop()
    elapsed = time.time() - start

    # Calculate latency percentiles
    if latencies:
        lat_array = np.array(latencies)
        p50 = float(np.percentile(lat_array, 50))
        p95 = float(np.percentile(lat_array, 95))
        p99 = float(np.percentile(lat_array, 99))
        p999 = float(np.percentile(lat_array, 99.9))
    else:
        p50 = p95 = p99 = p999 = 0.0

    result = BenchmarkResult(
        name=f"Latency @ {tick_rate}/s/instrument",
        messages_per_second=streamer.total_published / elapsed,
        latency_p50_us=p50,
        latency_p95_us=p95,
        latency_p99_us=p99,
        latency_p999_us=p999,
        total_messages=streamer.total_published,
        dropped_messages=subscriber.stats.messages_dropped,
        duration_seconds=elapsed,
        subscribers=1,
    )

    print(result)
    return result


async def benchmark_scalability(
    duration: float = 3.0,
    max_subscribers: int = 50,
) -> list[BenchmarkResult]:
    """Benchmark how throughput scales with subscriber count."""
    print(f"\n📊 Scalability Benchmark")
    print("-" * 40)

    results = []
    subscriber_counts = [1, 5, 10, 20, 50]
    subscriber_counts = [c for c in subscriber_counts if c <= max_subscribers]

    for num_subs in subscriber_counts:
        config = SimulatorConfig(
            instruments=("AAPL", "GOOGL", "MSFT"),
            tick_rate_per_instrument=1000,
            num_subscribers=num_subs,
            queue_size=50_000,
            backpressure_policy="drop",
            enable_monitoring=False,
        )

        subscribers = [
            Subscriber(f"sub-{i}", queue_size=config.queue_size)
            for i in range(num_subs)
        ]

        streamer = MarketDataStreamer(config, seed=42)
        for sub in subscribers:
            streamer.add_subscriber(sub)

        start = time.time()
        await streamer.start(realtime=False)
        await asyncio.sleep(duration)
        await streamer.stop()
        elapsed = time.time() - start

        rate = streamer.total_published / elapsed
        total_dropped = sum(s.stats.messages_dropped for s in subscribers)

        result = BenchmarkResult(
            name=f"Scalability ({num_subs} subscribers)",
            messages_per_second=rate,
            latency_p50_us=0,
            latency_p95_us=0,
            latency_p99_us=0,
            latency_p999_us=0,
            total_messages=streamer.total_published,
            dropped_messages=total_dropped,
            duration_seconds=elapsed,
            subscribers=num_subs,
        )
        results.append(result)

        print(f"  {num_subs:>3} subscribers: {rate:>12,.0f} msg/s")

    return results


def print_summary(results: list[BenchmarkResult]) -> None:
    """Print benchmark summary."""
    print("\n" + "=" * 60)
    print("BENCHMARK SUMMARY")
    print("=" * 60)

    print(f"\n{'Benchmark':<40} {'Throughput':>15} {'p99 Latency':>12}")
    print("-" * 70)

    for r in results:
        print(f"{r.name:<40} {r.messages_per_second:>12,.0f}/s {r.latency_p99_us:>10.1f}μs")

    # Performance assessment
    print("\n" + "-" * 70)
    throughput_results = [r for r in results if "Throughput" in r.name]
    latency_results = [r for r in results if "Latency" in r.name]

    if throughput_results:
        max_throughput = max(r.messages_per_second for r in throughput_results)
        if max_throughput >= 100_000:
            print(f"✅ Throughput target (100k msg/s): PASSED ({max_throughput:,.0f} msg/s)")
        else:
            print(f"❌ Throughput target (100k msg/s): FAILED ({max_throughput:,.0f} msg/s)")

    if latency_results:
        p99_latency = min(r.latency_p99_us for r in latency_results)
        if p99_latency < 1000:  # 1ms
            print(f"✅ Latency target (p99 < 1ms): PASSED ({p99_latency:.1f}μs)")
        else:
            print(f"❌ Latency target (p99 < 1ms): FAILED ({p99_latency:.1f}μs)")

    print("=" * 60)


async def run_all_benchmarks(quick: bool = False) -> None:
    """Run all benchmarks."""
    print("\n🚀 Market Data Simulator Benchmarks")
    print("=" * 60)

    duration = 2.0 if quick else 5.0
    results = []

    # Generator benchmark (sync)
    benchmark_generator_raw(iterations=10_000 if quick else 100_000)

    # Throughput benchmark
    result = await benchmark_throughput(duration=duration, num_subscribers=10)
    results.append(result)

    # Latency benchmarks at different rates
    for rate in [100, 500, 1000]:
        result = await benchmark_latency(duration=duration, tick_rate=rate)
        results.append(result)

    # Scalability benchmark
    if not quick:
        scale_results = await benchmark_scalability(
            duration=2.0,
            max_subscribers=50,
        )
        results.extend(scale_results)

    # Print summary
    print_summary(results)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Benchmark market data simulator")
    parser.add_argument("--quick", action="store_true", help="Quick benchmark (shorter duration)")
    parser.add_argument("--throughput-only", action="store_true", help="Only run throughput test")
    parser.add_argument("--latency-only", action="store_true", help="Only run latency test")
    parser.add_argument("--generator-only", action="store_true", help="Only run generator test")
    args = parser.parse_args()

    if args.generator_only:
        benchmark_generator_raw()
        return

    if args.throughput_only:
        asyncio.run(benchmark_throughput())
        return

    if args.latency_only:
        asyncio.run(benchmark_latency())
        return

    asyncio.run(run_all_benchmarks(quick=args.quick))


if __name__ == "__main__":
    main()
