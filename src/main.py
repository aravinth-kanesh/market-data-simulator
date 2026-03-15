"""
Command-line interface for the market data simulator.

Provides a CLI to start, configure, and control the simulator with
support for graceful shutdown and performance monitoring.

Usage:
    # Start with defaults
    python -m src.main

    # Custom configuration
    python -m src.main --tick-rate 500 --subscribers 20 --instruments AAPL,GOOGL,MSFT

    # Benchmark mode (no rate limiting)
    python -m src.main --benchmark --duration 10

    # Verbose logging
    python -m src.main --log-level DEBUG
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from typing import NoReturn

from .config import SimulatorConfig, DEFAULT_INSTRUMENTS, DEFAULT_INITIAL_PRICES
from .generator import MarketDataGenerator, MarketTick
from .streamer import MarketDataStreamer
from .subscriber import Subscriber


def setup_logging(level: str) -> None:
    """
    Configure logging for the application.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR).
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # Custom format for trading systems - includes microsecond precision
    formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    root_logger.addHandler(handler)

    # Reduce noise from other libraries
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="High-performance real-time market data simulator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Instrument configuration
    parser.add_argument(
        "--instruments",
        type=str,
        default=",".join(DEFAULT_INSTRUMENTS),
        help="Comma-separated list of instrument symbols",
    )

    # Performance configuration
    parser.add_argument(
        "--tick-rate",
        type=int,
        default=100,
        help="Ticks per second per instrument",
    )
    parser.add_argument(
        "--subscribers",
        type=int,
        default=10,
        help="Number of simulated subscribers",
    )
    parser.add_argument(
        "--queue-size",
        type=int,
        default=10_000,
        help="Queue size per subscriber",
    )
    parser.add_argument(
        "--backpressure",
        choices=["drop", "block"],
        default="drop",
        help="Backpressure policy when queues are full",
    )

    # Market simulation
    parser.add_argument(
        "--volatility",
        type=float,
        default=0.20,
        help="Annualised volatility (0.0-1.0)",
    )
    parser.add_argument(
        "--spread-bps",
        type=float,
        default=5.0,
        help="Bid-ask spread in basis points",
    )

    # Monitoring
    parser.add_argument(
        "--monitor-interval",
        type=float,
        default=5.0,
        help="Seconds between performance reports",
    )
    parser.add_argument(
        "--no-monitor",
        action="store_true",
        help="Disable performance monitoring",
    )

    # Execution mode
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Run in benchmark mode (no rate limiting)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=0,
        help="Run duration in seconds (0 = run until interrupted)",
    )

    # General
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging verbosity",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility",
    )

    return parser.parse_args()


def create_config(args: argparse.Namespace) -> SimulatorConfig:
    """Create configuration from command-line arguments."""
    instruments = tuple(s.strip().upper() for s in args.instruments.split(","))

    # Build initial prices for custom instruments
    initial_prices = {}
    for symbol in instruments:
        if symbol in DEFAULT_INITIAL_PRICES:
            initial_prices[symbol] = DEFAULT_INITIAL_PRICES[symbol]
        else:
            # Default price for unknown symbols
            initial_prices[symbol] = 100.0

    return SimulatorConfig(
        instruments=instruments,
        initial_prices=initial_prices,
        tick_rate_per_instrument=args.tick_rate,
        num_subscribers=args.subscribers,
        queue_size=args.queue_size,
        backpressure_policy=args.backpressure,
        volatility=args.volatility,
        spread_bps=args.spread_bps,
        monitoring_interval_seconds=args.monitor_interval,
        enable_monitoring=not args.no_monitor,
        log_level=args.log_level,
    )


async def create_demo_subscriber(
    subscriber_id: str,
    config: SimulatorConfig,
) -> Subscriber:
    """
    Create a demo subscriber that logs received ticks.

    In a real system, this would be replaced with actual market data consumers.
    """
    received_count = 0
    last_log_time = time.time()
    log_interval = 10.0  # Log every 10 seconds

    async def handler(tick: MarketTick) -> None:
        nonlocal received_count, last_log_time
        received_count += 1

        # Periodic logging to avoid spam
        now = time.time()
        if now - last_log_time >= log_interval:
            logging.getLogger(__name__).debug(
                f"Subscriber {subscriber_id}: received {received_count:,} ticks, "
                f"last: {tick.instrument} @ {tick.price:.2f}"
            )
            last_log_time = now

    return Subscriber(
        subscriber_id=subscriber_id,
        queue_size=config.queue_size,
        handler=handler,
        backpressure_policy=config.backpressure_policy,
    )


async def run_simulator(
    config: SimulatorConfig,
    benchmark: bool,
    duration: float,
    seed: int | None,
) -> None:
    """
    Run the market data simulator.

    Args:
        config: Simulator configuration.
        benchmark: If True, run without rate limiting.
        duration: Run duration in seconds (0 = run until interrupted).
        seed: Random seed for reproducibility.
    """
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("Market Data Simulator Starting")
    logger.info("=" * 60)
    logger.info(f"Instruments: {', '.join(config.instruments)}")
    logger.info(f"Tick rate: {config.tick_rate_per_instrument}/s per instrument")
    logger.info(f"Total target rate: {config.total_tick_rate:,}/s")
    logger.info(f"Subscribers: {config.num_subscribers}")
    logger.info(f"Mode: {'Benchmark (max speed)' if benchmark else 'Real-time'}")
    if duration > 0:
        logger.info(f"Duration: {duration}s")
    logger.info("=" * 60)

    # Create streamer
    streamer = MarketDataStreamer(config, seed=seed)

    # Create demo subscribers
    subscribers = []
    for i in range(config.num_subscribers):
        sub = await create_demo_subscriber(f"sub-{i:03d}", config)
        subscribers.append(sub)
        streamer.add_subscriber(sub)

    # Run
    try:
        if duration > 0:
            # Run for fixed duration
            await streamer.start(realtime=not benchmark)
            await asyncio.sleep(duration)
            await streamer.stop()
        else:
            # Run until interrupted
            await streamer.run_until_shutdown(realtime=not benchmark)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        await streamer.stop()

    # Final summary
    logger.info("=" * 60)
    logger.info("Final Summary")
    logger.info("=" * 60)
    logger.info(f"Total published: {streamer.total_published:,}")
    logger.info(f"Total dropped: {streamer.total_dropped:,}")
    logger.info(f"Average throughput: {streamer.throughput:,.0f} msg/s")

    # Subscriber summaries
    for sub in subscribers:
        stats = sub.stats
        logger.info(
            f"{sub.id}: received={stats.messages_received:,}, "
            f"dropped={stats.messages_dropped:,}, "
            f"gaps={stats.gaps_detected}"
        )

    logger.info("=" * 60)


def main() -> NoReturn:
    """Main entry point."""
    args = parse_args()
    setup_logging(args.log_level)

    try:
        config = create_config(args)
    except ValueError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        asyncio.run(
            run_simulator(
                config=config,
                benchmark=args.benchmark,
                duration=args.duration,
                seed=args.seed,
            )
        )
    except KeyboardInterrupt:
        pass

    sys.exit(0)


if __name__ == "__main__":
    main()
