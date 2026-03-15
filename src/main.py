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

from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table
from rich import box

from .config import SimulatorConfig, DEFAULT_INSTRUMENTS, DEFAULT_INITIAL_PRICES
from .generator import MarketDataGenerator, MarketTick
from .streamer import MarketDataStreamer
from .subscriber import Subscriber

console = Console()


def setup_logging(level: str) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    handler = RichHandler(
        console=console,
        show_time=True,
        show_path=False,
        rich_tracebacks=True,
        markup=True,
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    root_logger.addHandler(handler)

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

    config_table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    config_table.add_column("Key", style="bold cyan")
    config_table.add_column("Value", style="white")
    config_table.add_row("Instruments", ", ".join(config.instruments))
    config_table.add_row("Tick Rate", f"{config.tick_rate_per_instrument:,}/s per instrument")
    config_table.add_row("Total Target Rate", f"{config.total_tick_rate:,}/s")
    config_table.add_row("Subscribers", str(config.num_subscribers))
    config_table.add_row("Mode", "[yellow]Benchmark (max speed)[/yellow]" if benchmark else "[green]Real-time[/green]")
    if duration > 0:
        config_table.add_row("Duration", f"{duration}s")
    console.print(Panel(config_table, title="[bold green]Market Data Simulator[/bold green]", border_style="green"))

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
    summary_table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    summary_table.add_column("Metric", style="bold cyan")
    summary_table.add_column("Value", style="bold white")
    summary_table.add_row("Total Published", f"{streamer.total_published:,} ticks")
    summary_table.add_row(
        "Total Dropped",
        "[green]0[/green]" if streamer.total_dropped == 0 else f"[red]{streamer.total_dropped:,}[/red]",
    )
    summary_table.add_row("Average Throughput", f"{streamer.throughput:,.0f} msg/s")
    console.print(Panel(summary_table, title="[bold green]Final Summary[/bold green]", border_style="green"))

    sub_table = Table(box=box.SIMPLE_HEAVY, padding=(0, 2))
    sub_table.add_column("Subscriber", style="bold cyan")
    sub_table.add_column("Received", justify="right", style="white")
    sub_table.add_column("Dropped", justify="right")
    sub_table.add_column("Gaps", justify="right")
    for sub in subscribers:
        stats = sub.stats
        dropped = "[green]0[/green]" if stats.messages_dropped == 0 else f"[red]{stats.messages_dropped:,}[/red]"
        gaps = "[green]0[/green]" if stats.gaps_detected == 0 else f"[red]{stats.gaps_detected}[/red]"
        sub_table.add_row(sub.id, f"{stats.messages_received:,}", dropped, gaps)
    console.print(Panel(sub_table, title="[bold cyan]Subscriber Stats[/bold cyan]", border_style="cyan"))


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
