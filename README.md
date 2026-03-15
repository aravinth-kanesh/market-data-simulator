# Real-Time Market Data Simulator

A high-performance market data simulator for systematic trading workloads. Uses Python asyncio to stream tick data concurrently to multiple subscribers with sub-millisecond latency.

## Features

- **Realistic Price Movements**: Geometric Brownian Motion (GBM) for mathematically accurate price simulation
- **High Throughput**: 1.4M+ ticks/second raw generation; 127k+ msg/s end-to-end across 10 subscribers
- **Low Latency**: Sub-200µs p99 latency under sustained load
- **Concurrent Streaming**: Per-subscriber queue isolation supporting 10+ concurrent consumers
- **Backpressure Handling**: Configurable drop or block policies for slow consumers
- **Comprehensive Monitoring**: Real-time latency percentiles (p50, p95, p99, p99.9) and throughput metrics
- **Instrument Filtering**: Subscribers can subscribe to specific instruments
- **Graceful Shutdown**: Clean handling of SIGINT/SIGTERM signals

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Main Event Loop                          │
│                         (asyncio)                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐    ┌──────────────────────────────────┐   │
│  │                 │    │         MarketDataStreamer        │   │
│  │ MarketDataGen   │───▶│  ┌─────────────────────────────┐ │   │
│  │                 │    │  │      Publish/Fan-out         │ │   │
│  │ ┌─────────────┐ │    │  └─────────────────────────────┘ │   │
│  │ │ AAPL State  │ │    │              │                    │   │
│  │ ├─────────────┤ │    │    ┌─────────┴─────────┐         │   │
│  │ │ GOOGL State │ │    │    ▼         ▼         ▼         │   │
│  │ ├─────────────┤ │    │ ┌─────┐  ┌─────┐  ┌─────┐       │   │
│  │ │ MSFT State  │ │    │ │Queue│  │Queue│  │Queue│       │   │
│  │ └─────────────┘ │    │ └──┬──┘  └──┬──┘  └──┬──┘       │   │
│  └─────────────────┘    │    │        │        │          │   │
│                         │    ▼        ▼        ▼          │   │
│                         │  Sub-1    Sub-2    Sub-N        │   │
│                         └──────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              PerformanceMonitor                          │   │
│  │  - Latency tracking (sliding window)                     │   │
│  │  - Throughput calculation                                │   │
│  │  - Drop rate monitoring                                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
market-data-simulator/
├── src/
│   ├── __init__.py        # Package exports
│   ├── config.py          # Configuration management
│   ├── generator.py       # Market data generation (GBM)
│   ├── streamer.py        # Asyncio streaming infrastructure
│   ├── subscriber.py      # Subscriber implementation
│   ├── monitor.py         # Performance monitoring
│   └── main.py            # CLI entry point
├── tests/
│   ├── test_generator.py  # Generator unit tests
│   ├── test_streamer.py   # Streamer & subscriber tests
│   └── test_integration.py # End-to-end tests
├── benchmarks/
│   └── benchmark.py       # Performance benchmarking
├── examples/
│   └── demo.py            # Usage examples
├── requirements.txt       # Dependencies
├── setup.py              # Package setup
└── pyproject.toml        # Tool configuration
```

## Quick Start

### Installation

```bash
cd market-data-simulator

python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

pip install -r requirements.txt
pip install -e .
```

### Basic Usage

```bash
# Run with defaults (10 instruments, 100 ticks/s/instrument, 10 subscribers)
python -m src.main

# Custom configuration
python -m src.main --instruments AAPL,GOOGL,MSFT --tick-rate 500 --subscribers 20

# Benchmark mode (no rate limiting)
python -m src.main --benchmark --duration 10

# See all options
python -m src.main --help
```

### Programmatic Usage

```python
import asyncio
from src.config import SimulatorConfig
from src.generator import MarketTick
from src.streamer import MarketDataStreamer
from src.subscriber import Subscriber

async def main():
    config = SimulatorConfig(
        instruments=("AAPL", "GOOGL", "MSFT"),
        tick_rate_per_instrument=100,
    )

    async def on_tick(tick: MarketTick):
        print(f"{tick.instrument}: ${tick.price:.2f}")

    subscriber = Subscriber("my-sub", handler=on_tick)
    streamer = MarketDataStreamer(config)
    streamer.add_subscriber(subscriber)

    await streamer.start()
    await asyncio.sleep(5)
    await streamer.stop()

asyncio.run(main())
```

## Running Tests

```bash
# All tests
pytest

# With coverage
pytest --cov=src --cov-report=term-missing

# Specific test file
pytest tests/test_generator.py -v
```

## Running Benchmarks

```bash
# Full benchmark suite
python benchmarks/benchmark.py

# Quick benchmark
python benchmarks/benchmark.py --quick

# Specific benchmarks
python benchmarks/benchmark.py --throughput-only
python benchmarks/benchmark.py --latency-only
python benchmarks/benchmark.py --generator-only
```

## Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `instruments` | 10 stocks | Tuple of instrument symbols |
| `tick_rate_per_instrument` | 100 | Ticks per second per instrument |
| `num_subscribers` | 10 | Number of subscribers |
| `queue_size` | 10,000 | Queue size per subscriber |
| `backpressure_policy` | "drop" | "drop" or "block" when queue full |
| `volatility` | 0.20 | Annualised volatility (20%) |
| `spread_bps` | 5.0 | Bid-ask spread in basis points |
| `monitoring_interval_seconds` | 5.0 | Metrics reporting interval |

## Design Decisions

### Why asyncio instead of threading?

Python threads contend on the GIL, which limits parallelism for CPU-bound work. asyncio uses cooperative multitasking on a single thread, which avoids locking overhead entirely and scales to thousands of concurrent subscribers with one task each rather than one thread. The event loop's scheduling is also more predictable, which matters for latency consistency.

### Why Geometric Brownian Motion?

GBM is the standard model for equity price simulation. Prices stay positive, returns are log-normally distributed (consistent with empirical data), and drift and volatility are independently configurable. It is also the basis for Black-Scholes, so the model is familiar to anyone in the quant space.

Formula: `dS = μSdt + σSdW`

### Why per-subscriber queues?

A single shared queue would mean a slow subscriber blocks or drops messages for all others. Per-subscriber queues provide isolation: each consumer handles its own backpressure independently, non-matching instruments never enter a queue, and queue size is bounded to prevent unbounded memory growth.

### Performance optimisations

- Slots on dataclasses: ~40% memory reduction per tick
- NumPy batch generation: 10x faster than pure Python loops
- Deque for sliding window: O(1) append and popleft
- Pre-calculated constants: avoid repeated computation on the hot path
- Monotonic clock for timing: more accurate than wall clock

## Performance Characteristics

Typical results on Apple M1:

| Metric | Value |
|--------|-------|
| Raw Generator Throughput (batch) | 1,400,000+ ticks/s |
| End-to-End Throughput (1 subscriber) | 366,000+ msg/s |
| End-to-End Throughput (10 subscribers) | 127,000+ msg/s |
| Latency p50 | 45-84 µs |
| Latency p99 | 167-283 µs |
| Latency p99.9 | 327-853 µs |
| Memory (10 subscribers) | ~50 MB |

## Market Data Format

Each tick contains:

```python
@dataclass
class MarketTick:
    instrument: str    # Symbol (e.g., "AAPL")
    timestamp: float   # Unix timestamp (microsecond precision)
    price: float       # Last trade price
    volume: int        # Trade volume
    bid: float         # Best bid price
    ask: float         # Best ask price
    sequence: int      # Monotonic sequence number
```

## Monitoring Metrics

The system tracks:
- **Latency**: End-to-end time from generation to subscriber receipt
- **Throughput**: Messages per second (windowed and cumulative)
- **Drop Rate**: Percentage of messages dropped due to backpressure
- **Queue Depth**: Current queue size per subscriber
- **Sequence Gaps**: Detect missed messages
