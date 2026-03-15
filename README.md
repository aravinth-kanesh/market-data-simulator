# Real-Time Market Data Simulator

A high-performance, production-quality market data simulator designed for systematic trading systems. Built with Python's asyncio for concurrent streaming to multiple subscribers with sub-millisecond latency.

## Features

- **Realistic Price Movements**: Geometric Brownian Motion (GBM) for mathematically accurate price simulation
- **High Throughput**: 100,000+ messages/second with proper tuning
- **Low Latency**: Sub-millisecond p99 latency for local subscribers
- **Concurrent Streaming**: Support for 10+ concurrent subscribers with per-subscriber queues
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
# Clone and setup
cd market-data-simulator

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install in development mode
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
    # Configure
    config = SimulatorConfig(
        instruments=("AAPL", "GOOGL", "MSFT"),
        tick_rate_per_instrument=100,
    )

    # Create handler
    async def on_tick(tick: MarketTick):
        print(f"{tick.instrument}: ${tick.price:.2f}")

    # Setup
    subscriber = Subscriber("my-sub", handler=on_tick)
    streamer = MarketDataStreamer(config)
    streamer.add_subscriber(subscriber)

    # Run
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

### Why Asyncio Instead of Threading?

1. **No GIL Contention**: Threading in Python suffers from the Global Interpreter Lock. Asyncio provides true concurrency for I/O-bound operations.

2. **Simpler Mental Model**: Cooperative multitasking is easier to reason about than preemptive threading. No locks needed.

3. **Better Scalability**: Asyncio can handle thousands of concurrent connections with minimal overhead (one task per subscriber, not one thread).

4. **Lower Latency**: No context switching overhead between threads. Event loop scheduling is more predictable.

### Why Geometric Brownian Motion?

GBM is the standard model for stock price simulation because:
- Prices stay positive (unlike simple random walk)
- Returns are log-normally distributed (matches empirical observations)
- Mathematically tractable (basis for Black-Scholes)
- Configurable drift and volatility

Formula: `dS = μSdt + σSdW`

### Why Per-Subscriber Queues?

1. **Isolation**: Slow subscribers don't block fast ones
2. **Backpressure**: Each subscriber handles congestion independently
3. **Filtering**: Efficient - non-matching instruments never enter queue
4. **Memory Bounded**: Fixed queue size prevents unbounded growth

### Performance Optimisations

1. **Slots on dataclasses**: ~40% memory reduction
2. **NumPy for batch operations**: 10x faster than pure Python
3. **Deque for sliding window**: O(1) append/popleft
4. **Pre-calculated constants**: Avoid repeated computation in hot path
5. **Monotonic clock for timing**: More accurate than wall clock

## Performance Characteristics

Typical results on Apple M1:

| Metric | Value |
|--------|-------|
| Raw Generator Throughput (batch) | 1,400,000+ ticks/s |
| End-to-End Throughput (1 subscriber) | 366,000+ msg/s |
| End-to-End Throughput (10 subscribers) | 127,000+ msg/s |
| Latency p50 | 45–84 μs |
| Latency p99 | 167–283 μs |
| Latency p99.9 | 327–853 μs |
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

## Production Considerations

For production deployment, consider:

1. **Persistence**: Add write-ahead log for recovery
2. **Network Transport**: Replace queues with ZeroMQ/nanomsg
3. **Serialisation**: Use Protocol Buffers or FlatBuffers
4. **Monitoring**: Export to Prometheus/Grafana
5. **High Availability**: Add leader election for failover

## License

MIT
