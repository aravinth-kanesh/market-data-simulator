# Interview Preparation Guide

## Market Data Simulator - Technical Deep Dive

This document provides comprehensive preparation for technical interviews about this project. It covers design decisions, trade-offs, performance optimisations, and anticipated questions.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture Deep Dive](#architecture-deep-dive)
3. [Design Decisions & Trade-offs](#design-decisions--trade-offs)
4. [Performance Optimisations](#performance-optimisations)
5. [Interview Questions & Answers](#interview-questions--answers)
6. [What I Would Do Differently](#what-i-would-do-differently)
7. [Production Considerations](#production-considerations)
8. [Relevant Trading System Concepts](#relevant-trading-system-concepts)

---

## Project Overview

### What This Project Demonstrates

1. **Systems Programming**: Memory-efficient data structures, performance optimisation
2. **Concurrent Programming**: Asyncio, event loops, cooperative multitasking
3. **Financial Domain Knowledge**: GBM, bid-ask spreads, market microstructure
4. **Software Engineering**: Clean architecture, testing, documentation
5. **Performance Engineering**: Latency measurement, throughput optimisation, profiling

### Key Metrics Achieved

| Metric | Target | Achieved |
|--------|--------|----------|
| Generator throughput | 100k/s | **1.3M/s** (batch) |
| E2E with subscribers | 100k/s | 45k/s* |
| Latency p99 | <1ms | **95-160μs** |
| Latency p99.9 | <10ms | **150-430μs** |

*Note: E2E throughput is bounded by Python's async overhead with callbacks. Pure C++ would achieve 10x+.

---

## Architecture Deep Dive

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         SimulatorConfig                              │
│  (Immutable, validated configuration - frozen dataclass)             │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      MarketDataGenerator                             │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    InstrumentState (per symbol)              │   │
│  │  - Current price          - RNG state (numpy)                │   │
│  │  - Volatility/drift       - Sequence counter                 │   │
│  │  - GBM parameters         - generate_tick() / generate_batch()│   │
│  └─────────────────────────────────────────────────────────────┘   │
│  Round-robin scheduling across instruments                          │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ MarketTick
┌─────────────────────────────────────────────────────────────────────┐
│                      MarketDataStreamer                              │
│  - Owns generator instance                                          │
│  - Manages subscriber registry                                      │
│  - Main event loop (realtime or fast mode)                         │
│  - Publishes to all subscribers (fan-out)                          │
│  - Signal handling (SIGINT/SIGTERM)                                │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
            ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
            │ Subscriber  │ │ Subscriber  │ │ Subscriber  │
            │ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌─────────┐ │
            │ │ Queue   │ │ │ │ Queue   │ │ │ │ Queue   │ │
            │ └─────────┘ │ │ └─────────┘ │ │ └─────────┘ │
            │ - Filter    │ │ - Filter    │ │ - Filter    │
            │ - Handler   │ │ - Handler   │ │ - Handler   │
            │ - Stats     │ │ - Stats     │ │ - Stats     │
            └─────────────┘ └─────────────┘ └─────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      PerformanceMonitor                              │
│  - Sliding window of latencies (deque)                              │
│  - Percentile calculation (numpy)                                   │
│  - Throughput tracking                                              │
│  - Drop rate monitoring                                             │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Generation**: `InstrumentState.generate_tick()` applies GBM formula
2. **Scheduling**: `MarketDataGenerator.next_tick()` round-robins across instruments
3. **Publishing**: `MarketDataStreamer.publish()` iterates subscribers
4. **Filtering**: Each subscriber checks `wants_instrument()`
5. **Queuing**: `Subscriber.deliver()` puts tick in asyncio.Queue
6. **Processing**: Subscriber's handler callback processes tick
7. **Monitoring**: `PerformanceMonitor.record_publish()` tracks latency

### Memory Layout

```
MarketTick (slots=True):
┌────────────────┬────────────────┬────────────────┐
│ instrument     │ timestamp      │ price          │
│ (str ref)      │ (float, 8B)    │ (float, 8B)    │
├────────────────┼────────────────┼────────────────┤
│ volume         │ bid            │ ask            │
│ (int, 8B)      │ (float, 8B)    │ (float, 8B)    │
├────────────────┼────────────────┴────────────────┘
│ sequence       │
│ (int, 8B)      │
└────────────────┘
Total: ~64 bytes per tick (with slots, vs ~152 bytes without)
```

---

## Design Decisions & Trade-offs

### 1. Asyncio vs Threading vs Multiprocessing

**Decision**: Asyncio (single-threaded event loop)

**Why Asyncio:**
- **No GIL contention**: Python's Global Interpreter Lock makes true parallelism impossible with threads for CPU-bound work
- **Simpler concurrency model**: Cooperative multitasking is easier to reason about than preemptive threading
- **No locks needed**: Single-threaded means no race conditions, no deadlocks
- **Lower memory**: One thread vs N threads (each with ~8MB stack)
- **Better for I/O**: Market data systems are I/O-bound (network, subscribers)

**Trade-offs:**
- ❌ Can't utilise multiple CPU cores (mitigated by ProcessPoolExecutor for batch generation)
- ❌ One slow handler blocks the event loop (mitigated by async handlers)
- ❌ CPU-bound work blocks everything (mitigated by keeping handlers fast)

**When Threading Would Be Better:**
- If subscribers do CPU-heavy processing (use ProcessPoolExecutor instead)
- If integrating with blocking C libraries

**Code Evidence:**
```python
# streamer.py - Single event loop handles all subscribers
async def _realtime_loop(self) -> None:
    while self._running:
        tick = self._generator.next_tick()
        await self.publish(tick)  # Fan-out to all subscribers
        await asyncio.sleep(tick_interval)
```

---

### 2. Geometric Brownian Motion vs Random Walk

**Decision**: GBM for price simulation

**Why GBM:**
- **Prices stay positive**: Unlike additive random walk, multiplicative GBM can't go negative
- **Log-normal returns**: Matches empirical observations of real markets
- **Industry standard**: Foundation of Black-Scholes, used everywhere in quant finance
- **Configurable parameters**: Drift (μ) and volatility (σ) have intuitive meanings

**The Math:**
```
dS = μSdt + σSdW

Discrete form:
S(t+dt) = S(t) * exp((μ - σ²/2)dt + σ√dt * Z)

Where:
- μ = drift (annual expected return)
- σ = volatility (annual standard deviation)
- Z = standard normal random variable
- dt = time step
```

**Trade-offs:**
- ❌ Assumes constant volatility (real markets have volatility clustering)
- ❌ No jumps (real markets have discontinuities)
- ❌ Symmetric returns (real markets have fat tails)

**More Realistic Alternatives:**
- Heston model (stochastic volatility)
- Jump-diffusion models
- GARCH for volatility clustering

**Code Evidence:**
```python
# generator.py - GBM implementation
def generate_tick(self) -> MarketTick:
    z = self._rng.standard_normal()
    drift_term = (self.drift - 0.5 * self.volatility**2) * self._dt
    diffusion_term = self.volatility * self._sqrt_dt * z
    self.price *= np.exp(drift_term + diffusion_term)  # Multiplicative!
```

---

### 3. Per-Subscriber Queues vs Shared Queue

**Decision**: Each subscriber has its own asyncio.Queue

**Why Per-Subscriber:**
- **Isolation**: Slow subscriber doesn't block fast ones
- **Independent backpressure**: Each subscriber handles congestion independently
- **Filtering efficiency**: Non-matching instruments never enter queue
- **Memory bounded**: Each queue has fixed max size

**Trade-offs:**
- ❌ More memory (N queues vs 1)
- ❌ More iteration (publisher loops through all subscribers)
- ❌ Potential for inconsistent state (subscriber A processes tick 100 while B is on tick 50)

**Alternative: Shared Ring Buffer:**
```
Pros: Less memory, better cache locality, natural ordering
Cons: Slow consumers block everyone, complex coordination
Used by: LMAX Disruptor, Aeron
```

**Code Evidence:**
```python
# subscriber.py - Each subscriber owns a queue
class Subscriber:
    def __init__(self, ...):
        self._queue: asyncio.Queue[MarketTick] = asyncio.Queue(maxsize=queue_size)
```

---

### 4. Drop vs Block Backpressure Policy

**Decision**: Configurable, defaulting to "drop"

**Drop Policy (Default):**
- Publisher never blocks
- Messages are discarded when queue full
- Maintains system responsiveness
- Appropriate for: Real-time systems where stale data is useless

**Block Policy:**
- Publisher waits for queue space
- No message loss
- Can cause cascading backpressure
- Appropriate for: Systems where every message must be processed

**Trade-offs:**
| Aspect | Drop | Block |
|--------|------|-------|
| Latency | Predictable | Can spike |
| Completeness | May lose data | No loss |
| Cascading failures | Isolated | Can propagate |
| Use case | Real-time | Batch/audit |

**Code Evidence:**
```python
# subscriber.py - Backpressure handling
async def deliver(self, tick: MarketTick) -> bool:
    try:
        if self._backpressure_policy == "drop":
            self._queue.put_nowait(tick)  # Raises QueueFull
        else:
            await self._queue.put(tick)   # Blocks until space
        return True
    except asyncio.QueueFull:
        self._stats.messages_dropped += 1
        return False
```

---

### 5. Frozen Dataclass for Configuration

**Decision**: `@dataclass(frozen=True, slots=True)` for SimulatorConfig

**Why Frozen:**
- **Thread-safe**: Immutable objects can be shared without locks
- **Prevents bugs**: Can't accidentally modify config during runtime
- **Hashable**: Can be used as dict key or in sets
- **Clear intent**: Documents that config shouldn't change

**Why Slots:**
- **Memory reduction**: ~40% smaller objects
- **Faster attribute access**: Direct offset vs dict lookup
- **Prevents typos**: Can't add arbitrary attributes

**Trade-offs:**
- ❌ Can't add attributes dynamically
- ❌ Can't use `__dict__` for introspection
- ❌ Slightly more complex inheritance

**Code Evidence:**
```python
# config.py
@dataclass(frozen=True, slots=True)
class SimulatorConfig:
    instruments: tuple[str, ...] = DEFAULT_INSTRUMENTS
    tick_rate_per_instrument: int = 100
    # ... validation in __post_init__
```

---

### 6. NumPy for Batch Operations

**Decision**: Use NumPy for vectorised price generation

**Why NumPy:**
- **10-100x faster**: SIMD operations, C implementation
- **Single RNG call**: Generate all random numbers at once
- **Cache-friendly**: Contiguous memory access
- **Reduced interpreter overhead**: Less Python bytecode execution

**Benchmark Comparison:**
```
Single tick (pure Python loop): 644k ticks/s
Batch generation (NumPy):       1.3M ticks/s  (2x faster)
```

**Trade-offs:**
- ❌ Memory allocation for arrays
- ❌ Not suitable for real-time single-tick generation
- ❌ Dependency on NumPy

**Code Evidence:**
```python
# generator.py - Vectorized batch generation
def generate_batch(self, count: int) -> list[MarketTick]:
    z_values = self._rng.standard_normal(count)  # One RNG call
    log_returns = drift_term + vol_term * z_values  # Vectorized
    cumulative_returns = np.cumsum(log_returns)
    prices = self.price * np.exp(cumulative_returns)  # All prices at once
```

---

## Performance Optimisations

### 1. Object Allocation Reduction

**Problem**: Python object creation is expensive (memory allocation, reference counting)

**Solutions Applied:**
- `__slots__` on all hot-path classes (MarketTick, InstrumentState, Subscriber)
- Pre-allocated deques with maxlen
- Reuse of numpy arrays where possible

**Impact**: ~40% memory reduction, faster attribute access

### 2. Efficient Data Structures

| Use Case | Structure | Why |
|----------|-----------|-----|
| Sliding window | `deque(maxlen=N)` | O(1) append/popleft, automatic eviction |
| Subscriber registry | `dict[str, Subscriber]` | O(1) lookup by ID |
| Instrument prices | `dict[str, float]` | O(1) access |
| Message queue | `asyncio.Queue` | Thread-safe, async-native |

### 3. Timing Optimisations

```python
# Use monotonic clock for interval timing (not affected by system clock changes)
next_tick_time = time.monotonic()

# Pre-calculate constants outside loop
tick_interval = 1.0 / total_rate
drift_term = (self.drift - 0.5 * self.volatility**2) * self._dt
```

### 4. Percentile Calculation

**Naive approach**: Sort entire array every time - O(n log n)

**Our approach**:
- Collect samples in deque (O(1) append)
- Calculate percentiles only on report (every N seconds)
- Use numpy.percentile (optimised C implementation)

**Alternative for production**: t-digest or HDR Histogram for streaming percentiles

---

## Interview Questions & Answers

### Architecture & Design

**Q1: Why did you choose asyncio instead of threading?**

A: Three main reasons:
1. **GIL avoidance**: Python's Global Interpreter Lock means threads can't truly run in parallel for CPU-bound work. Asyncio gives us concurrency without the GIL overhead.
2. **Simpler mental model**: Cooperative multitasking means I know exactly when context switches happen (at await points). No race conditions, no locks needed.
3. **Better fit for I/O**: Market data systems are fundamentally I/O-bound - we're pushing data to subscribers, not crunching numbers. Asyncio excels here.

The trade-off is we can't utilise multiple cores, but for this use case that's acceptable. In production, I'd use a hybrid approach - asyncio for I/O coordination, ProcessPoolExecutor for CPU-heavy work.

---

**Q2: How does your system handle backpressure?**

A: I implemented two configurable policies:

1. **Drop policy** (default): If a subscriber's queue is full, we drop the message and increment a counter. The publisher never blocks. This is appropriate for real-time systems where stale data is useless.

2. **Block policy**: The publisher waits for queue space. No data loss, but can cause cascading slowdowns.

Each subscriber has its own queue, so a slow subscriber doesn't affect fast ones. I track drop rates in the monitoring system so operators can detect overwhelmed subscribers.

---

**Q3: Why Geometric Brownian Motion for price simulation?**

A: GBM is the standard model in quantitative finance for several reasons:
1. **Prices stay positive**: It's multiplicative, not additive, so prices can't go negative
2. **Log-normal returns**: This matches empirical observations of real markets
3. **Interpretable parameters**: Drift is expected return, volatility is standard deviation
4. **Foundation of options pricing**: Black-Scholes assumes GBM

I'm aware of limitations - real markets have fat tails, volatility clustering, and jumps. For production, I might use Heston (stochastic volatility) or add jump-diffusion.

---

**Q4: What are the bottlenecks in your system?**

A: I identified three main bottlenecks through profiling:

1. **Python function call overhead**: Each tick requires multiple function calls. In benchmark mode, this limits us to ~50k msg/s with callbacks. Batch processing helps (1.3M/s).

2. **Asyncio event loop scheduling**: The event loop adds overhead for each await. I mitigate this by batching work where possible.

3. **Object allocation**: Creating MarketTick objects is expensive. I use `__slots__` to reduce memory and could use object pooling for further gains.

For a production system in C++, I'd expect 10-100x improvement.

---

**Q5: How would you scale this to handle more subscribers?**

A: Several approaches depending on the scaling dimension:

**More subscribers (100+):**
- Shard subscribers across multiple event loops (one per CPU core)
- Use multiprocessing with shared memory for the generator
- Consider a pub/sub broker (Redis, Kafka) for fan-out

**More instruments (1000+):**
- Partition instruments across generator instances
- Use consistent hashing to route subscribers to the right partition

**Geographic distribution:**
- Deploy generators in each region
- Use market data conflation to reduce bandwidth

**Higher throughput (1M+ msg/s):**
- Rewrite hot path in C++ or Rust
- Use kernel bypass networking (DPDK, io_uring)
- LMAX Disruptor pattern for lock-free queuing

---

### Performance & Latency

**Q6: How did you optimise for latency?**

A: Multiple layers of optimisation:

1. **Data structures**: `__slots__` on dataclasses, deque for O(1) operations, pre-sized queues
2. **Avoid allocations**: Reuse objects where possible, NumPy arrays for batch ops
3. **Minimize work in hot path**: Pre-calculate constants, avoid logging in publish loop
4. **Efficient timing**: Monotonic clock, sleep calculation accounts for processing time
5. **No locks**: Single-threaded asyncio means no lock contention

Result: p99 latency of 95-160μs, which is excellent for Python.

---

**Q7: How do you measure latency?**

A: I measure end-to-end latency from tick generation to subscriber receipt:

```python
# At generation
tick.timestamp = time.time()

# At receipt
latency_ns = (time.time() - tick.timestamp) * 1_000_000_000
```

I collect samples in a sliding window (deque with maxlen) and calculate percentiles using NumPy. This gives p50, p95, p99, p99.9.

**Caveats I'm aware of:**
- `time.time()` resolution is ~1μs, not nanosecond
- Clock synchronisation matters for distributed systems
- Coordinated omission can skew measurements (I avoid this by measuring in the subscriber)

---

**Q8: Your throughput benchmark shows 45k msg/s, but you targeted 100k. What happened?**

A: Good catch! The 45k is with 10 active subscribers each running callback handlers. The overhead comes from:

1. **Asyncio scheduling**: Each `await` in the handler triggers event loop
2. **Python function calls**: 10 callbacks per tick = 450k function calls/second
3. **Lock contention**: The latency tracking uses an asyncio.Lock for thread-safety in the benchmark

The raw generator achieves 1.3M ticks/s in batch mode. For production throughput at 100k+, I would:
- Remove unnecessary work from callbacks
- Use a compiled extension for the hot path
- Consider a shared ring buffer instead of per-subscriber queues

---

### Concurrency & Correctness

**Q9: Is your system thread-safe?**

A: It's designed for single-threaded asyncio use, which sidesteps most thread-safety concerns:

- **Generator**: Each InstrumentState has its own RNG, so no shared mutable state
- **Subscribers**: Each has its own queue; asyncio.Queue is coroutine-safe
- **Monitor**: Uses a sliding window that's only accessed from one task

If I needed multi-threaded access, I would:
- Use thread-safe queues (queue.Queue or janus)
- Add locks around shared state
- Consider lock-free data structures

---

**Q10: How do you handle subscriber failures?**

A: Several mechanisms:

1. **Queue isolation**: If a subscriber crashes, others continue unaffected
2. **Try/except in handlers**: Errors are logged but don't crash the system
3. **Backpressure**: Full queues trigger drops, not crashes
4. **Graceful shutdown**: SIGINT/SIGTERM handlers clean up properly

In production, I'd add:
- Dead letter queue for failed messages
- Automatic subscriber reconnection
- Health checks and circuit breakers

---

**Q11: What happens if the generator falls behind?**

A: In realtime mode, I track the target tick time and compare to actual time:

```python
sleep_time = next_tick_time - time.monotonic()
if sleep_time > 0:
    await asyncio.sleep(sleep_time)
else:
    # Falling behind - reset timing if too far behind
    if sleep_time < -0.1:  # 100ms behind
        next_tick_time = time.monotonic()
```

If we're slightly behind, we skip the sleep and catch up. If we're way behind (100ms+), we reset the timing to avoid a burst of catch-up ticks.

---

### Code Quality & Testing

**Q12: How do you ensure the price simulation is correct?**

A: Multiple levels of validation:

1. **Unit tests**: Verify prices stay positive, spreads are calculated correctly
2. **Statistical tests**: Check that log returns are approximately normal, volatility scales correctly
3. **Reproducibility**: Same seed produces identical sequences
4. **Property-based reasoning**: GBM mathematically guarantees positive prices

```python
def test_price_stays_positive(self):
    """GBM property: prices never go negative."""
    state = InstrumentState(initial_price=1.0, volatility=0.50, ...)
    for _ in range(10000):
        tick = state.generate_tick()
        assert tick.price > 0
```

---

**Q13: How would you debug a latency spike in production?**

A: Systematic approach:

1. **Check the percentiles**: Is it p99 or p99.9? How frequent?
2. **Correlate with events**: GC pauses, other processes, time of day
3. **Add tracing**: Instrument each stage (generation, publish, queue, handler)
4. **Profile**: Use py-spy or cProfile to find hot spots
5. **Check resources**: CPU, memory, file descriptors, queue depths

Common culprits in Python:
- GC pauses (disable GC during critical sections)
- Event loop starvation (long-running sync code)
- Memory pressure (object allocation in hot path)

---

### Production & Operations

**Q14: What monitoring would you add in production?**

A: I'd export metrics to Prometheus/Grafana:

**Latency metrics:**
- Histogram of end-to-end latency
- Per-subscriber latency breakdown
- Queue wait time vs processing time

**Throughput metrics:**
- Messages per second (generation, delivery, drops)
- Per-instrument rates
- Subscriber consumption rates

**Health metrics:**
- Queue depths (early warning for backpressure)
- Error rates
- Memory usage
- Event loop lag

**Alerts:**
- p99 latency > threshold
- Drop rate > threshold
- Queue depth growing
- Subscriber disconnected

---

**Q15: How would you deploy this in production?**

A: Several considerations:

**Containerization:**
- Docker container with pinned dependencies
- CPU and memory limits
- Liveness/readiness probes

**High availability:**
- Multiple instances behind load balancer
- Stateless design (no local state to sync)
- Graceful shutdown for rolling updates

**Configuration:**
- Environment variables for tuning
- Feature flags for gradual rollout
- Config validation at startup

**Observability:**
- Structured logging (JSON)
- Distributed tracing (OpenTelemetry)
- Metric export (Prometheus)

---

### Domain Knowledge

**Q16: What is bid-ask spread and why does it matter?**

A: The bid-ask spread is the difference between the highest price a buyer will pay (bid) and the lowest price a seller will accept (ask).

**Why it matters:**
- **Transaction cost**: You buy at ask, sell at bid - the spread is your immediate loss
- **Liquidity indicator**: Tight spreads = liquid market, wide spreads = illiquid
- **Market maker profit**: They earn the spread by providing liquidity

**In my simulator:**
```python
half_spread = price * (spread_bps / 10000) / 2
bid = price - half_spread
ask = price + half_spread
```

I use 5 basis points (0.05%) which is typical for liquid US equities.

---

**Q17: What is market microstructure?**

A: Market microstructure is the study of how markets operate at the detailed level:

- **Order book dynamics**: How orders arrive, match, and affect prices
- **Price discovery**: How information gets incorporated into prices
- **Transaction costs**: Spreads, market impact, slippage
- **Market making**: How liquidity providers operate
- **High-frequency trading**: Strategies at microsecond timescales

My simulator models some of this (spreads, tick-by-tick updates) but simplifies others (no order book, no market impact).

---

**Q18: What's the difference between latency and throughput?**

A: They measure different things and can conflict:

**Latency**: Time for one message to travel through the system
- Measured in microseconds/milliseconds
- Important for: Real-time trading, user experience
- Optimised by: Reducing work per message, faster algorithms

**Throughput**: Number of messages processed per unit time
- Measured in messages/second
- Important for: High-volume data processing
- Optimised by: Batching, parallelism, pipelining

**The tension**: Batching improves throughput but increases latency (waiting to fill batch). My system lets you choose: realtime mode optimises latency, benchmark mode optimises throughput.

---

## What I Would Do Differently

### With More Time

1. **Order book simulation**: Add limit order book with matching engine
2. **Realistic volume patterns**: Time-of-day effects, earnings announcements
3. **Correlated instruments**: Multi-asset correlation matrix
4. **Network transport**: Replace queues with ZeroMQ/nanomsg
5. **Persistence**: Write-ahead log for replay and recovery

### In Production

1. **Rewrite hot path in C++/Rust**: 10-100x performance improvement
2. **Kernel bypass**: DPDK or io_uring for network I/O
3. **Lock-free queues**: LMAX Disruptor pattern
4. **Binary serialisation**: Protocol Buffers or FlatBuffers
5. **Dedicated hardware**: CPU pinning, huge pages, NUMA awareness

### Design Mistakes to Avoid

1. **Don't use threading for I/O-bound work**: Asyncio is almost always better
2. **Don't block the event loop**: Keep handlers fast, use executors for slow work
3. **Don't ignore backpressure**: Systems fail when you pretend queues are infinite
4. **Don't measure latency wrong**: Coordinated omission is a real trap

---

## Production Considerations

### Reliability

- **Circuit breakers**: Stop publishing to failed subscribers
- **Dead letter queues**: Capture failed messages for analysis
- **Graceful degradation**: Shed load under pressure
- **Chaos engineering**: Regularly test failure modes

### Security

- **Authentication**: Verify subscriber identity
- **Authorization**: Check subscription permissions
- **Rate limiting**: Prevent abuse
- **Encryption**: TLS for network transport

### Compliance

- **Audit logging**: Record all data for regulatory requirements
- **Data retention**: Store ticks for required period
- **Timestamps**: Accurate, synchronised clocks (PTP)
- **Sequence numbers**: Detect and prove data integrity

---

## Relevant Trading System Concepts

### Terms to Know

| Term | Definition |
|------|------------|
| **Tick** | Smallest price movement or a single market data update |
| **Latency** | Time delay in data transmission |
| **Jitter** | Variation in latency |
| **Throughput** | Messages processed per second |
| **Backpressure** | Mechanism to slow producers when consumers can't keep up |
| **Fan-out** | Publishing one message to many subscribers |
| **Market data** | Real-time price and volume information |
| **Order book** | List of buy and sell orders at various prices |
| **Spread** | Difference between bid and ask prices |
| **Slippage** | Difference between expected and actual execution price |

### Key Papers/Resources

1. **LMAX Architecture**: High-performance trading system design
2. **Mechanical Sympathy**: Martin Thompson's blog on low-latency systems
3. **Trading and Exchanges** (Harris): Market microstructure textbook
4. **High-Frequency Trading** (Aldridge): Industry overview

---

## Final Tips for the Interview

1. **Know your numbers**: Latency targets, throughput achieved, memory usage
2. **Explain trade-offs**: Every decision has pros and cons
3. **Connect to production**: How would this change in a real system?
4. **Show depth**: Be ready to go deeper on any component
5. **Acknowledge limitations**: Show you know what's simplified

Good luck!
