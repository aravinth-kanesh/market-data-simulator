"""
Configuration management for the market data simulator.

Provides a centralised, immutable configuration object with validation
and sensible defaults for all simulator parameters.
"""

from dataclasses import dataclass, field
from typing import Final
import os


# Default instrument universe - representative US equities
DEFAULT_INSTRUMENTS: Final[tuple[str, ...]] = (
    "AAPL", "GOOGL", "MSFT", "AMZN", "META",
    "NVDA", "TSLA", "JPM", "V", "JNJ",
)

# Default initial prices (roughly based on real prices, scaled for simplicity)
DEFAULT_INITIAL_PRICES: Final[dict[str, float]] = {
    "AAPL": 175.0,
    "GOOGL": 140.0,
    "MSFT": 380.0,
    "AMZN": 180.0,
    "META": 500.0,
    "NVDA": 880.0,
    "TSLA": 250.0,
    "JPM": 195.0,
    "V": 280.0,
    "JNJ": 160.0,
}


@dataclass(frozen=True, slots=True)
class SimulatorConfig:
    """
    Immutable configuration for the market data simulator.

    Using frozen=True ensures thread-safety and prevents accidental mutation.
    Using slots=True reduces memory footprint - important for high-frequency systems.

    Attributes:
        instruments: Tuple of instrument symbols to simulate.
        initial_prices: Mapping of instrument to starting price.
        tick_rate_per_instrument: Target ticks per second per instrument.
        num_subscribers: Number of concurrent subscribers to support.
        queue_size: Size of each subscriber's message queue.
        backpressure_policy: How to handle full queues ('drop' or 'block').
        volatility: Annual volatility for price simulation (0.0 to 1.0).
        drift: Annual drift/return for price simulation.
        spread_bps: Bid-ask spread in basis points.
        monitoring_interval_seconds: How often to report performance metrics.
        enable_monitoring: Whether to collect and report performance metrics.
        log_level: Logging verbosity ('DEBUG', 'INFO', 'WARNING', 'ERROR').
    """

    # Instrument configuration
    instruments: tuple[str, ...] = DEFAULT_INSTRUMENTS
    initial_prices: dict[str, float] = field(default_factory=lambda: DEFAULT_INITIAL_PRICES.copy())

    # Performance configuration
    tick_rate_per_instrument: int = 100  # ticks per second per instrument
    num_subscribers: int = 10
    queue_size: int = 10_000  # messages per subscriber queue
    backpressure_policy: str = "drop"  # 'drop' or 'block'

    # Market simulation parameters
    volatility: float = 0.20  # 20% annual volatility (typical for equities)
    drift: float = 0.05  # 5% annual drift
    spread_bps: float = 5.0  # 5 basis points spread (typical for liquid stocks)

    # Monitoring configuration
    monitoring_interval_seconds: float = 5.0
    enable_monitoring: bool = True

    # General configuration
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        self._validate()

    def _validate(self) -> None:
        """
        Validate all configuration parameters.

        Raises:
            ValueError: If any parameter is invalid.
        """
        if not self.instruments:
            raise ValueError("Must specify at least one instrument")

        if self.tick_rate_per_instrument < 1 or self.tick_rate_per_instrument > 10_000:
            raise ValueError(
                f"tick_rate_per_instrument must be between 1 and 10000, "
                f"got {self.tick_rate_per_instrument}"
            )

        if self.num_subscribers < 1 or self.num_subscribers > 1000:
            raise ValueError(
                f"num_subscribers must be between 1 and 1000, "
                f"got {self.num_subscribers}"
            )

        if self.queue_size < 100 or self.queue_size > 1_000_000:
            raise ValueError(
                f"queue_size must be between 100 and 1000000, "
                f"got {self.queue_size}"
            )

        if self.backpressure_policy not in ("drop", "block"):
            raise ValueError(
                f"backpressure_policy must be 'drop' or 'block', "
                f"got '{self.backpressure_policy}'"
            )

        if self.volatility < 0.0 or self.volatility > 2.0:
            raise ValueError(
                f"volatility must be between 0.0 and 2.0, got {self.volatility}"
            )

        if self.spread_bps < 0.0 or self.spread_bps > 100.0:
            raise ValueError(
                f"spread_bps must be between 0.0 and 100.0, got {self.spread_bps}"
            )

        if self.monitoring_interval_seconds < 0.1:
            raise ValueError(
                f"monitoring_interval_seconds must be at least 0.1, "
                f"got {self.monitoring_interval_seconds}"
            )

        # Validate initial prices exist for all instruments
        missing = set(self.instruments) - set(self.initial_prices.keys())
        if missing:
            raise ValueError(
                f"Missing initial prices for instruments: {missing}"
            )

    @property
    def total_tick_rate(self) -> int:
        """Total target ticks per second across all instruments."""
        return self.tick_rate_per_instrument * len(self.instruments)

    @property
    def tick_interval_ns(self) -> int:
        """Interval between ticks in nanoseconds for each instrument."""
        return int(1_000_000_000 / self.tick_rate_per_instrument)

    @classmethod
    def from_env(cls) -> "SimulatorConfig":
        """
        Create configuration from environment variables.

        Environment variables (all optional):
            MDS_INSTRUMENTS: Comma-separated list of instruments
            MDS_TICK_RATE: Ticks per second per instrument
            MDS_NUM_SUBSCRIBERS: Number of subscribers
            MDS_QUEUE_SIZE: Queue size per subscriber
            MDS_BACKPRESSURE: 'drop' or 'block'
            MDS_VOLATILITY: Annual volatility
            MDS_SPREAD_BPS: Spread in basis points
            MDS_MONITORING_INTERVAL: Monitoring interval in seconds
            MDS_LOG_LEVEL: Logging level

        Returns:
            SimulatorConfig with values from environment or defaults.
        """
        kwargs: dict = {}

        if instruments := os.environ.get("MDS_INSTRUMENTS"):
            kwargs["instruments"] = tuple(s.strip() for s in instruments.split(","))

        if tick_rate := os.environ.get("MDS_TICK_RATE"):
            kwargs["tick_rate_per_instrument"] = int(tick_rate)

        if num_subs := os.environ.get("MDS_NUM_SUBSCRIBERS"):
            kwargs["num_subscribers"] = int(num_subs)

        if queue_size := os.environ.get("MDS_QUEUE_SIZE"):
            kwargs["queue_size"] = int(queue_size)

        if backpressure := os.environ.get("MDS_BACKPRESSURE"):
            kwargs["backpressure_policy"] = backpressure

        if volatility := os.environ.get("MDS_VOLATILITY"):
            kwargs["volatility"] = float(volatility)

        if spread := os.environ.get("MDS_SPREAD_BPS"):
            kwargs["spread_bps"] = float(spread)

        if interval := os.environ.get("MDS_MONITORING_INTERVAL"):
            kwargs["monitoring_interval_seconds"] = float(interval)

        if log_level := os.environ.get("MDS_LOG_LEVEL"):
            kwargs["log_level"] = log_level.upper()

        return cls(**kwargs)
