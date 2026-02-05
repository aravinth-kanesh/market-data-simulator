"""
Unit tests for the market data generator.

Tests verify:
- Correct price movement characteristics (GBM)
- Bid/ask spread calculations
- Batch generation efficiency
- Reproducibility with seeds
- Edge cases and error handling
"""

import pytest
import numpy as np
import time

from src.config import SimulatorConfig
from src.generator import MarketDataGenerator, MarketTick, InstrumentState


class TestMarketTick:
    """Tests for MarketTick dataclass."""

    def test_tick_creation(self):
        """Test basic tick creation."""
        tick = MarketTick(
            instrument="AAPL",
            timestamp=time.time(),
            price=175.50,
            volume=100,
            bid=175.48,
            ask=175.52,
            sequence=1,
        )

        assert tick.instrument == "AAPL"
        assert tick.price == 175.50
        assert tick.volume == 100
        assert tick.bid < tick.price < tick.ask

    def test_tick_repr(self):
        """Test tick string representation."""
        tick = MarketTick(
            instrument="GOOGL",
            timestamp=0,
            price=140.00,
            volume=50,
            bid=139.98,
            ask=140.02,
            sequence=1,
        )

        repr_str = repr(tick)
        assert "GOOGL" in repr_str
        assert "140.00" in repr_str


class TestInstrumentState:
    """Tests for InstrumentState class."""

    def test_initial_state(self):
        """Test initial instrument state."""
        state = InstrumentState(
            symbol="TEST",
            initial_price=100.0,
            volatility=0.20,
            drift=0.05,
            spread_bps=5.0,
            tick_rate=100,
            seed=42,
        )

        assert state.symbol == "TEST"
        assert state.price == 100.0
        assert state.volatility == 0.20

    def test_generate_tick(self):
        """Test single tick generation."""
        state = InstrumentState(
            symbol="TEST",
            initial_price=100.0,
            volatility=0.20,
            drift=0.05,
            spread_bps=5.0,
            tick_rate=100,
            seed=42,
        )

        tick = state.generate_tick()

        assert tick.instrument == "TEST"
        assert tick.price > 0  # GBM keeps prices positive
        assert tick.bid < tick.ask
        assert tick.volume >= 1
        assert tick.sequence == 1

    def test_price_stays_positive(self):
        """Test that prices never go negative (GBM property)."""
        state = InstrumentState(
            symbol="TEST",
            initial_price=1.0,  # Start low
            volatility=0.50,  # High volatility
            drift=-0.10,  # Negative drift
            spread_bps=5.0,
            tick_rate=100,
            seed=42,
        )

        # Generate many ticks
        for _ in range(10000):
            tick = state.generate_tick()
            assert tick.price > 0, "Price went negative!"

    def test_spread_calculation(self):
        """Test bid-ask spread is correct."""
        state = InstrumentState(
            symbol="TEST",
            initial_price=100.0,
            volatility=0.20,
            drift=0.05,
            spread_bps=10.0,  # 10 bps = 0.1%
            tick_rate=100,
            seed=42,
        )

        tick = state.generate_tick()

        # Spread should be approximately 0.1% of price
        spread = tick.ask - tick.bid
        expected_spread = tick.price * 0.001
        assert abs(spread - expected_spread) < expected_spread * 0.01

    def test_reproducibility_with_seed(self):
        """Test that same seed produces same sequence."""
        state1 = InstrumentState(
            symbol="TEST",
            initial_price=100.0,
            volatility=0.20,
            drift=0.05,
            spread_bps=5.0,
            tick_rate=100,
            seed=12345,
        )

        state2 = InstrumentState(
            symbol="TEST",
            initial_price=100.0,
            volatility=0.20,
            drift=0.05,
            spread_bps=5.0,
            tick_rate=100,
            seed=12345,
        )

        # Generate sequences and compare prices
        prices1 = [state1.generate_tick().price for _ in range(100)]
        prices2 = [state2.generate_tick().price for _ in range(100)]

        assert prices1 == prices2

    def test_batch_generation(self):
        """Test batch tick generation."""
        state = InstrumentState(
            symbol="TEST",
            initial_price=100.0,
            volatility=0.20,
            drift=0.05,
            spread_bps=5.0,
            tick_rate=100,
            seed=42,
        )

        ticks = state.generate_batch(100)

        assert len(ticks) == 100
        assert all(t.instrument == "TEST" for t in ticks)
        assert all(t.price > 0 for t in ticks)

        # Check sequences are incrementing
        sequences = [t.sequence for t in ticks]
        assert sequences == list(range(1, 101))

    def test_batch_generation_empty(self):
        """Test batch generation with zero count."""
        state = InstrumentState(
            symbol="TEST",
            initial_price=100.0,
            volatility=0.20,
            drift=0.05,
            spread_bps=5.0,
            tick_rate=100,
        )

        ticks = state.generate_batch(0)
        assert ticks == []


class TestMarketDataGenerator:
    """Tests for MarketDataGenerator class."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return SimulatorConfig(
            instruments=("AAPL", "GOOGL", "MSFT"),
            tick_rate_per_instrument=100,
        )

    @pytest.fixture
    def generator(self, config):
        """Create test generator."""
        return MarketDataGenerator(config, seed=42)

    def test_generator_creation(self, generator, config):
        """Test generator initialisation."""
        assert len(generator.instruments) == 3
        assert "AAPL" in generator.instruments
        assert "GOOGL" in generator.instruments
        assert "MSFT" in generator.instruments

    def test_next_tick_round_robin(self, generator):
        """Test round-robin tick generation."""
        instruments_seen = []
        for _ in range(9):  # 3 instruments * 3 cycles
            tick = generator.next_tick()
            instruments_seen.append(tick.instrument)

        # Should cycle through instruments
        assert instruments_seen[:3] == ["AAPL", "GOOGL", "MSFT"]
        assert instruments_seen[3:6] == ["AAPL", "GOOGL", "MSFT"]

    def test_next_tick_specific_instrument(self, generator):
        """Test generating tick for specific instrument."""
        tick = generator.next_tick(instrument="GOOGL")
        assert tick.instrument == "GOOGL"

        tick = generator.next_tick(instrument="AAPL")
        assert tick.instrument == "AAPL"

    def test_next_tick_invalid_instrument(self, generator):
        """Test error on invalid instrument."""
        with pytest.raises(KeyError):
            generator.next_tick(instrument="INVALID")

    def test_batch_generation(self, generator):
        """Test batch generation distributes across instruments."""
        ticks = generator.generate_batch(30)

        assert len(ticks) == 30

        # Count per instrument
        counts = {}
        for tick in ticks:
            counts[tick.instrument] = counts.get(tick.instrument, 0) + 1

        # Should be evenly distributed
        assert counts["AAPL"] == 10
        assert counts["GOOGL"] == 10
        assert counts["MSFT"] == 10

    def test_batch_generation_specific_instrument(self, generator):
        """Test batch generation for specific instrument."""
        ticks = generator.generate_batch(100, instrument="AAPL")

        assert len(ticks) == 100
        assert all(t.instrument == "AAPL" for t in ticks)

    def test_get_current_prices(self, generator):
        """Test getting current prices."""
        # Generate some ticks to move prices
        for _ in range(100):
            generator.next_tick()

        prices = generator.get_current_prices()

        assert len(prices) == 3
        assert all(p > 0 for p in prices.values())

    def test_iterator(self, generator):
        """Test generator as iterator."""
        count = 0
        for tick in generator:
            assert isinstance(tick, MarketTick)
            count += 1
            if count >= 100:
                break

    def test_reproducibility(self, config):
        """Test reproducibility with same seed."""
        gen1 = MarketDataGenerator(config, seed=99999)
        gen2 = MarketDataGenerator(config, seed=99999)

        ticks1 = [gen1.next_tick().price for _ in range(100)]
        ticks2 = [gen2.next_tick().price for _ in range(100)]

        assert ticks1 == ticks2


class TestPriceDistribution:
    """Statistical tests for price distribution properties."""

    def test_returns_are_normal_like(self):
        """Test that log returns approximate normal distribution."""
        state = InstrumentState(
            symbol="TEST",
            initial_price=100.0,
            volatility=0.20,
            drift=0.0,  # Zero drift for cleaner test
            spread_bps=5.0,
            tick_rate=100,
            seed=42,
        )

        # Generate many prices
        prices = [state.generate_tick().price for _ in range(10000)]

        # Calculate log returns
        log_returns = np.diff(np.log(prices))

        # Returns should be approximately normal
        # Check mean is close to zero (with zero drift)
        assert abs(np.mean(log_returns)) < 0.001

        # Check skewness is close to zero
        from scipy import stats
        skewness = stats.skew(log_returns)
        assert abs(skewness) < 0.1

    def test_volatility_scaling(self):
        """Test that realised volatility matches configured volatility."""
        # High volatility
        state_high = InstrumentState(
            symbol="HIGH",
            initial_price=100.0,
            volatility=0.40,  # 40%
            drift=0.0,
            spread_bps=5.0,
            tick_rate=100,
            seed=42,
        )

        # Low volatility
        state_low = InstrumentState(
            symbol="LOW",
            initial_price=100.0,
            volatility=0.10,  # 10%
            drift=0.0,
            spread_bps=5.0,
            tick_rate=100,
            seed=42,
        )

        prices_high = [state_high.generate_tick().price for _ in range(1000)]
        prices_low = [state_low.generate_tick().price for _ in range(1000)]

        std_high = np.std(np.diff(np.log(prices_high)))
        std_low = np.std(np.diff(np.log(prices_low)))

        # High volatility should produce larger moves
        assert std_high > std_low * 2  # At least 2x larger
