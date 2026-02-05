"""
Market Data Simulator - High-performance real-time market data streaming.

A production-quality market data simulator supporting:
- Realistic price movements (Geometric Brownian Motion)
- Concurrent streaming to multiple subscribers
- Sub-millisecond latency with backpressure handling
- Comprehensive performance monitoring
"""

__version__ = "1.0.0"
__author__ = "Aravinth"

from .config import SimulatorConfig
from .generator import MarketDataGenerator, MarketTick
from .streamer import MarketDataStreamer
from .subscriber import Subscriber
from .monitor import PerformanceMonitor

__all__ = [
    "SimulatorConfig",
    "MarketDataGenerator",
    "MarketTick",
    "MarketDataStreamer",
    "Subscriber",
    "PerformanceMonitor",
]
