"""
Market surveillance service for detecting market anomalies and manipulation.
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import numpy as np
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
import re
import logging

from crypto_alpha_python.models.market_data import MarketData
from crypto_alpha_python.models.surveillance import (
    VolumeSpikeResponse,
    PriceJumpResponse,
    WashTradingResponse,
    VolumeSpike,
    PriceJump,
    SuspiciousPeriod,
)
from crypto_alpha_python.services.market_data import get_market_data
from crypto_alpha_python.services.redis import redis_service

logger = logging.getLogger(__name__)

class MarketSurveillance:
    """Service for detecting market anomalies and manipulation patterns."""
    
    def __init__(self, session: AsyncSession):
        """Initialize the surveillance service."""
        self.session = session
    
    def _parse_window(self, window: str) -> timedelta:
        """
        Parse window string and convert to timedelta.
        
        Supports formats:
        - Nx seconds: e.g. "30s"
        - Nx minutes: e.g. "5min"
        - Nx hours: e.g. "2h"
        - Nx days: e.g. "3d"
        - Nx weeks: e.g. "1w"
        - Nx months: e.g. "2m" (approximate, using 30 days per month)
        """
        match = re.match(r"^(\d+)(min|h|d|w|m|s)$", window)
        if not match:
            raise ValueError(f"Invalid window format: {window}. Use patterns like '1min', '5min', '1h', '1d', '1w', '1m'.")
        
        value = int(match.group(1))
        unit = match.group(2)
        
        # Convert to timedelta based on unit
        if unit == "s":
            return timedelta(seconds=value)
        elif unit == "min":
            return timedelta(minutes=value)
        elif unit == "h":
            return timedelta(hours=value)
        elif unit == "d":
            return timedelta(days=value)
        elif unit == "w":
            return timedelta(weeks=value)
        elif unit == "m":
            # Approximate months as 30 days
            return timedelta(days=30 * value)
        else:
            # This should never happen due to regex validation
            raise ValueError(f"Unknown time unit: {unit}")
    
    async def detect_volume_spike(
        self,
        symbol: str,
        threshold: float = 3.0,
        window: str = "1h",
        exchange: str | None = None,
    ) -> VolumeSpikeResponse:
        """
        Detect volume spikes in market data.
        
        Args:
            symbol: The trading symbol (e.g., 'BTCUSDT')
            threshold: Z-score threshold for spike detection
            window: Time window for analysis
            exchange: Optional exchange name to filter data
            
        Returns:
            VolumeSpikeResponse with detection results
        """
        try:
            delta = self._parse_window(window)
            logger.debug(f"Analyzing volume spikes for {symbol} over {window} window")
        except ValueError as e:
            logger.error(f"Error parsing window for volume spike detection: {e}")
            return VolumeSpikeResponse(
                symbol=symbol,
                window=window,
                threshold=threshold,
                detected=False,
                message=f"Invalid window format: {e}",
            )
        
        # Generate cache key
        cache_key = redis_service.generate_key(
            prefix="volume_spike",
            symbol=symbol,
            window=window,
            exchange=exchange,
            threshold=threshold
        )
        
        # Try to get from cache first
        cached_response = await redis_service.get(cache_key)
        if cached_response:
            return VolumeSpikeResponse(**cached_response)
        
        end_time = datetime.utcnow()
        start_time = end_time - delta
        
        # Get market data
        data = await get_market_data(
            session=self.session,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            exchange=exchange,
        )
        
        if not data:
            logger.warning(f"No data available for {symbol} in the specified time window")
            return VolumeSpikeResponse(
                symbol=symbol,
                window=window,
                threshold=threshold,
                detected=False,
                message="No data available for the specified time window",
            )
        
        # Calculate volume statistics
        volumes = [d.volume for d in data]
        mean_volume = np.mean(volumes)
        std_volume = np.std(volumes)
        
        if std_volume == 0:
            logger.warning(f"Zero standard deviation for {symbol} volumes")
            return VolumeSpikeResponse(
                symbol=symbol,
                window=window,
                threshold=threshold,
                detected=False,
                message="Constant volume - no spikes possible",
                mean_volume=float(mean_volume),
                std_volume=0.0,
            )
        
        # Find spikes
        spikes = []
        for d in data:
            z_score = (d.volume - mean_volume) / std_volume if std_volume > 0 else 0
            if z_score > threshold:
                spikes.append(VolumeSpike(
                    timestamp=d.timestamp,
                    volume=d.volume,
                    z_score=float(z_score),
                ))
        
        result = VolumeSpikeResponse(
            symbol=symbol,
            window=window,
            threshold=threshold,
            detected=len(spikes) > 0,
            spikes=spikes,
            mean_volume=float(mean_volume),
            std_volume=float(std_volume),
        )
        
        if len(spikes) > 0:
            result.message = f"Detected {len(spikes)} volume spike(s)"
            logger.info(f"Detected {len(spikes)} volume spike(s) for {symbol}")
        else:
            result.message = "No volume spikes detected"
            logger.debug(f"No volume spikes detected for {symbol}")
        
        # Cache the result for 5 minutes
        await redis_service.set(
            cache_key,
            result.dict(),
            expire_seconds=300
        )
        
        return result
    
    async def detect_price_jump(
        self,
        symbol: str,
        threshold: float = 0.02,
        window: str = "1h",
        exchange: str | None = None,
    ) -> PriceJumpResponse:
        """
        Detect significant price jumps.
        
        Args:
            symbol: The trading symbol (e.g., 'BTCUSDT')
            threshold: Price change threshold as a decimal (0.02 = 2%)
            window: Time window for analysis
            exchange: Optional exchange name to filter data
            
        Returns:
            PriceJumpResponse with detection results
        """
        try:
            delta = self._parse_window(window)
            logger.debug(f"Analyzing price jumps for {symbol} over {window} window")
        except ValueError as e:
            logger.error(f"Error parsing window for price jump detection: {e}")
            return PriceJumpResponse(
                symbol=symbol,
                window=window,
                threshold=threshold,
                detected=False,
                message=f"Invalid window format: {e}",
            )
        
        # Generate cache key
        cache_key = redis_service.generate_key(
            prefix="price_jump",
            symbol=symbol,
            window=window,
            exchange=exchange,
            threshold=threshold
        )
        
        # Try to get from cache first
        cached_response = await redis_service.get(cache_key)
        if cached_response:
            return PriceJumpResponse(**cached_response)
        
        end_time = datetime.utcnow()
        start_time = end_time - delta
        
        # Get market data
        data = await get_market_data(
            session=self.session,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            exchange=exchange,
        )
        
        if not data:
            logger.warning(f"No data available for {symbol} in the specified time window")
            return PriceJumpResponse(
                symbol=symbol,
                window=window,
                threshold=threshold,
                detected=False,
                message="No data available for the specified time window",
            )
        
        # Sort data by timestamp to ensure correct ordering
        data.sort(key=lambda x: x.timestamp)
        
        # Calculate price changes
        jumps = []
        for i in range(1, len(data)):
            price_change = (data[i].last_price - data[i-1].last_price) / data[i-1].last_price
            if abs(price_change) > threshold:
                jumps.append(PriceJump(
                    timestamp=data[i].timestamp,
                    price=data[i].last_price,
                    change=float(price_change),
                    volume=data[i].volume,
                ))
        
        result = PriceJumpResponse(
            symbol=symbol,
            window=window,
            threshold=threshold,
            detected=len(jumps) > 0,
            jumps=jumps,
        )
        
        if len(jumps) > 0:
            result.message = f"Detected {len(jumps)} price jump(s)"
            logger.info(f"Detected {len(jumps)} price jump(s) for {symbol}")
        else:
            result.message = "No significant price jumps detected"
            logger.debug(f"No price jumps detected for {symbol}")
        
        # Cache the result for 5 minutes
        await redis_service.set(
            cache_key,
            result.dict(),
            expire_seconds=300
        )
        
        return result
    
    async def detect_wash_trading(
        self,
        symbol: str,
        window: str = "1h",
        exchange: str | None = None,
    ) -> WashTradingResponse:
        """
        Detect potential wash trading patterns.
        
        Args:
            symbol: The trading symbol (e.g., 'BTCUSDT')
            window: Time window for analysis
            exchange: Optional exchange name to filter data
            
        Returns:
            WashTradingResponse with detection results
        """
        try:
            delta = self._parse_window(window)
            logger.debug(f"Analyzing wash trading for {symbol} over {window} window")
        except ValueError as e:
            logger.error(f"Error parsing window for wash trading detection: {e}")
            return WashTradingResponse(
                symbol=symbol,
                window=window,
                detected=False,
                message=f"Invalid window format: {e}",
            )
        
        # Generate cache key
        cache_key = redis_service.generate_key(
            prefix="wash_trading",
            symbol=symbol,
            window=window,
            exchange=exchange
        )
        
        # Try to get from cache first
        cached_response = await redis_service.get(cache_key)
        if cached_response:
            return WashTradingResponse(**cached_response)
        
        end_time = datetime.utcnow()
        start_time = end_time - delta
        
        # Get market data
        data = await get_market_data(
            session=self.session,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            exchange=exchange,
        )
        
        if not data:
            logger.warning(f"No data available for {symbol} in the specified time window")
            return WashTradingResponse(
                symbol=symbol,
                window=window,
                detected=False,
                message="No data available for the specified time window",
            )
        
        # Sort data by timestamp to ensure correct ordering
        data.sort(key=lambda x: x.timestamp)
        
        # Calculate suspicious patterns
        suspicious_periods = []
        for i in range(1, len(data)):
            try:
                # Check for rapid price reversals
                price_change = (data[i].last_price - data[i-1].last_price) / data[i-1].last_price
                
                # Need to handle the case where previous volume could be zero
                prev_volume = max(data[i-1].volume, 0.0001)  # Avoid division by zero
                volume_change = (data[i].volume - data[i-1].volume) / prev_volume
                
                # Look for patterns where price changes direction rapidly with high volume
                if abs(price_change) > 0.01 and abs(volume_change) > 2.0:
                    suspicious_periods.append(SuspiciousPeriod(
                        timestamp=data[i].timestamp,
                        price_change=float(price_change),
                        volume_change=float(volume_change),
                        price=data[i].last_price,
                        volume=data[i].volume,
                    ))
            except (ZeroDivisionError, ValueError) as e:
                logger.warning(f"Error calculating wash trading metrics: {e}")
                continue
        
        result = WashTradingResponse(
            symbol=symbol,
            window=window,
            detected=len(suspicious_periods) > 0,
            suspicious_periods=suspicious_periods,
        )
        
        if len(suspicious_periods) > 0:
            result.message = f"Detected {len(suspicious_periods)} suspicious trading pattern(s)"
            logger.info(f"Detected {len(suspicious_periods)} suspicious trading pattern(s) for {symbol}")
        else:
            result.message = "No suspicious wash trading patterns detected"
            logger.debug(f"No wash trading patterns detected for {symbol}")
        
        # Cache the result for 5 minutes
        await redis_service.set(
            cache_key,
            result.dict(),
            expire_seconds=300
        )
        
        return result 