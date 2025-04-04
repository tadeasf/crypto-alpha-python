"""
Market surveillance service for detecting market anomalies and manipulation.
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import numpy as np
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

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

class MarketSurveillance:
    """Service for detecting market anomalies and manipulation patterns."""
    
    def __init__(self, session: AsyncSession):
        """Initialize the surveillance service."""
        self.session = session
    
    async def detect_volume_spike(
        self,
        symbol: str,
        threshold: float = 3.0,
        window: str = "1h",
        exchange: str | None = None,
    ) -> VolumeSpikeResponse:
        """
        Detect volume spikes in market data.
        """
        # Convert window string to timedelta
        value = int(window[:-1])
        unit = window[-1]
        delta = {
            "s": timedelta(seconds=value),
            "m": timedelta(minutes=value),
            "h": timedelta(hours=value),
            "d": timedelta(days=value),
        }[unit]
        
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
            return VolumeSpikeResponse(
                symbol=symbol,
                window=window,
                threshold=threshold,
                detected=False,
                message="No data available",
            )
        
        # Calculate volume statistics
        volumes = [d.volume for d in data]
        mean_volume = np.mean(volumes)
        std_volume = np.std(volumes)
        
        # Find spikes
        spikes = []
        for d in data:
            z_score = (d.volume - mean_volume) / std_volume
            if z_score > threshold:
                spikes.append(VolumeSpike(
                    timestamp=d.timestamp,
                    volume=d.volume,
                    z_score=float(z_score),
                ))
        
        return VolumeSpikeResponse(
            symbol=symbol,
            window=window,
            threshold=threshold,
            detected=len(spikes) > 0,
            spikes=spikes,
            mean_volume=float(mean_volume),
            std_volume=float(std_volume),
        )
    
    async def detect_price_jump(
        self,
        symbol: str,
        threshold: float = 0.02,
        window: str = "1h",
        exchange: str | None = None,
    ) -> PriceJumpResponse:
        """
        Detect significant price jumps.
        """
        # Convert window string to timedelta
        value = int(window[:-1])
        unit = window[-1]
        delta = {
            "s": timedelta(seconds=value),
            "m": timedelta(minutes=value),
            "h": timedelta(hours=value),
            "d": timedelta(days=value),
        }[unit]
        
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
            return PriceJumpResponse(
                symbol=symbol,
                window=window,
                threshold=threshold,
                detected=False,
                message="No data available",
            )
        
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
        
        return PriceJumpResponse(
            symbol=symbol,
            window=window,
            threshold=threshold,
            detected=len(jumps) > 0,
            jumps=jumps,
        )
    
    async def detect_wash_trading(
        self,
        symbol: str,
        window: str = "1h",
        exchange: str | None = None,
    ) -> WashTradingResponse:
        """
        Detect potential wash trading patterns.
        """
        # Convert window string to timedelta
        value = int(window[:-1])
        unit = window[-1]
        delta = {
            "s": timedelta(seconds=value),
            "m": timedelta(minutes=value),
            "h": timedelta(hours=value),
            "d": timedelta(days=value),
        }[unit]
        
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
            return WashTradingResponse(
                symbol=symbol,
                window=window,
                detected=False,
                message="No data available",
            )
        
        # Calculate suspicious patterns
        suspicious_periods = []
        for i in range(1, len(data)):
            # Check for rapid price reversals
            price_change = (data[i].last_price - data[i-1].last_price) / data[i-1].last_price
            volume_change = (data[i].volume - data[i-1].volume) / data[i-1].volume
            
            # Look for patterns where price changes direction rapidly with high volume
            if abs(price_change) > 0.01 and abs(volume_change) > 2.0:
                suspicious_periods.append(SuspiciousPeriod(
                    timestamp=data[i].timestamp,
                    price_change=float(price_change),
                    volume_change=float(volume_change),
                    price=data[i].last_price,
                    volume=data[i].volume,
                ))
        
        return WashTradingResponse(
            symbol=symbol,
            window=window,
            detected=len(suspicious_periods) > 0,
            suspicious_periods=suspicious_periods,
        ) 