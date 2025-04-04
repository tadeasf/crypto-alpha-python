"""
Pydantic models for surveillance responses.
"""
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel

class VolumeSpike(BaseModel):
    """Model for volume spike data."""
    timestamp: datetime
    volume: float
    z_score: float

class VolumeSpikeResponse(BaseModel):
    """Response model for volume spike detection."""
    symbol: str
    window: str
    threshold: float
    detected: bool
    message: Optional[str] = None
    spikes: List[VolumeSpike] = []
    mean_volume: Optional[float] = None
    std_volume: Optional[float] = None

class PriceJump(BaseModel):
    """Model for price jump data."""
    timestamp: datetime
    price: float
    change: float
    volume: float

class PriceJumpResponse(BaseModel):
    """Response model for price jump detection."""
    symbol: str
    window: str
    threshold: float
    detected: bool
    message: Optional[str] = None
    jumps: List[PriceJump] = []

class SuspiciousPeriod(BaseModel):
    """Model for suspicious trading period data."""
    timestamp: datetime
    price_change: float
    volume_change: float
    price: float
    volume: float

class WashTradingResponse(BaseModel):
    """Response model for wash trading detection."""
    symbol: str
    window: str
    detected: bool
    message: Optional[str] = None
    suspicious_periods: List[SuspiciousPeriod] = [] 