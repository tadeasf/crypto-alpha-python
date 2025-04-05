"""
SQLAlchemy declarative base and all models.
Import all models here for Alembic to discover them.
"""
from sqlmodel import SQLModel as Base

# Import all models here
from crypto_alpha_python.models.market_data import MarketData  # noqa
from crypto_alpha_python.models.surveillance import (  # noqa
    VolumeSpike,
    PriceJump,
    SuspiciousPeriod,
) 