# Crypto Market Alpha Engine

A high-performance crypto market data processing and analysis system built with Python, FastAPI, and TimescaleDB.

## Features

### Market Data Collection
- Real-time market data aggregation from multiple exchanges (Binance, Coinbase)
- WebSocket streaming for live price updates
- Historical data storage with TimescaleDB
- Configurable data collection intervals

### Market Analysis
- Volatility calculation with configurable time windows
- Order book imbalance analysis
- Time-Weighted Average Price (TWAP) calculation
- Bid-ask spread analysis
- Volume profile analysis

### Portfolio Management
- Portfolio value tracking
- Position management
- Order execution and tracking
- Performance metrics:
  - Value at Risk (VaR) calculation
  - Sharpe ratio analysis
  - Portfolio returns tracking

### Market Surveillance
- Volume spike detection using z-scores
- Price jump detection
- Wash trading pattern detection
- Anomaly detection with configurable thresholds

### Technical Features
- High-performance data storage with TimescaleDB
- Real-time caching with Redis
- Monitoring with Prometheus and Grafana
- Async/await architecture for better performance
- Type-safe API with Pydantic models
- Comprehensive API documentation with Swagger UI

## API Endpoints

### Market Data
- `GET /api/v1/market-data/{symbol}` - Get historical market data
- `GET /api/v1/market-data/spreads/{symbol}` - Get bid-ask spreads
- `WS /api/v1/ws/market-data/{symbol}` - WebSocket stream for real-time data

### Analysis
- `GET /api/v1/analysis/volatility/{symbol}` - Calculate volatility
- `GET /api/v1/analysis/orderbook-imbalance/{symbol}` - Get order book imbalance
- `GET /api/v1/analysis/twap/{symbol}` - Calculate TWAP

### Portfolio
- `GET /api/v1/portfolio/value/{user_id}` - Get portfolio value
- `GET /api/v1/portfolio/var/{user_id}` - Calculate Value at Risk
- `GET /api/v1/portfolio/sharpe/{user_id}` - Calculate Sharpe ratio

### Orders
- `POST /api/v1/orders/` - Create new order
- `GET /api/v1/orders/{order_id}` - Get order details
- `GET /api/v1/orders/user/{user_id}` - Get user's orders
- `PATCH /api/v1/orders/{order_id}/status` - Update order status

### Surveillance
- `GET /api/v1/surveillance/volume-spike/{symbol}` - Detect volume spikes
- `GET /api/v1/surveillance/price-jump/{symbol}` - Detect price jumps
- `GET /api/v1/surveillance/wash-trading/{symbol}` - Detect wash trading

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Exchange API keys (Binance, Coinbase)
- TimescaleDB
- Redis

## Environment Variables

Create a `.env` file with the following variables:

```env
# Database
POSTGRES_SERVER=localhost
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=crypto_alpha

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# Exchange API Keys
BINANCE_API_KEY=your_binance_key
BINANCE_API_SECRET=your_binance_secret
COINBASE_API_KEY=your_coinbase_key
COINBASE_API_SECRET=your_coinbase_secret

# JWT
SECRET_KEY=your_secret_key
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
```

## Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/crypto-alpha-python.git
cd crypto-alpha-python
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -e .
```

4. Copy the environment file and update with your settings:
```bash
cp .env.example .env
# Edit .env with your configuration
```

5. Start the development environment:
```bash
docker-compose up -d
```

6. Initialize the database:
```bash
python -m crypto_alpha_python.db.init_db
```

7. Start the application:
```bash
uvicorn crypto_alpha_python.main:app --reload
```

## API Documentation

Once the application is running, you can access:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Monitoring

- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (default credentials: admin/admin)

## Development

### Code Style

This project uses:
- Black for code formatting
- isort for import sorting
- mypy for type checking
- ruff for linting

To format code:
```bash
black .
isort .
```

To run type checking:
```bash
mypy .
```

To run linting:
```bash
ruff check .
```

### Database Migrations

Create a new migration:
```bash
alembic revision --autogenerate -m "description"
```

Apply migrations:
```bash
alembic upgrade head
```

### Testing

Run tests with:
```bash
pytest
```

## License

MIT License - see LICENSE file for details
