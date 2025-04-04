# Crypto Market Alpha Engine

A high-performance crypto market data processing and analysis system built with Python, FastAPI, and TimescaleDB.

## Features

- Real-time market data aggregation from multiple exchanges (Binance, Coinbase)
- Quantitative analysis endpoints (volatility, order book imbalance, TWAP)
- Portfolio simulation engine with backtesting capabilities
- Market surveillance and anomaly detection
- High-performance data storage with TimescaleDB
- Real-time caching with Redis
- Monitoring with Prometheus and Grafana

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Exchange API keys (Binance, Coinbase)

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

### Testing

Run tests with:
```bash
pytest
```

## License

MIT License - see LICENSE file for details
