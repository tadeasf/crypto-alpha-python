"""
Database initialization script.
"""
import asyncio
from crypto_alpha_python.db.session import init_db
from crypto_alpha_python.core.logging import setup_logging

async def main() -> None:
    """Initialize the database."""
    setup_logging()
    await init_db()
    print("Database initialized successfully!")

if __name__ == "__main__":
    asyncio.run(main()) 