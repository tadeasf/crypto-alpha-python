"""
Main application entry point for the Crypto Market Alpha Engine.
"""
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app
from sqlmodel.ext.asyncio.session import AsyncSession

from crypto_alpha_python.api.v1.api import api_router
from crypto_alpha_python.core.config import settings
from crypto_alpha_python.core.logging import setup_logging, logger
from crypto_alpha_python.core.tasks import task_manager
from crypto_alpha_python.db.session import get_session


# Setup logging
setup_logging()

# Configure Swagger UI
swagger_ui_parameters = {
    "syntaxHighlight": {
        "activated": True,
        "theme": "monokai"
    },
    "docExpansion": "full",
    "defaultModelsExpandDepth": 2,
    "defaultModelExpandDepth": 2,
    "displayOperationId": True,
    "displayRequestDuration": True,
    "filter": True,
    "showExtensions": True,
    "showCommonExtensions": True,
    "tryItOutEnabled": True,
    "persistAuthorization": True,
    "deepLinking": True,
    "supportedSubmitMethods": ["get", "put", "post", "delete", "options", "head", "patch", "trace"]
}

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="A high-performance crypto market data processing and analysis system",
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    swagger_ui_parameters=swagger_ui_parameters,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Include API router
app.include_router(api_router, prefix=settings.API_V1_STR)

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    # Start market data collection for default symbols
    default_symbols = ["BTCUSDT", "ETHUSDT"]
    try:
        async for session in get_session():
            try:
                await task_manager.start_collection(
                    session=session,
                    symbols=default_symbols,
                    is_default=True,  # Mark as default/protected symbols
                )
            except Exception as e:
                logger.error(f"Error starting market data collection: {e}")
                raise
    except Exception as e:
        logger.error(f"Error during startup: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    await task_manager.stop_collection()

@app.get("/")
async def root():
    return {
        "message": "Welcome to Crypto Market Alpha Engine",
        "version": settings.VERSION,
        "docs_url": "/docs",
        "redoc_url": "/redoc",
    }
