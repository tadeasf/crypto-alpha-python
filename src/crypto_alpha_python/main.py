"""
Main application entry point for the Crypto Market Alpha Engine.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from crypto_alpha_python.api.v1.api import api_router
from crypto_alpha_python.core.config import settings
from crypto_alpha_python.core.logging import setup_logging

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

@app.get("/")
async def root():
    return {
        "message": "Welcome to Crypto Market Alpha Engine",
        "version": settings.VERSION,
        "docs_url": "/docs",
        "redoc_url": "/redoc",
    }
