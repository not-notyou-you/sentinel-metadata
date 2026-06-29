# api/main.py
"""
FastAPI application entry point for the Sentinel-1 Flood Detection Data Pipeline API.
Includes middleware, lifespan management, and router registration.

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara

Run:
    uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from etl.database_client import DatabaseClient
from api.routes import health, lineage, products, quality, scenes

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Dependency: shared DB client (singleton)
# ---------------------------------------------------------------------------
_db_client: DatabaseClient | None = None


def get_db() -> DatabaseClient:
    """FastAPI dependency — returns the shared DatabaseClient."""
    if _db_client is None:
        raise RuntimeError("DatabaseClient not initialized. App startup failed.")
    return _db_client


# ---------------------------------------------------------------------------
# Lifespan (startup / shutdown)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Initialize and dispose DB client around app lifecycle."""
    global _db_client
    logger.info("[API] Startup: initializing DatabaseClient")
    _db_client = DatabaseClient.from_env()
    health_info = _db_client.check_health()
    if not health_info.get("connected"):
        logger.error("[API] DB health check FAILED: %s", health_info)
    else:
        logger.info("[API] DB connected. Pool: %s", health_info)
    yield
    logger.info("[API] Shutdown: disposing DatabaseClient")
    if _db_client:
        _db_client.dispose()


# ---------------------------------------------------------------------------
# App instance
# ---------------------------------------------------------------------------

app = FastAPI(
    title       = "Sentinel-1 Flood Detection Data Pipeline API",
    description = (
        "REST API for querying Sentinel-1 SAR scenes, data products, "
        "quality metrics, and transformation lineage for flood detection research.\n\n"
        "**Author:** Julius Marselinus (BRONTO) — NIM 00000111989\n"
        "**Program:** Sistem Informasi, Universitas Multimedia Nusantara"
    ),
    version     = "1.0.0",
    docs_url    = "/docs",
    redoc_url   = "/redoc",
    openapi_url = "/openapi.json",
    lifespan    = lifespan,
)

# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["*"],   # tighten in production
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next) -> Response:
    """Log all requests with timing."""
    start = time.perf_counter()
    response: Response = await call_next(request)
    elapsed_ms = int((time.perf_counter() - start) * 1000)
    logger.info(
        "%s %s → %d (%dms)",
        request.method, request.url.path, response.status_code, elapsed_ms
    )
    return response


# ---------------------------------------------------------------------------
# Global exception handler
# ---------------------------------------------------------------------------

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Unhandled exception on %s: %s", request.url.path, exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "path": str(request.url.path)},
    )


# ---------------------------------------------------------------------------
# Router registration
# ---------------------------------------------------------------------------

app.include_router(health.router,   prefix="/api",          tags=["Health"])
app.include_router(scenes.router,   prefix="/api/scenes",   tags=["Scenes"])
app.include_router(products.router, prefix="/api/products", tags=["Products"])
app.include_router(quality.router,  prefix="/api/quality",  tags=["Quality"])
app.include_router(lineage.router,  prefix="/api/metadata", tags=["Lineage"])


# ---------------------------------------------------------------------------
# Root redirect
# ---------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
async def root():
    return {"message": "Sentinel-1 Pipeline API", "docs": "/docs", "version": "1.0.0"}
