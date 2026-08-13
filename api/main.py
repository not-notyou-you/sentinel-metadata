# api/main.py
from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import logging
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from etl.database_client import DatabaseClient
from api.routes import health, lineage, pipeline, preview, products, quality, scenes, storage

logger = logging.getLogger(__name__)

_db_client: DatabaseClient | None = None


def get_db() -> DatabaseClient:
    if _db_client is None:
        raise RuntimeError("DatabaseClient not initialized. App startup failed.")
    return _db_client


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    global _db_client
    logger.info("[API] startup: initializing DatabaseClient")
    _db_client = DatabaseClient.from_env()
    health_info = _db_client.check_health()
    if not health_info.get("connected"):
        logger.error("[API] DB health check FAILED: %s", health_info)
    else:
        logger.info("[API] DB connected. Pool: %s", health_info)
    yield
    logger.info("[API] shutdown: disposing DatabaseClient")
    if _db_client:
        _db_client.dispose()


app = FastAPI(
    title="Sentinel-1 Flood Detection Data Pipeline API",
    description=(
        "REST API for querying Sentinel-1 SAR scenes, data products, "
        "quality metrics, and transformation lineage for flood detection research."
    ),
    version="1.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next) -> Response:
    start = time.perf_counter()
    response: Response = await call_next(request)
    elapsed_ms = int((time.perf_counter() - start) * 1000)
    logger.info("%s %s -> %d (%dms)", request.method, request.url.path, response.status_code, elapsed_ms)
    return response


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Unhandled exception on %s: %s", request.url.path, exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "path": str(request.url.path)},
    )


app.include_router(health.router, prefix="/api", tags=["Health"])
app.include_router(scenes.router, prefix="/api/scenes", tags=["Scenes"])
app.include_router(products.router, prefix="/api/products", tags=["Products"])
app.include_router(quality.router, prefix="/api/quality", tags=["Quality"])
app.include_router(lineage.router, prefix="/api/metadata", tags=["Lineage"])
app.include_router(preview.router, prefix="/api/preview", tags=["Preview"])
app.include_router(storage.router, prefix="/api/storage", tags=["Storage"])
app.include_router(pipeline.router, prefix="/api/pipeline", tags=["Pipeline"])

app.mount("/", StaticFiles(directory="web", html=True), name="web")