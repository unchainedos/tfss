from fastapi import FastAPI, Request, Response
from routes import router
from cache import redis_cache
from db import db
from metrics import get_metrics, track_endpoint_metrics, increment_endpoint_counter, record_endpoint_duration
import uvicorn
from contextlib import asynccontextmanager
import time
from prometheus_client import CONTENT_TYPE_LATEST


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    await redis_cache.connect()
    print("Application started")
    
    yield
    
    await redis_cache.disconnect()
    await db.disconnect()
    print("Application stopped")

app = FastAPI(
    title="Tasks API with Metrics",
    description="REST API для задач с метриками Prometheus",
    version="2.0.0",
    lifespan=lifespan
)

@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start_time = time.time()
    
    try:
        response = await call_next(request)
        
        path = request.url.path
        method = request.method
        
        if path == "/":
            increment_endpoint_counter('root')
        elif path == "/health":
            increment_endpoint_counter('health_check')
            record_endpoint_duration('health_check', time.time() - start_time)
        elif path == "/metrics":
            pass  
        
        return response
        
    except Exception as e:
        raise
    finally:
        pass

app.include_router(router, prefix="/api/v1")

@app.get("/")
@track_endpoint_metrics("root", "GET")
async def root():
    return {
        "message": "Tasks API with Metrics is running",
        "docs": "/docs",
        "redoc": "/redoc",
        "health": "/health",
        "metrics": "/metrics",
        "cache_stats": "/api/v1/tasks/cache/stats"
    }

@app.get("/health")
@track_endpoint_metrics("health_check", "GET")
async def health_check():
    health_status = {
        "status": "healthy",
        "database": "disconnected",
        "cache": "disabled",
        "timestamp": __import__("datetime").datetime.utcnow().isoformat()
    }
    
    try:
        if db.pool:
            async with db.pool.acquire() as conn:
                await conn.execute("SELECT 1")
            health_status["database"] = "connected"
    except Exception as e:
        health_status["database"] = f"error: {str(e)}"
        health_status["status"] = "unhealthy"
    
    if redis_cache.enabled:
        if redis_cache.is_connected():
            try:
                await redis_cache.client.ping()
                health_status["cache"] = "connected"
            except Exception as e:
                health_status["cache"] = f"error: {str(e)}"
                health_status["status"] = "unhealthy"
        else:
            health_status["cache"] = "disconnected"
            health_status["status"] = "degraded"
    
    return health_status

@app.get("/metrics")
async def metrics():
    return Response(
        content=get_metrics(),
        media_type=CONTENT_TYPE_LATEST
    )

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)