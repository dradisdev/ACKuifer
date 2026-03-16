"""FastAPI application entry point."""

from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.routers import admin, api, map, signup

app = FastAPI(title="ACKuifer", version="1.0.0")


@app.get("/health")
def health_check():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Include routers
app.include_router(map.router)
app.include_router(api.router)
app.include_router(signup.router)
app.include_router(admin.router)
