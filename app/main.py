"""FastAPI application entry point."""

from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.secret_guard import enforce_secrets

# S-1: on Railway, refuse to start without a real ADMIN_PASSWORD and SECRET_KEY.
enforce_secrets("web")

from app.routers import admin, api, map, signup  # noqa: E402

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
