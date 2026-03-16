"""FastAPI application entry point."""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.routers import api, map, signup

app = FastAPI(title="ACKuifer", version="1.0.0")

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Include routers
app.include_router(map.router)
app.include_router(api.router)
app.include_router(signup.router)
