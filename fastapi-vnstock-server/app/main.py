from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.routers.dnse_trade import router as dnse_trade_router
from app.routers.health import router as health_router
from app.routers.market import router as market_router
from app.routers.vnstock_api import router as vnstock_api_router

app = FastAPI(title=settings.app_name)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(market_router)
app.include_router(vnstock_api_router)
app.include_router(dnse_trade_router)

