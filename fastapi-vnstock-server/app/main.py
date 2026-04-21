from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.routers.ai import router as ai_router
from app.routers.auto_trading import router as auto_trading_router
from app.routers.automation import router as automation_router
from app.routers.dnse_trade import router as dnse_trade_router
from app.routers.experience import router as experience_router
from app.routers.health import router as health_router
from app.routers.monitoring import router as monitoring_router
from app.routers.market import router as market_router
from app.routers.gmail import router as gmail_router
from app.routers.news import router as news_router
from app.routers.scanner import router as scanner_router
from app.routers.signal_engine import router as signal_engine_router
from app.routers.trading_core import router as trading_core_router
from app.routers.vnstock_api import router as vnstock_api_router
from app.services.automation_scheduler_service import (
    start_automation_scheduler,
    stop_automation_scheduler,
)
from app.services.dnse_trading_token_scheduler import (
    start_dnse_trading_token_refresh_scheduler,
    stop_dnse_trading_token_refresh_scheduler,
)
from app.services.mail_signal_scheduler_service import (
    start_mail_signal_scheduler,
    stop_mail_signal_scheduler,
)

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
app.include_router(ai_router)
app.include_router(market_router)
app.include_router(gmail_router)
app.include_router(news_router)
app.include_router(vnstock_api_router)
app.include_router(dnse_trade_router)
app.include_router(experience_router)
app.include_router(scanner_router)
app.include_router(auto_trading_router)
app.include_router(automation_router)
app.include_router(trading_core_router)
app.include_router(monitoring_router)
app.include_router(signal_engine_router)


@app.on_event("startup")
async def on_startup() -> None:
    await start_automation_scheduler()
    await start_dnse_trading_token_refresh_scheduler()
    await start_mail_signal_scheduler()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await stop_mail_signal_scheduler()
    await stop_dnse_trading_token_refresh_scheduler()
    await stop_automation_scheduler()

