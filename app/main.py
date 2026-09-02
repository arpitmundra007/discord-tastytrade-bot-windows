"""
FastAPI app - the real control panel. GET / serves a live dashboard that
handles everything: first-run setup (Discord + Tastytrade credentials),
live risk tuning, and monitoring - all over the same origin, no CORS needed
since it's one process on your machine talking to itself.
"""
from __future__ import annotations
import csv
import io
import os
import platform
import subprocess
import sys
import threading
import time
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse
from typing import Literal

from pydantic import BaseModel

from app.config import settings, update_risk_settings, update_credentials, is_configured, is_vault_active, clear_discord_credentials, clear_tastytrade_credentials
from app.browser_launcher import maybe_open_dashboard, SKIP_ENV_VAR
from app.db import get_recent_trades, get_open_positions, get_recent_positions
from app.position_manager import position_manager
from app.quote_prewarmer import quote_prewarmer
from app.risk_engine import evaluate
from app.runtime_state import is_paused, set_paused, uptime_seconds, get_discord_error, get_position_alert, set_position_alert, get_discord_identity
from app.signal_parser import parse_signal
from app.tastytrade_client import tastytrade_client

app = FastAPI(title="Discord -> Tastytrade Signal Bot")

DASHBOARD_PATH = Path(__file__).parent / "static" / "dashboard.html"


@app.on_event("startup")
async def _on_startup():
    maybe_open_dashboard()



class SignalIn(BaseModel):
    text: str


class SettingsIn(BaseModel):
    dry_run: bool | None = None
    max_slippage_pct: float | None = None
    default_contracts: int | None = None
    max_contracts_hard_cap: int | None = None
    take_profit_pct: float | None = None
    stop_loss_pct: float | None = None
    size_tag_map: dict | None = None
    sizing_mode: str | None = None
    budget_usd: float | None = None
    entry_order_type: str | None = None
    stop_order_type: str | None = None
    partial_tp_enabled: bool | None = None
    partial_tp_pct: float | None = None
    runner_tp_pct: float | None = None
    check_buying_power_before_order: bool | None = None
    exit_mode: Literal["own", "channel"] | None = None
    prewarm_enabled: bool | None = None
    prewarm_symbols: list[str] | None = None
    prewarm_max_dte: int | None = None


class CredentialsIn(BaseModel):
    discord_user_token: str | None = None
    # Pydantic correctly parses a JSON string like "1492522026299298024" into
    # an exact Python int (arbitrary precision - verified). What it CANNOT
    # recover from is a value that arrived as a JSON *number* already rounded
    # by a client's own JS Number() conversion before the request was even
    # sent - Discord snowflake IDs (18-19 digits) exceed Number's safe
    # integer range (~16 digits). The dashboard sends these as strings for
    # exactly this reason - any other client hitting this endpoint needs to
    # do the same, or the ID silently corrupts before this code ever runs.
    discord_signal_channel_ids: list[int] | None = None
    tt_env: str | None = None
    tt_client_secret: str | None = None
    tt_refresh_token: str | None = None
    tt_account_number: str | None = None
    anthropic_api_key: str | None = None


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD_PATH.read_text(encoding="utf-8")


@app.get("/health")
async def health():
    return {"status": "ok", "dry_run": settings.dry_run}


@app.get("/api/status")
async def api_status():
    return {
        "configured": is_configured(),
        "secure_storage_active": is_vault_active(),
        "dry_run": settings.dry_run,
        "paused": is_paused(),
        "stream_connected": tastytrade_client.is_stream_connected(),
        "tastytrade_error": tastytrade_client.get_last_error(),
        "discord_error": get_discord_error(),
        "discord_identity": get_discord_identity(),
        "discord_connected": bool(settings.discord_user_token) and get_discord_error() is None and get_discord_identity() is not None,
        "tt_account_nickname": tastytrade_client.account.nickname if tastytrade_client.account else None,
        "tt_connected": tastytrade_client.account is not None and tastytrade_client.get_last_error() is None,
        "position_stream_connected": position_manager.is_stream_connected(),
        "position_stream_error": position_manager.get_last_error(),
        "position_alert": get_position_alert(),
        "prewarm": quote_prewarmer.status(),
        "uptime_seconds": uptime_seconds(),
        # Discord channel IDs are 64-bit snowflakes (18-19 digits) - JavaScript's
        # Number type only safely represents integers up to 2^53-1 (~16 digits).
        # Sending these as bare JSON numbers means the BROWSER's own JSON.parse
        # silently rounds them (e.g. ...298024 -> ...298000), even though this
        # response correctly writes the exact value into the JSON text - the
        # corruption happens purely on the receiving end. Sending as strings
        # avoids this entirely; the dashboard's own save path was fixed to match
        # (never call Number() on one of these - see channelIds handling in
        # dashboard.html).
        "channels": [str(cid) for cid in settings.discord_signal_channel_ids],
        "tt_env": "live" if "cert" not in settings.tt_base_url else "sandbox",
        "has_discord_token": bool(settings.discord_user_token),
        "has_tt_credentials": bool(settings.tt_client_secret and settings.tt_refresh_token and settings.tt_account_number),
        "has_anthropic_key": bool(settings.anthropic_api_key),
        "tt_account_number": settings.tt_account_number,
        "risk": {
            "max_slippage_pct": settings.max_slippage_pct,
            "default_contracts": settings.default_contracts,
            "max_contracts_hard_cap": settings.max_contracts_hard_cap,
            "take_profit_pct": settings.take_profit_pct,
            "stop_loss_pct": settings.stop_loss_pct,
            "size_tag_map": settings.size_tag_map,
            "sizing_mode": settings.sizing_mode,
            "budget_usd": settings.budget_usd,
            "entry_order_type": settings.entry_order_type,
            "stop_order_type": settings.stop_order_type,
            "partial_tp_enabled": settings.partial_tp_enabled,
            "partial_tp_pct": settings.partial_tp_pct,
            "runner_tp_pct": settings.runner_tp_pct,
            "check_buying_power_before_order": settings.check_buying_power_before_order,
            "exit_mode": settings.exit_mode,
            "prewarm_enabled": settings.prewarm_enabled,
            "prewarm_symbols": settings.prewarm_symbols,
            "prewarm_max_dte": settings.prewarm_max_dte,
        },
    }


@app.post("/api/pause")
async def api_pause():
    set_paused(True)
    return {"paused": True}


@app.post("/api/resume")
async def api_resume():
    set_paused(False)
    return {"paused": False}


@app.get("/api/trades")
async def api_trades(limit: int = 50):
    return {"trades": get_recent_trades(limit)}


@app.get("/api/trades/export.csv")
async def api_trades_export():
    """
    Every trade log entry ever recorded, as a CSV - built for actually
    studying execution speed, not just glancing at the dashboard. The
    dashboard's own "Time to execute" column only shows the total via a
    hover tooltip (doesn't even work on mobile) - this instead breaks every
    timing stage out into its own column, since that's what real analysis
    (e.g. "is quote fetch consistently my bottleneck") actually needs.
    order_payload's shape varies by which code path logged the row (a live
    order, a rejection, a management-signal alert, etc.) - every field
    below is read defensively and left blank rather than raising if a
    particular row doesn't have it.
    """
    trades = get_recent_trades(limit=1_000_000)  # no real cap - this is a full export, not a dashboard page

    fieldnames = [
        "id", "timestamp", "approved", "reason",
        "symbol", "strike", "option_type", "expiration", "action", "size_tag",
        "contracts", "entry_price", "take_profit", "stop_loss",
        "posted_to_received_ms", "parse_ms", "used_llm_fallback",
        "quote_fetch_ms", "risk_eval_ms", "order_submit_ms", "order_preview_ms", "total_ms",
        "raw_signal",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()

    for t in trades:
        parsed = t.get("parsed") or {}
        payload = t.get("order_payload") or {}
        timing = payload.get("timing") or {}
        writer.writerow({
            "id": t.get("id"),
            "timestamp": t.get("ts"),
            "approved": "Yes" if t.get("approved") else "No",
            "reason": t.get("reason"),
            "symbol": parsed.get("symbol"),
            "strike": parsed.get("strike"),
            "option_type": parsed.get("option_type"),
            "expiration": parsed.get("expiration"),
            "action": parsed.get("action"),
            "size_tag": parsed.get("size_tag"),
            "contracts": payload.get("contracts"),
            "entry_price": payload.get("entry_limit"),
            "take_profit": payload.get("take_profit"),
            "stop_loss": payload.get("stop_loss"),
            "posted_to_received_ms": timing.get("posted_to_received_ms"),
            "parse_ms": timing.get("parse_ms"),
            "used_llm_fallback": timing.get("used_llm_fallback"),
            "quote_fetch_ms": timing.get("quote_fetch_ms"),
            "risk_eval_ms": timing.get("risk_eval_ms"),
            "order_submit_ms": timing.get("order_submit_ms"),
            "order_preview_ms": timing.get("order_preview_ms"),
            "total_ms": timing.get("total_ms"),
            "raw_signal": t.get("raw_signal"),
        })

    buf.seek(0)
    filename = f"trade_log_{time.strftime('%Y-%m-%d_%H%M%S')}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/positions")
async def api_positions(limit: int = 50):
    """Open positions currently being tracked by the partial-TP/breakeven
    state machine, plus recent history (closed ones included) for context."""
    return {"open": get_open_positions(), "recent": get_recent_positions(limit)}


@app.post("/api/positions/clear-alert")
async def api_clear_position_alert():
    """Dismisses the sticky 'a position may be unprotected' dashboard
    warning - only do this after confirming directly with Tastytrade that
    the position in question is actually protected or closed."""
    set_position_alert(None)
    return {"status": "cleared"}


@app.post("/api/settings")
async def api_settings(payload: SettingsIn):
    data = {k: v for k, v in payload.model_dump().items() if v is not None}
    update_risk_settings(data)
    return {"status": "updated", "applied": data}


@app.post("/api/credentials")
async def api_credentials(payload: CredentialsIn):
    data = {k: v for k, v in payload.model_dump().items() if v is not None}
    update_credentials(data)
    return {"status": "saved - restart required to connect with new credentials"}


def _trigger_restart():
    """
    Shared restart mechanics behind /api/restart AND the two disconnect
    endpoints below - all three need the exact same "change something in
    .env, then relaunch so the new process picks it up" behavior. See the
    original /api/restart docstring (still below, on the endpoint itself)
    for why POSIX and Windows need different approaches here.
    """
    def _restart():
        time.sleep(1.0)  # give the HTTP response time to reach the browser first
        os.environ[SKIP_ENV_VAR] = "1"
        if platform.system() == "Windows":
            watcher_cmd = [
                sys.executable, "-c",
                "import time,subprocess,sys; time.sleep(2); subprocess.run(sys.argv[1:])",
                sys.executable,
            ] + sys.argv
            subprocess.Popen(
                watcher_cmd,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS,
                close_fds=True,
            )
            os._exit(0)
        else:
            os.execv(sys.executable, [sys.executable] + sys.argv)

    threading.Thread(target=_restart, daemon=True).start()


@app.post("/api/restart")
async def api_restart():
    """
    Restarts the whole process in place so it picks up .env changes.

    On POSIX (Linux/Mac), os.execv genuinely replaces the process image -
    verified working, including a clean re-read of .env by the new process.

    On Windows, os.execv is unreliable: it doesn't truly replace the process
    the way POSIX exec does, and can leave the listening socket held by the
    old process while the "new" one tries to bind the same port, causing it
    to fail silently. Instead, spawn a detached watcher process that waits
    for this process to fully exit and release the port, then launches a
    fresh one - and exit this process immediately via os._exit so the port
    frees up right away rather than waiting on any cleanup.

    Only reconstructs the original invocation correctly if you started the
    app with `python run.py` as documented, from the project's root
    directory - a different launch method needs its own restart handling.
    """
    _trigger_restart()
    return {"status": "restarting"}


@app.post("/api/disconnect-discord")
async def api_disconnect_discord():
    """
    Full Discord logout: clears the saved user token (see
    clear_discord_credentials()'s own docstring for why channel IDs are
    left alone), then restarts - the running discord.py-self client keeps
    its live connection until the process actually restarts a moment
    later, same tradeoff already accepted for every other Setup-tab
    credential change. Afterward, Setup will show no Discord token
    configured, ready for a different account's token to be pasted in.
    """
    clear_discord_credentials()
    _trigger_restart()
    return {"status": "discord disconnected - restarting"}


@app.post("/api/disconnect-tastytrade")
async def api_disconnect_tastytrade():
    """
    Full Tastytrade logout: clears the client secret, refresh token, and
    account number, then restarts. Reconnecting afterward requires a fresh
    OAuth grant at developer.tastytrade.com - see clear_tastytrade_credentials()'s
    own docstring. Does NOT cancel any live orders or close any open
    positions on Tastytrade's side - those are broker-side and entirely
    independent of whether this app has an active session; this only logs
    the app itself out.
    """
    clear_tastytrade_credentials()
    _trigger_restart()
    return {"status": "tastytrade disconnected - restarting"}


@app.post("/api/shutdown")
async def api_shutdown():
    """
    Cleanly stops the whole process - a deliberate, user-initiated stop
    (unlike restart, this doesn't relaunch). This is a hard exit rather
    than an attempt at graceful cross-thread async cleanup: the Discord
    listener runs its own asyncio event loop in a separate thread from
    this one, which makes coordinating a fully graceful shutdown across
    both non-trivial and fragile to get right reliably. Since this is a
    deliberate stop rather than a crash, and orders are fire-and-forget
    once submitted to the broker (not something this process needs to stay
    alive to track), a hard exit is an acceptable tradeoff here.
    """
    def _shutdown():
        time.sleep(0.8)  # give the HTTP response time to reach the browser first
        os._exit(0)

    threading.Thread(target=_shutdown, daemon=True).start()
    return {"status": "shutting down"}


@app.post("/api/test-broker-latency")
async def api_test_broker_latency():
    """Safe latency check - a real, read-only, authenticated call to Tastytrade
    (account balances), never an order. Use this alongside Dry Run testing to
    estimate the one leg dry-run can't measure for real: the final broker
    network round-trip after the slippage decision is already made."""
    try:
        ms = await tastytrade_client.measure_broker_latency_ms()
        return {"latency_ms": ms}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/test-execution-timing")
async def api_test_execution_timing(payload: SignalIn):
    """
    Runs a pasted-in signal through the REAL pipeline (parse -> live quote ->
    risk evaluate -> build the real order -> submit to Tastytrade), timing
    every stage - but always via Tastytrade's own dry_run=True preview for
    the final leg, regardless of the bot's current Dry Run / Live setting.
    That's a genuine network round-trip through the same auth/validation/
    margin-calculation pipeline a live order hits, with zero execution risk.
    This can NEVER place a real order, no matter what mode the bot is in.

    The one thing this can't measure: Discord's own gateway delivery delay -
    there's no real Discord message behind a pasted-in string. Everything
    from parsing onward (including the option lookup and order submission
    network calls) is the real thing, not simulated locally.
    """
    from app.discord_selfbot import time_signal_execution
    return await time_signal_execution(payload.text)


@app.post("/test-parse")
async def test_parse(payload: SignalIn):
    signal = parse_signal(payload.text)
    return {"parsed": signal.__dict__ if signal else None}


@app.post("/test-signal")
async def test_signal(payload: SignalIn):
    signal = parse_signal(payload.text)
    if signal is None:
        return {"error": "could not parse signal"}
    live_price = await tastytrade_client.get_live_price(
        signal.symbol, signal.expiration, signal.option_type, signal.strike
    )
    decision = evaluate(signal, live_price)
    return {"parsed": signal.__dict__, "live_price": live_price, "decision": decision.__dict__}


@app.post("/process")
async def process(payload: SignalIn):
    """Manual test endpoint - runs a signal through the same pipeline a real
    Discord message would."""
    from app.discord_selfbot import process_signal_text
    # No real Discord message context here since this is a manual test
    # endpoint - channel_id is just for the activity log, not routing.
    channel_id = settings.discord_signal_channel_ids[0] if settings.discord_signal_channel_ids else 0
    await process_signal_text(payload.text, channel_id)
    return {"status": "processed - check logs / trades.db"}
