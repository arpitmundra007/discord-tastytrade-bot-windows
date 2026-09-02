"""
Self-bot mode: reads signal channels using YOUR OWN Discord account (via its
personal token) instead of an admin-invited bot application. This works on
channels you're just a member of, without needing anyone's permission to
invite a bot - which is the whole appeal, and also exactly what Discord's
own policy prohibits:

    "Automating normal user accounts (self-bots) outside of the OAuth2/bot
    API is forbidden, and can result in account termination if found."
    - https://support.discord.com/hc/en-us/articles/115002192352

That prohibition is unconditional in Discord's own wording - it is not
limited to spam or abusive behavior, and reading messages only does not
create an exception to it. Running this means accepting that your Discord
account (all of it - every server you're in, not just the trading one) is
at risk of termination if Discord's systems flag this account as automated.
This file exists because you asked for it after that tradeoff was made
explicit - it isn't a recommendation.

Depends on `discord.py-self`, NOT `discord.py` - the two packages occupy the
same `discord` import namespace and cannot be installed in the same
environment. See requirements.txt and the README's self-bot section.

Uses your Discord USER token (not a bot token) - see the README for how to
find it. Never use a browser extension or third-party "token grabber" tool
to get it; those are a common vector for actual account-stealing malware.
Retrieve it manually via your browser's own developer tools only.
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone

import discord

from app.config import settings
from app.db import log_trade, get_open_positions
from app.llm_parser import parse_signal_with_llm, to_parsed_signal
from app.position_manager import position_manager
from app.quote_prewarmer import quote_prewarmer
from app.risk_engine import evaluate
from app.runtime_state import is_paused, set_discord_error, set_position_alert, set_discord_identity
from app.signal_parser import parse_signal, parse_management_signal, ManagementSignal, Action, title_looks_like_entry
from app.tastytrade_client import tastytrade_client

log = logging.getLogger("signal_selfbot")

client = discord.Client()

def _message_text(message: "discord.Message") -> str:
    """
    The structured parser reads lines[0]=action, lines[1]=context,
    lines[2]=leg strictly BY POSITION, not by searching for them - so
    anything placed ahead of the embed's own title/description shifts every
    position down and breaks parsing, no matter what that content actually
    is. Real messages confirmed this isn't just a hypothetical: a
    message.content of just a raw ping ("<@1531933581813878844>") was
    silently becoming lines[0] ahead of the real embed title. Fixing only
    that ONE case (a "pure ping") would still leave the same bug for a ping
    plus other text, unrelated commentary, or anything else non-empty in
    message.content - so the actual fix is structural: when there's an
    embed, message.content is ALWAYS appended at the very END regardless of
    what it contains, never used for positional matching at all. The
    embed's own title -> description -> fields -> footer sequence is
    Discord's own reliable, structured data and is what actually carries
    the signal; message.content on a bot-generated signal message is always
    secondary (a ping, incidental commentary, or nothing).

    The one case where message.content DOES need to be the primary text:
    no embed at all - a human (or a differently-configured bot) typing a
    signal directly, with nothing but plain content. That's the free-form
    single-line signal path in signal_parser.py, and it has no positional
    expectations to break - it searches for its pattern anywhere in the
    text, so content ordering doesn't matter for that path regardless.
    """
    if message.embeds:
        parts = []
        for embed in message.embeds:
            if embed.title:
                parts.append(embed.title)
            if embed.description:
                parts.append(embed.description)
            for f in embed.fields:
                if f.value:
                    parts.append(f.value)
            if embed.footer and embed.footer.text:
                parts.append(embed.footer.text)
        if message.content:
            parts.append(message.content)
    else:
        parts = [message.content or ""]
    return "\n".join(p for p in parts if p)


async def _try_llm_fallback(raw_text: str, channel_id: int):
    """
    Called only when the regex parser can't make sense of a message. Tries
    the LLM extractor; if it succeeds but the signal isn't something this
    app can execute yet (wrong instrument type, a stop-loss update with no
    position tracking, low confidence), logs it clearly to the dashboard's
    activity feed instead of either failing silently or guessing at a trade.
    """
    llm_sig = await parse_signal_with_llm(raw_text)
    if llm_sig is None:
        log.info("Unparseable message in channel %s (regex and LLM both failed, or no API key set): %r", channel_id, raw_text[:200])
        first_line = raw_text.strip().splitlines()[0].strip() if raw_text.strip() else ""
        if title_looks_like_entry(first_line):
            # This message's title looks like it was meant to be a new
            # entry, but nothing (regex or LLM) could extract a tradable
            # signal from it - possibly missing required data in the
            # original post itself (confirmed to happen for real - see
            # signal_parser.py's module notes). Rather than let this vanish
            # with only a server-console log line, surface it clearly so a
            # human can check whether it needs manual action - silently
            # losing what looked like a real entry is worse than a
            # dashboard entry that turns out to be a false alarm.
            msg = (f'A "{first_line}" message came in that looked like a new entry, but neither the parser nor '
                   f"the LLM fallback could extract a complete, tradable signal from it - check it manually: {raw_text[:200]!r}")
            log_trade(raw_text, None, approved=False, reason=msg, order_payload={"channel_id": channel_id})
        return None

    log.info("LLM-parsed signal from channel %s: %s (%s)", channel_id, llm_sig, llm_sig.reasoning)
    gap = llm_sig.get_execution_gap()
    if gap:
        log.info("LLM-parsed signal not actioned: %s", gap)
        log_trade(raw_text, vars(llm_sig), approved=False, reason=f"LLM-parsed but not executable: {gap}", order_payload=None)
        return None

    parsed = to_parsed_signal(llm_sig)
    if parsed is None:
        # Defensive - get_execution_gap() should have already caught anything
        # that would land here, but never silently drop a signal without a
        # visible reason if it somehow does.
        log.warning("LLM signal passed execution-gap check but conversion still failed: %s", llm_sig)
        log_trade(raw_text, vars(llm_sig), approved=False, reason="LLM-parsed signal failed conversion to a tradable order", order_payload=None)
        return None

    return parsed


def _reject_if_not_an_opening_signal(signal, raw_text: str, channel_id: int) -> bool:
    """
    The actual safety boundary for the "never enter on an update" behavior -
    checked once, right after ANY parser produces a signal (regex, free-form,
    or LLM), rather than trusting each individual parser to have gotten this
    right on its own. Returns True if this signal was rejected (caller
    should stop processing).

    This exists because a plain string check used to determine order
    direction downstream ("Buy to Close".startswith("Buy")) was silently
    wrong for close-type actions - see _opening_and_closing_actions() in
    tastytrade_client.py for the full story. Even though signal_parser.py's
    _title_implies_update() and llm_parser.py's get_execution_gap() both
    already try to keep a close/update signal from ever reaching this point,
    this is the last line of defense: nothing downstream of this function
    ever sees an action other than BUY_TO_OPEN or SELL_TO_OPEN.
    """
    if signal.action in (Action.BUY_TO_OPEN, Action.SELL_TO_OPEN):
        return False
    msg = (f"Received a {signal.action.value} signal for {signal.symbol} {signal.strike}{signal.option_type} "
           f"{signal.expiration} - this app only opens new positions from a signal (BUY_TO_OPEN/SELL_TO_OPEN); "
           f"closing/updating an existing position is handled separately (see position management) and was NOT "
           f"acted on here to avoid opening a fresh, wrong-direction position by mistake.")
    log.warning(msg)
    log_trade(raw_text, vars(signal), approved=False, reason=msg, order_payload={"channel_id": channel_id})
    return True


async def _handle_management_signal(mgmt: ManagementSignal, channel_id: int):
    """
    "Close or Trim & Set SL to BE" messages trigger real, automatic
    execution via position_manager.handle_manual_trim() - but ONLY when all
    of these hold:
      - settings.exit_mode == "channel" (the "own" mode never auto-executes
        these, by explicit user choice - the bot's own resting bracket and
        automatic partial-TP-to-breakeven system are the only things that
        move a position in that mode)
      - the contract matches a position this bot itself tracks as open
      - that position is in a phase a trim actually makes sense for -
        "bracket1_live" (still on its initial bracket) or "partial_filled"
        (already past one trim, whether that trim was the bot's own
        automatic partial-TP or a previous manual one - real channel history
        showed sequential trims on the same position, so a second/third
        call is expected, each sized against whatever's CURRENTLY
        remaining, not the original entry size)
      - the title is specifically "close or trim" - NOT "Trade Update -
        Manage your risk", which reads as a loss/stop-out REPORT ("Sorry
        Guys" + steep negative P/L in the one real example seen) rather
        than an instruction to trim a specific fraction - applying the
        take-some-off-and-move-to-breakeven formula to what looks like a
        losing position doesn't match the channel's evident intent, so this
        title stays alert-only until real examples say otherwise.
    Every other case (wrong mode, no match, wrong phase, wrong title) stays
    logged permanently and raised as a sticky dashboard alert, no automatic
    action, since there's nothing safe to do automatically.
    """
    matching_position = None
    for p in get_open_positions():
        if (p["symbol"] == mgmt.symbol and p["strike"] == mgmt.strike
                and p["option_type"] == mgmt.option_type and p["expiration"] == mgmt.expiration.isoformat()):
            matching_position = p
            break

    is_trim_title = "close or trim" in mgmt.title.lower()
    trimmable_phase = matching_position is not None and matching_position["phase"] in ("bracket1_live", "partial_filled")

    if is_paused() and settings.exit_mode == "channel" and matching_position is not None and is_trim_title and trimmable_phase:
        # Kill switch stops ALL automated order actions, not just new
        # entries - a trim that would otherwise have executed is skipped
        # and clearly logged as such, not silently dropped.
        msg = (f'Trim NOT executed (trading paused via kill switch): "{mgmt.instruction_text}" for '
               f"{mgmt.symbol} {mgmt.strike}{mgmt.option_type} {mgmt.expiration} (position #{matching_position['id']}).")
        log.warning(msg)
        set_position_alert(msg)
        log_trade(mgmt.raw_text, vars(mgmt), approved=False, reason=msg,
                  order_payload={"matched_position_id": matching_position["id"], "channel_id": channel_id})
        return

    if settings.exit_mode == "channel" and matching_position is not None and is_trim_title and trimmable_phase:
        outcome = await position_manager.handle_manual_trim(matching_position, mgmt.instruction_text)
        msg = (f'Trim executed: "{mgmt.instruction_text}" for {mgmt.symbol} {mgmt.strike}{mgmt.option_type} '
               f"{mgmt.expiration} (position #{matching_position['id']}) \u2014 P/L at signal time was "
               f"{mgmt.pnl_pct:+.2f}% (${mgmt.pnl_dollars:+.2f}). {outcome}")
        log.info(msg)
        log_trade(mgmt.raw_text, vars(mgmt), approved=True, reason=msg,
                  order_payload={"matched_position_id": matching_position["id"], "channel_id": channel_id})
        return

    if settings.exit_mode != "channel":
        position_note = 'exit mode is set to "own" - channel trim/close instructions are never auto-executed in this mode (see the Risk tab)'
    elif matching_position is not None and is_trim_title:
        position_note = f"tracked position #{matching_position['id']} exists, but it's in phase {matching_position['phase']!r}, which a trim can't act on (already closed, needs attention, or mid-fill)"
    elif matching_position is not None:
        position_note = f"tracked position #{matching_position['id']} found, but this title (\"{mgmt.title}\") isn't treated as an executable trim instruction - only \"Close or Trim & Set SL to BE\" is"
    else:
        position_note = "no tracked open position found for this contract (bot may not have placed this entry itself, or it's already closed)"

    msg = (f'Manual action needed: "{mgmt.instruction_text}" received for {mgmt.symbol} {mgmt.strike}{mgmt.option_type} '
           f"{mgmt.expiration} \u2014 P/L {mgmt.pnl_pct:+.2f}% (${mgmt.pnl_dollars:+.2f}). {position_note}. "
           f"No automatic action was taken \u2014 handle this manually in Tastytrade.")
    log.warning(msg)
    set_position_alert(msg)
    log_trade(mgmt.raw_text, vars(mgmt), approved=False, reason=msg,
              order_payload={"matched_position_id": matching_position["id"] if matching_position else None, "channel_id": channel_id})


async def process_signal_text(raw_text: str, channel_id: int, posted_at: "datetime | None" = None):
    """
    posted_at: Discord's own server timestamp for when the message was
    actually posted (message.created_at), not just when our handler started
    running. Passed through from on_message so latency measurements reflect
    true end-to-end time, including Discord's own gateway delivery delay -
    not just this app's internal processing time. Falls back to "now" for
    calls with no real Discord message behind them (e.g. the /process
    manual test endpoint).
    """
    t_received = datetime.now(timezone.utc)
    posted_at = posted_at or t_received

    signal = parse_signal(raw_text)
    t_parsed = datetime.now(timezone.utc)
    used_llm = False
    if signal is None:
        mgmt_signal = parse_management_signal(raw_text)
        if mgmt_signal is not None:
            await _handle_management_signal(mgmt_signal, channel_id)
            return
        signal = await _try_llm_fallback(raw_text, channel_id)
        t_parsed = datetime.now(timezone.utc)
        used_llm = True
        if signal is None:
            return

    log.info("Parsed signal from channel %s: %s", channel_id, signal)

    if _reject_if_not_an_opening_signal(signal, raw_text, channel_id):
        return

    # Checked here - AFTER confirming this is a real, executable entry
    # signal - rather than before parsing, so pausing during a busy/chatty
    # period doesn't flood the trade log with "paused" entries for messages
    # that were never signals to begin with (commentary, recaps, etc). Only
    # genuine skipped signals get logged.
    if is_paused():
        timing = {"parse_ms": round((t_parsed - t_received).total_seconds() * 1000), "used_llm_fallback": used_llm}
        log.info("Trading paused via dashboard kill switch - not executing parsed signal from channel %s: %s", channel_id, signal)
        log_trade(raw_text, signal.__dict__, approved=False,
                  reason="Trading paused (kill switch) - signal was parsed but not executed",
                  order_payload={"timing": timing})
        return

    try:
        live_price = await tastytrade_client.get_live_price(
            signal.symbol, signal.expiration, signal.option_type, signal.strike
        )
    except Exception as e:
        t_failed = datetime.now(timezone.utc)
        timing = {
            "parse_ms": round((t_parsed - t_received).total_seconds() * 1000),
            "used_llm_fallback": used_llm,
            "quote_fetch_ms": round((t_failed - t_parsed).total_seconds() * 1000),
            "total_ms": round((t_failed - posted_at).total_seconds() * 1000),
        }
        log.exception("Live quote fetch failed for %s", signal)
        log_trade(raw_text, signal.__dict__, approved=False,
                  reason=f"Couldn't fetch a live quote: {e}", order_payload={"timing": timing})
        return
    t_quoted = datetime.now(timezone.utc)

    decision = evaluate(signal, live_price)
    t_evaluated = datetime.now(timezone.utc)

    timing = {
        "posted_to_received_ms": round((t_received - posted_at).total_seconds() * 1000),
        "parse_ms": round((t_parsed - t_received).total_seconds() * 1000),
        "used_llm_fallback": used_llm,
        "quote_fetch_ms": round((t_quoted - t_parsed).total_seconds() * 1000),
        "risk_eval_ms": round((t_evaluated - t_quoted).total_seconds() * 1000),
    }

    if not decision.approved:
        timing["total_ms"] = round((datetime.now(timezone.utc) - posted_at).total_seconds() * 1000)
        log.warning("Signal rejected: %s | timing=%s", decision.reason, timing)
        log_trade(raw_text, signal.__dict__, approved=False, reason=decision.reason, order_payload={"timing": timing})
        return

    try:
        order_result = await tastytrade_client.submit_bracket_order(
            signal,
            contracts=decision.contracts,
            entry_price=decision.entry_limit_price,
            take_profit_price=decision.take_profit_price,
            stop_loss_price=decision.stop_loss_price,
            dry_run=settings.dry_run,
            tp_contracts=decision.partial_contracts if settings.partial_tp_enabled else None,
        )
    except Exception as e:
        # A broker-side rejection (insufficient buying power, a symbol/expiry
        # it won't accept, account restrictions, etc) raises here rather than
        # returning a normal result - without this catch, that exception was
        # only ever caught by on_message's own try/except, which logs it to
        # the server console and nowhere else. That meant a rejected order
        # was invisible on the dashboard - indistinguishable from "signal
        # never arrived" instead of "signal arrived, broker said no." This
        # makes sure a broker rejection always lands in the trade log.
        timing["total_ms"] = round((datetime.now(timezone.utc) - posted_at).total_seconds() * 1000)
        log.exception("Order submission failed for %s", signal)
        # approved=False here even though risk checks passed - the dashboard
        # renders `approved` as a green/red badge, and a trade that never
        # actually executed has to render red, or it reads as a false
        # success at a glance.
        log_trade(raw_text, signal.__dict__, approved=False,
                  reason=f"Broker rejected the order: {e}", order_payload={"timing": timing})
        return
    t_ordered = datetime.now(timezone.utc)
    timing["order_submit_ms"] = round((t_ordered - t_evaluated).total_seconds() * 1000)
    timing["total_ms"] = round((t_ordered - posted_at).total_seconds() * 1000)

    log.info("Order result: %s | timing=%s", order_result, timing)
    order_result = {**order_result, "timing": timing}
    log_trade(raw_text, signal.__dict__, approved=True, reason=decision.reason, order_payload=order_result)

    if order_result.get("dry_run") is False:
        # Only real, submitted orders get tracked for the partial-TP /
        # move-to-breakeven state machine - dry-run previews have no real
        # order IDs to watch fills on.
        position_manager.open_position(signal, decision, order_result, raw_text, channel_id)


async def time_signal_execution(raw_text: str) -> dict:
    """
    Runs a raw signal through the exact same pipeline a real Discord message
    would (parse -> live quote -> risk evaluate -> build the real order ->
    submit it to Tastytrade), timing every stage - but ALWAYS forces
    Tastytrade's own dry_run=True preview for the final leg, regardless of
    the app's current Dry Run / Live setting. That's a real network
    round-trip through the same auth/validation/margin-calculation pipeline
    a live order hits, with zero execution risk - see submit_bracket_order's
    `simulate_only` param. This can never place a real order no matter what
    mode the bot itself is in, and never calls log_trade() or
    position_manager.open_position() - it's a measurement, not a signal.

    The one thing this can't measure: Discord's own gateway delivery delay
    (process_signal_text's posted_to_received_ms) - there's no real Discord
    message behind a pasted-in test string. Everything from parsing onward
    is the real thing.
    """
    t_start = datetime.now(timezone.utc)

    signal = parse_signal(raw_text)
    t_parsed = datetime.now(timezone.utc)
    used_llm = False
    if signal is None:
        signal = await _try_llm_fallback(raw_text, channel_id=0)
        t_parsed = datetime.now(timezone.utc)
        used_llm = True
        if signal is None:
            return {"error": "Could not parse this signal (regex or LLM fallback)", "timing": {"parse_ms": round((t_parsed - t_start).total_seconds() * 1000)}}

    timing = {"parse_ms": round((t_parsed - t_start).total_seconds() * 1000), "used_llm_fallback": used_llm}

    if signal.action not in (Action.BUY_TO_OPEN, Action.SELL_TO_OPEN):
        return {"error": f"This parsed as a {signal.action.value} signal, not a new entry - this app never "
                          f"acts on update/close signals as if they were new positions (see "
                          f"_reject_if_not_an_opening_signal in discord_selfbot.py).", "timing": timing}

    try:
        live_price = await tastytrade_client.get_live_price(
            signal.symbol, signal.expiration, signal.option_type, signal.strike
        )
    except Exception as e:
        return {"error": f"Couldn't fetch a live quote: {e}", "timing": timing,
                "signal": {"symbol": signal.symbol, "strike": signal.strike, "option_type": signal.option_type, "expiration": str(signal.expiration)}}
    t_quoted = datetime.now(timezone.utc)
    timing["quote_fetch_ms"] = round((t_quoted - t_parsed).total_seconds() * 1000)

    decision = evaluate(signal, live_price)
    t_evaluated = datetime.now(timezone.utc)
    timing["risk_eval_ms"] = round((t_evaluated - t_quoted).total_seconds() * 1000)

    signal_summary = {
        "symbol": signal.symbol, "strike": signal.strike, "option_type": signal.option_type,
        "expiration": str(signal.expiration), "is_mid_price": signal.is_mid_price,
    }

    if not decision.approved:
        timing["total_ms"] = round((datetime.now(timezone.utc) - t_start).total_seconds() * 1000)
        return {"approved": False, "reason": decision.reason, "timing": timing, "signal": signal_summary}

    try:
        order_result = await tastytrade_client.submit_bracket_order(
            signal,
            contracts=decision.contracts,
            entry_price=decision.entry_limit_price,
            take_profit_price=decision.take_profit_price,
            stop_loss_price=decision.stop_loss_price,
            dry_run=True,        # always - see simulate_only doc, this never depends on settings.dry_run
            simulate_only=True,  # forces a real Tastytrade dry_run=True network round-trip, never a real order
            tp_contracts=decision.partial_contracts if settings.partial_tp_enabled else None,
        )
    except Exception as e:
        timing["total_ms"] = round((datetime.now(timezone.utc) - t_start).total_seconds() * 1000)
        return {"approved": True, "error": f"Order preview call itself failed: {e}", "timing": timing,
                "signal": signal_summary, "decision": {"contracts": decision.contracts, "entry_limit_price": decision.entry_limit_price}}
    t_ordered = datetime.now(timezone.utc)
    timing["order_preview_ms"] = round((t_ordered - t_evaluated).total_seconds() * 1000)
    timing["total_ms"] = round((t_ordered - t_start).total_seconds() * 1000)

    return {
        "approved": True,
        "signal": signal_summary,
        "decision": {
            "contracts": decision.contracts,
            "entry_limit_price": decision.entry_limit_price,
            "take_profit_price": decision.take_profit_price,
            "stop_loss_price": decision.stop_loss_price,
            "slippage_pct": decision.slippage_pct,
        },
        "order_preview": order_result,
        "timing": timing,
        "note": "Tastytrade dry_run preview only - no real order was placed, regardless of the bot's current Dry Run / Live setting.",
    }


@client.event
async def on_ready():
    log.warning(
        "SELF-BOT MODE ACTIVE - logged in as %s using a personal account token. "
        "This account is at risk of termination per Discord's own ToS regardless "
        "of read-only usage. Monitoring channel IDs: %s",
        client.user, settings.discord_signal_channel_ids,
    )
    set_discord_error(None)  # a fresh, successful connect - clear any stale error from a previous attempt
    set_discord_identity(str(client.user) if client.user else None)
    try:
        await tastytrade_client.connect()
        tastytrade_client.start_streaming()
    except Exception:
        log.error(
            "Tastytrade connection failed on startup - Discord listener is still running, "
            "but no orders will work until this is fixed and the app is restarted. "
            "Check TT_CLIENT_SECRET / TT_REFRESH_TOKEN / TT_ACCOUNT_NUMBER / TT_BASE_URL."
        )
        return  # position_manager.start() needs a working connection - no point trying it if the above failed

    # Runs in the background from here on (see app/quote_prewarmer.py) -
    # never blocks startup and never affects order placement if it fails;
    # worst case a signal on an un-warmed contract just pays the normal
    # per-signal quote-fetch cost it always would have.
    try:
        quote_prewarmer.start()
    except Exception:
        log.exception("Quote prewarmer failed to start - live signals still work normally, just without the warm-cache speed-up")
    try:
        await position_manager.start()
    except Exception as e:
        # Distinct from tastytrade_client's own error state (get_last_error()
        # above may show "connected fine") - this is specifically "connected
        # OK, but reconciling/starting the position tracker itself failed,"
        # which needs its own visible reason rather than looking like a
        # healthy connection with silently-broken position tracking.
        log.exception("position_manager.start() failed on startup")
        set_discord_error(f"Connected to Tastytrade, but starting the position tracker failed: {e}")


@client.event
async def on_disconnect():
    log.warning("Discord gateway connection dropped - discord.py will attempt to reconnect automatically")
    set_discord_error("Discord connection dropped - attempting to reconnect (signals will be missed until this resolves)")


@client.event
async def on_resumed():
    log.info("Discord gateway connection resumed")
    set_discord_error(None)


@client.event
async def on_error(event_method, *args, **kwargs):
    # discord.py's catch-all for an exception raised inside any other event
    # handler (including on_message, though that has its own try/except
    # below too - this is the backstop for anything that isn't). Console-only
    # by design here: there's no single signal/trade this can be attributed
    # to at this level, so there's nothing meaningful to put in the trade log -
    # but it must not be allowed to vanish entirely, which is discord.py's
    # own default behavior (it normally just prints to stderr).
    log.exception("Unhandled exception in Discord event handler %r", event_method)


@client.event
async def on_message(message: "discord.Message"):
    if message.channel.id not in settings.discord_signal_channel_ids:
        return
    try:
        await process_signal_text(_message_text(message), message.channel.id, posted_at=message.created_at)
    except Exception as e:
        # Last-resort safety net: every known failure mode inside
        # process_signal_text already logs to the trade log with a specific
        # reason (parse failure, quote fetch failure, risk rejection, broker
        # rejection, etc) - this only fires for something genuinely
        # unanticipated. Still logs to the trade log rather than console-only,
        # on the principle that a signal that came in and then silently
        # vanished is worse than one flagged with an unhelpfully generic reason.
        log.exception("Unhandled error processing message %s", message.id)
        log_trade(_message_text(message), None, approved=False,
                  reason=f"Unexpected error while processing this message: {e}",
                  order_payload={"channel_id": message.channel.id})


def run():
    # No-op if run.py's configure_logging() already ran (which it always
    # does when started normally via start.bat) - logging.basicConfig() is
    # idempotent and only applies if the root logger has no handlers yet.
    # This only matters if this module is ever run standalone/directly.
    logging.basicConfig(level=logging.INFO)
    if not settings.discord_user_token:
        set_discord_error("DISCORD_USER_TOKEN is not set - see README's self-bot section")
        log.error("DISCORD_USER_TOKEN is not set - see README's self-bot section")
        return
    try:
        # No "Bot " prefix - this logs in as the user account itself.
        client.run(settings.discord_user_token)
    except Exception as e:
        set_discord_error(f"Discord login failed: {e}")
        log.error("Discord client.run() failed: %s", e)
