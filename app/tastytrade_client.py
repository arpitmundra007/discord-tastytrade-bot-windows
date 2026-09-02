"""
Wraps the `tastytrade` (tastyware) SDK: session auth, a PERSISTENT live-quote
stream, and OTOCO (entry + take-profit + stop-loss) order construction/
submission.

Verified against tastytrade SDK v13.2.2 (pip install tastytrade), which
diverged from the method names shown in some older examples/docs:
  - Account.a_get()          -> Account.get(session, account_number=...)
  - account.a_place_complex_order() -> account.place_complex_order()
  - Option.get_option(...)   -> Option.get(session, occ_symbol_string),
    plus an explicit option.set_streamer_symbol() call before reading
    option.streamer_symbol (it's not auto-populated)
  - Session(...) takes is_test: bool, not a base URL string - TT_BASE_URL
    from .env is translated to is_test here based on whether it contains
    "cert"
  - NewComplexOrder(...) defaults to type=ComplexOrderType.OCO if not set
    explicitly - silently wrong for a trigger_order+orders OTOCO structure,
    so `type=ComplexOrderType.OTOCO` is now passed explicitly
This was verified by actually constructing a NewComplexOrder with real
field values and confirming it validates and serializes correctly
(including auto-computed price-effect Credit/Debit), not just import-checked.

NOTE: NewOrder/NewComplexOrder are marked deprecated in v13.2.2 in favor of
OTOCOOrder and similar dedicated classes - still functional and verified
working here, but worth migrating to the newer classes if a future SDK
version removes the old ones.
If you're on a different SDK version, re-check these against
https://tastyworks-api.readthedocs.io/ - method names have moved before and
may move again.

--- Why persistent, and what that costs ---
Opening a fresh DXLink streaming connection per signal (the original version
of this file) costs 1-3+ seconds of handshake/subscribe/first-event latency
on every single trade - the dominant source of delay in the whole pipeline.
Keeping ONE connection open for the life of the process removes that cost
for every signal after the first, but trades it for a new problem: a
long-lived connection can silently die (network blip, server-side restart,
auth token expiry) and, unless you handle that, every signal after the drop
just times out waiting for a quote that will never arrive. Everything below
marked "reconnect" exists to handle that failure mode automatically instead
of requiring a manual restart.
"""
from __future__ import annotations
import asyncio
import logging
import time
from datetime import date
from decimal import Decimal

from tastytrade import Session, Account, DXLinkStreamer
from tastytrade.dxfeed import Quote
from tastytrade.instruments import Option
from tastytrade.order import NewOrder, NewComplexOrder, OrderAction, OrderTimeInForce, OrderType, ComplexOrderType

from app.config import settings
from app.signal_parser import Action, ParsedSignal

log = logging.getLogger("tastytrade_client")

# Reuse a cached quote if it's fresher than this - avoids re-waiting on a new
# event for rapid-fire signals on the same symbol.
QUOTE_MAX_AGE_SECONDS = 3.0
# How long to wait for a fresh quote on first subscribe before giving up.
QUOTE_WAIT_TIMEOUT_SECONDS = 2.0
# Reconnect backoff after a stream failure.
RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 30.0


def occ_symbol(symbol: str, expiration: date, option_type: str, strike: float) -> str:
    """OCC-style option symbol, e.g. SPY   260804P00731000. Used both for the
    dry-run/log preview AND as the actual symbol string passed to
    Option.get() when fetching quotes and building live orders - so a bug
    here doesn't just show up as a wrong log line, it breaks order lookup."""
    root = symbol.ljust(6)
    yy = expiration.strftime("%y%m%d")
    strike_int = int(round(strike * 1000))
    return f"{root}{yy}{option_type}{strike_int:08d}"


def _split_tp_sl_orders(orders: list) -> tuple[object | None, object | None]:
    """Given the two child orders back from a placed OTOCO/OCO complex order,
    returns (take_profit_order, stop_loss_order) identified by order_type
    (TP is always LIMIT; SL is STOP or STOP_LIMIT) rather than by list
    position - submission order isn't a documented guarantee of response
    order, so this is the reliable way to tell them apart."""
    tp_order = None
    sl_order = None
    for o in orders:
        if o.order_type in (OrderType.STOP, OrderType.STOP_LIMIT):
            sl_order = o
        else:
            tp_order = o
    return tp_order, sl_order


def average_fill_price(placed_order) -> float | None:
    """Volume-weighted average fill price across all legs/fills of a
    PlacedOrder that's reached status FILLED - the real execution price,
    which can differ from the limit price that was requested. Returns None
    if the order has no fill data yet (shouldn't happen once status is
    actually FILLED, but defensive since it's coming from a live feed)."""
    total_qty = Decimal(0)
    total_value = Decimal(0)
    for leg in placed_order.legs or []:
        for fill in leg.fills or []:
            qty = Decimal(str(fill.quantity))
            price = Decimal(str(fill.fill_price))
            total_qty += qty
            total_value += qty * price
    if total_qty == 0:
        return None
    return float(total_value / total_qty)


def _opening_and_closing_actions(action: "Action") -> tuple[OrderAction, OrderAction]:
    """
    Explicit map for the only two actions that can ever legitimately reach
    order CONSTRUCTION code (building a brand-new bracket, or closing an
    existing position that this app itself opened) - BUY_TO_OPEN or
    SELL_TO_OPEN. Raises for anything else rather than guessing, because the
    previous version of this logic (`action.value.startswith("Buy")`) was a
    real, dangerous bug: "Buy to Close".startswith("Buy") is True, so a
    BUY_TO_CLOSE signal would have silently become a brand-new BUY_TO_OPEN
    order, and "Sell to Close" fell through to the `else` branch and became
    a brand-new SELL_TO_OPEN (naked short) order - either way, a "close"
    instruction would have opened a fresh, wrong-direction position instead
    of closing anything. process_signal_text() in discord_selfbot.py is the
    primary guard (refuses to hand a non-opening action to this module at
    all), but this stays strict here too, on the principle that this module
    shouldn't depend on every caller getting that right forever.
    """
    if action == Action.BUY_TO_OPEN:
        return OrderAction.BUY_TO_OPEN, OrderAction.SELL_TO_CLOSE
    if action == Action.SELL_TO_OPEN:
        return OrderAction.SELL_TO_OPEN, OrderAction.BUY_TO_CLOSE
    raise ValueError(f"Refusing to build an opening order for action={action!r} - only BUY_TO_OPEN/SELL_TO_OPEN can open a new position or be closed by this module.")


class TastytradeClient:
    def __init__(self):
        self._session: Session | None = None
        self._account: Account | None = None

        self._streamer: DXLinkStreamer | None = None
        self._stream_task: asyncio.Task | None = None
        self._stream_ready = asyncio.Event()  # set once the streamer is connected and usable
        self._last_error: str | None = None

        self._quotes: dict[str, tuple[Quote, float]] = {}  # streamer_symbol -> (quote, received_at)
        self._quote_events: dict[str, asyncio.Event] = {}
        self._subscribed: set[str] = set()  # symbols to (re)subscribe to, survives reconnects
        self._option_cache: dict[str, Option] = {}  # occ_symbol -> Option instance, see _get_option()

    async def warm_option_cache(self, occ_sym: str) -> None:
        """
        Public entry point for background pre-fetching (see
        app/quote_prewarmer.py) - just calls _get_option() and discards the
        result. Identical caching behavior to the internal path
        get_live_price()/submit_bracket_order() already use, so a contract
        warmed here is indistinguishable to later code from one a real
        signal already touched: once cached, _get_option() never re-fetches
        it (see that method's own docstring), so a successful warm-up pass
        removes this contract's REST round trip for every signal for the
        rest of the process's life, not just the next one.
        """
        await self._get_option(occ_sym)

    async def subscribe_many(self, streamer_symbols: list[str]) -> None:
        """
        Batch version of _ensure_subscribed() - registers many streamer
        symbols in ONE DXLink subscribe call instead of one network
        round-trip per symbol. Meant for bulk background warm-up (see
        app/quote_prewarmer.py); a single live signal still uses
        _ensure_subscribed() via get_live_price(), since there's only ever
        one symbol to add per signal and no batching to gain there.

        Symbols already subscribed are skipped automatically - safe to call
        repeatedly with an overlapping list across warm-up passes.
        """
        new_symbols = []
        for sym in streamer_symbols:
            self._quote_events.setdefault(sym, asyncio.Event())
            if sym not in self._subscribed:
                self._subscribed.add(sym)
                new_symbols.append(sym)
        if not new_symbols:
            return
        if self._stream_ready.is_set() and self._streamer:
            await self._streamer.subscribe(Quote, new_symbols)
        # If the stream isn't connected right now, _stream_manager's own
        # (re)connect path already resubscribes everything in
        # self._subscribed - these are covered automatically, same as a
        # single _ensure_subscribed() call during a disconnect.

    async def _get_option(self, occ_sym: str) -> Option:
        """
        Option.get() is a real REST round-trip, and a single signal's
        lifecycle can otherwise call it 2-3 times for the exact same
        contract (get_live_price, submit_bracket_order, and later
        submit_partial_exit_oco if it gets that far) - pure waste, since an
        instrument's definition doesn't change between those calls. Cached
        for the life of this process (not time-limited) - an OCC symbol
        encodes the exact contract (strike/expiration/right), so there's
        nothing about it that could go stale.
        """
        cached = self._option_cache.get(occ_sym)
        if cached is not None:
            return cached
        option = await Option.get(self._session, occ_sym)
        self._option_cache[occ_sym] = option
        return option

    # ---------- lifecycle ----------

    async def connect(self):
        """Authenticate and fetch the account. Call once at startup."""
        try:
            is_test = "cert" in settings.tt_base_url
            self._session = Session(settings.tt_client_secret, settings.tt_refresh_token, is_test=is_test)
            # Tastytrade requires a User-Agent formatted as <app-name>/<version> -
            # without it, requests can fail with misleading auth errors that
            # have nothing to do with the credentials themselves. The SDK's
            # Session() doesn't expose a way to pass this through its
            # constructor (it already sets its own `headers` kwarg internally,
            # so passing headers=... there collides and raises), so it's set
            # directly on the underlying httpx client instead.
            self._session._client.headers.update({"User-Agent": "discord-tastytrade-bot/1.0"})
            self._account = await Account.get(self._session, account_number=settings.tt_account_number)
            self._last_error = None
        except Exception as e:
            self._last_error = f"Tastytrade connect failed: {e}"
            log.exception("Tastytrade connect() failed - check TT_CLIENT_SECRET, TT_REFRESH_TOKEN, TT_ACCOUNT_NUMBER, and TT_BASE_URL")
            raise

    def start_streaming(self):
        """Starts the persistent, self-healing quote stream in the background. Call once at startup, after connect()."""
        if self._stream_task is None:
            self._stream_task = asyncio.create_task(self._stream_manager())

    async def close(self):
        if self._stream_task:
            self._stream_task.cancel()
        if self._streamer:
            try:
                await self._streamer.__aexit__(None, None, None)
            except Exception:
                pass

    # ---------- persistent stream with auto-reconnect ----------

    async def _stream_manager(self):
        """
        Owns the DXLink connection for the process lifetime. On any failure,
        closes cleanly, backs off, and reconnects - resubscribing to every
        symbol that was active before the drop. This is the piece that makes
        "persistent" safe to run unattended instead of degrading silently.
        """
        delay = RECONNECT_BASE_DELAY
        while True:
            try:
                self._stream_ready.clear()
                # NOTE: verify DXLinkStreamer's constructor/entry signature against
                # your installed SDK version - this opens it as a long-lived context
                # manager rather than the one-shot `async with` used per-call before.
                self._streamer = DXLinkStreamer(self._session)
                await self._streamer.__aenter__()

                if self._subscribed:
                    await self._streamer.subscribe(Quote, list(self._subscribed))

                log.info("DXLink stream connected (%d symbols resubscribed)", len(self._subscribed))
                self._stream_ready.set()
                delay = RECONNECT_BASE_DELAY  # reset backoff after a clean connect

                async for quote in self._streamer.listen(Quote):
                    sym = quote.event_symbol
                    self._quotes[sym] = (quote, time.monotonic())
                    event = self._quote_events.get(sym)
                    if event:
                        event.set()

                # listen() returning at all means the stream ended - treat as a drop
                log.warning("DXLink stream ended unexpectedly, reconnecting")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._last_error = f"DXLink stream error: {e}"
                log.exception("DXLink stream error, reconnecting in %.1fs", delay)

            self._stream_ready.clear()
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY)

    def get_last_error(self) -> str | None:
        return self._last_error

    # ---------- accessors for other modules that need the raw session/account ----------
    # (position_manager.py needs these for its own AlertStreamer connection and
    # for get_order()/get_live_orders() reconciliation calls)

    @property
    def session(self):
        return self._session

    @property
    def account(self):
        return self._account

    async def _ensure_subscribed(self, streamer_symbol: str):
        self._quote_events.setdefault(streamer_symbol, asyncio.Event())
        if streamer_symbol in self._subscribed:
            return
        self._subscribed.add(streamer_symbol)
        if self._stream_ready.is_set() and self._streamer:
            await self._streamer.subscribe(Quote, [streamer_symbol])
        # if the stream isn't connected right now, _stream_manager will pick up
        # this symbol on its next (re)connect since it's already in self._subscribed

    def is_stream_connected(self) -> bool:
        return self._stream_ready.is_set()

    async def measure_broker_latency_ms(self) -> float:
        """
        Round-trip time to a real, read-only, authenticated Tastytrade
        endpoint (account balances) - hits the same live API infrastructure
        an order submission would, without ever placing an order. Use this
        as an estimate for the one leg dry-run testing can't measure for
        real: the final broker network round-trip after the slippage
        decision is already made.
        """
        if self._account is None:
            raise RuntimeError("Not connected to Tastytrade yet - check the Setup tab and the Quote Stream status on the Live tab first.")
        start = time.monotonic()
        await self._account.get_balances(self._session)
        return round((time.monotonic() - start) * 1000, 1)

    async def get_derivative_buying_power(self) -> float:
        """Real, current options buying power on the account - NOT the same
        thing as settings.budget_usd, which is just a number you configured
        and has no connection to the account until this call is made. Used
        as a pre-flight check before submitting an order so a too-large
        budget produces a clean, specific rejection instead of a raw broker
        error after a live submission attempt."""
        balances = await self._account.get_balances(self._session)
        return float(balances.derivative_buying_power)

    # ---------- public API used by the risk engine / bot ----------

    async def get_live_price(self, symbol: str, expiration: date, option_type: str, strike: float) -> float:
        t0 = time.monotonic()
        option = await self._get_option(occ_symbol(symbol, expiration, option_type, strike))
        option_lookup_ms = round((time.monotonic() - t0) * 1000, 1)

        option.set_streamer_symbol()
        streamer_symbol = option.streamer_symbol
        t1 = time.monotonic()
        await self._ensure_subscribed(streamer_symbol)
        subscribe_ms = round((time.monotonic() - t1) * 1000, 1)

        cached = self._quotes.get(streamer_symbol)
        wait_ms = 0.0
        used_cache = False
        if cached and (time.monotonic() - cached[1]) < QUOTE_MAX_AGE_SECONDS:
            quote = cached[0]
            used_cache = True
        else:
            event = self._quote_events[streamer_symbol]
            event.clear()
            t2 = time.monotonic()
            try:
                await asyncio.wait_for(event.wait(), timeout=QUOTE_WAIT_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                if streamer_symbol not in self._quotes:
                    wait_ms = round((time.monotonic() - t2) * 1000, 1)
                    log.warning("get_live_price(%s): option_lookup_ms=%.1f subscribe_ms=%.1f wait_ms=%.1f (TIMED OUT, no quote at all)",
                                streamer_symbol, option_lookup_ms, subscribe_ms, wait_ms)
                    raise RuntimeError(
                        f"No quote received for {streamer_symbol} within {QUOTE_WAIT_TIMEOUT_SECONDS}s "
                        f"(stream_ready={self._stream_ready.is_set()})"
                    )
            wait_ms = round((time.monotonic() - t2) * 1000, 1)
            quote = self._quotes[streamer_symbol][0]

        total_ms = round((time.monotonic() - t0) * 1000, 1)
        # Logged unconditionally (not just on slow outliers) since knowing
        # the split between these three legs on EVERY call is exactly what
        # tells you whether a slow quote fetch is an option-lookup problem,
        # a fresh-subscription problem, or genuinely waiting on the feed
        # itself to deliver a tick - three very different things to fix.
        log.info("get_live_price(%s): option_lookup_ms=%.1f subscribe_ms=%.1f wait_ms=%.1f used_cache=%s total_ms=%.1f",
                  streamer_symbol, option_lookup_ms, subscribe_ms, wait_ms, used_cache, total_ms)

        bid = float(quote.bid_price or 0)
        ask = float(quote.ask_price or 0)
        if bid and ask:
            return round((bid + ask) / 2, 2)
        return float(bid or ask or 0)

    async def _preflight_check(self, order) -> None:
        """
        Submits `order` as a dry_run=True preview - no real order is placed -
        and raises a clean, specific error if Tastytrade's own margin engine
        says it wouldn't be accepted (insufficient buying power, or any other
        broker-side validation failure). Deliberately uses Tastytrade's own
        computed buying-power effect rather than estimating cost as
        entry_price * contracts * 100 here - that formula is only correct
        for a long option's premium debit; short options require a real
        margin calculation this app has no business re-implementing.
        """
        preview = await self._account.place_complex_order(self._session, order, dry_run=True)
        if preview.errors:
            raise RuntimeError("; ".join(m.message for m in preview.errors))
        bp = preview.buying_power_effect
        if bp is not None and bp.new_buying_power is not None and float(bp.new_buying_power) < 0:
            raise RuntimeError(
                f"Insufficient buying power: this order needs ${float(bp.change_in_buying_power):.2f} "
                f"but only ${float(bp.current_buying_power):.2f} is available"
            )

    async def cancel_order(self, order_id: str) -> bool:
        """Best-effort cancel - returns False (doesn't raise) if the order
        already filled or was already cancelled by the time this runs, which
        is a normal race (not a bug) whenever this is called right as a
        manual trim instruction arrives."""
        try:
            await self._account.delete_order(self._session, int(order_id))
            return True
        except Exception:
            log.warning("Couldn't cancel order %s - likely already filled/cancelled", order_id, exc_info=True)
            return False

    async def close_position_at_market(self, signal: ParsedSignal, contracts: int) -> dict:
        """
        Single-leg MARKET order to close `contracts` of an existing position
        right now, at whatever the current price is - used for a manual
        "trim now" instruction from the channel, which gives no specific
        price target to work toward (unlike the bot's own take-profit
        levels, which are limit orders at a computed price). Market
        guarantees the fill actually happens; the tradeoff is the same one
        already made for entry_order_type="market" - no price protection at
        the moment of submission.
        """
        option = await self._get_option(
            occ_symbol(signal.symbol, signal.expiration, signal.option_type, signal.strike)
        )
        opening_action, closing_action = _opening_and_closing_actions(signal.action)
        leg = option.build_leg(Decimal(contracts), closing_action)
        order = NewOrder(time_in_force=OrderTimeInForce.DAY, order_type=OrderType.MARKET, legs=[leg])
        result = await self._account.place_order(self._session, order, dry_run=False)
        return {"order_id": str(result.order.id), "contracts": contracts}

    async def submit_bracket_order(
        self,
        signal: ParsedSignal,
        contracts: int,
        entry_price: float,
        take_profit_price: float,
        stop_loss_price: float,
        dry_run: bool = True,
        tp_contracts: int | None = None,
        simulate_only: bool = False,
    ) -> dict:
        """
        Submits entry + OTOCO(take-profit, stop-loss). The stop-loss leg is
        always sized for the FULL `contracts` (it has to protect the whole
        position until/unless a partial take-profit carves part of it off).
        The take-profit leg defaults to the same full size, but pass
        `tp_contracts` < contracts for a partial-take-profit setup (see
        position_manager.py) - the remainder stays completely unprotected
        by this order alone; something else has to submit a follow-up order
        for it once this TP leg fills, which is exactly what
        submit_partial_exit_oco() below is for.

        `simulate_only=True` only matters when `dry_run=True` (the app's own
        setting, from the Live tab kill switch / DRY_RUN env var) - normally
        that returns immediately after building the order, with NO network
        call for the order-submission leg at all (by design - Discord-
        triggered signals shouldn't pay that latency or risk touching the
        account while testing parsing/risk logic). Pass simulate_only=True
        to instead submit the exact same order to Tastytrade's own
        dry_run=True preview endpoint - a real network round-trip through
        the same auth/validation/margin-calculation pipeline a live order
        would hit, with zero execution risk (Tastytrade guarantees dry_run
        never places a real order) - see time_signal_execution() in
        discord_selfbot.py, which is the only caller that sets this.
        """
        tp_contracts = contracts if tp_contracts is None else tp_contracts
        option_lookup_start = time.monotonic()
        option = await self._get_option(
            occ_symbol(signal.symbol, signal.expiration, signal.option_type, signal.strike)
        )
        option_lookup_ms = round((time.monotonic() - option_lookup_start) * 1000, 1)

        opening_action, closing_action = _opening_and_closing_actions(signal.action)

        opening_leg = option.build_leg(Decimal(contracts), opening_action)
        tp_leg = option.build_leg(Decimal(tp_contracts), closing_action)
        sl_leg = option.build_leg(Decimal(contracts), closing_action)

        entry_price_signed = Decimal(str(-entry_price)) if opening_action == OrderAction.BUY_TO_OPEN else Decimal(str(entry_price))

        # Entry leg: Limit protects against paying worse than entry_price but
        # may not fill at all if the market moves away first. Market
        # guarantees a fill but gives up all price protection at the moment
        # the slippage check already passed - configurable since this is a
        # real tradeoff, not a clear-cut default.
        if settings.entry_order_type == "market":
            trigger_order = NewOrder(
                time_in_force=OrderTimeInForce.DAY,
                order_type=OrderType.MARKET,
                legs=[opening_leg],
            )
        else:
            trigger_order = NewOrder(
                time_in_force=OrderTimeInForce.DAY,
                order_type=OrderType.LIMIT,
                legs=[opening_leg],
                price=entry_price_signed,
            )

        # Stop-loss leg: "stop" (pure stop-market) has NO price field - once
        # triggered it becomes a market order, guaranteeing the position
        # actually closes at the cost of uncertain fill price. "stop_limit"
        # bounds the fill price but risks not filling at all in a gap,
        # leaving the position open and unprotected - usually the worse
        # outcome for something meant to cap a loss, so "stop" is the default.
        if settings.stop_order_type == "stop_limit":
            stop_leg_order = NewOrder(
                time_in_force=OrderTimeInForce.GTC,
                order_type=OrderType.STOP_LIMIT,
                legs=[sl_leg],
                stop_trigger=Decimal(str(stop_loss_price)),
                price=Decimal(str(stop_loss_price)),
            )
        else:
            stop_leg_order = NewOrder(
                time_in_force=OrderTimeInForce.GTC,
                order_type=OrderType.STOP,
                legs=[sl_leg],
                stop_trigger=Decimal(str(stop_loss_price)),
            )

        otoco = NewComplexOrder(
            type=ComplexOrderType.OTOCO,
            trigger_order=trigger_order,
            orders=[
                NewOrder(
                    time_in_force=OrderTimeInForce.GTC,
                    order_type=OrderType.LIMIT,
                    legs=[tp_leg],
                    price=Decimal(str(take_profit_price)),
                ),
                stop_leg_order,
            ],
        )

        payload_preview = {
            "symbol": occ_symbol(signal.symbol, signal.expiration, signal.option_type, signal.strike),
            "contracts": contracts,
            "tp_contracts": tp_contracts,
            "entry_limit": entry_price,
            "entry_order_type": settings.entry_order_type,
            "take_profit": take_profit_price,
            "stop_loss": stop_loss_price,
            "stop_order_type": settings.stop_order_type,
            "option_lookup_ms": option_lookup_ms,
        }

        if dry_run and not simulate_only:
            return {"dry_run": True, **payload_preview}

        if dry_run and simulate_only:
            # Tastytrade's OWN dry_run flag now, not the app's - a real
            # network round-trip that validates the order and computes its
            # real buying-power effect server-side, but is guaranteed by
            # Tastytrade never to actually place anything.
            preview_start = time.monotonic()
            preview = await self._account.place_complex_order(self._session, otoco, dry_run=True)
            order_preview_ms = round((time.monotonic() - preview_start) * 1000, 1)
            bp = preview.buying_power_effect
            return {
                "dry_run": True,
                "simulated": True,
                "order_preview_ms": order_preview_ms,
                "would_be_rejected": bool(preview.errors),
                "broker_messages": [m.message for m in (preview.errors or [])] + [m.message for m in (preview.warnings or [])],
                "buying_power_required": float(bp.change_in_buying_power) if bp else None,
                "current_buying_power": float(bp.current_buying_power) if bp else None,
                **payload_preview,
            }

        if settings.check_buying_power_before_order:
            await self._preflight_check(otoco)

        result = await self._account.place_complex_order(self._session, otoco, dry_run=False)
        placed = result.complex_order
        tp_order, sl_order = _split_tp_sl_orders(placed.orders)
        return {
            "dry_run": False,
            "result": str(result),
            "complex_order_id": str(placed.id),
            "entry_order_id": str(placed.trigger_order.id) if placed.trigger_order else None,
            "tp_order_id": str(tp_order.id) if tp_order else None,
            "sl_order_id": str(sl_order.id) if sl_order else None,
            **payload_preview,
        }

    async def submit_partial_exit_oco(
        self,
        signal: ParsedSignal,
        remaining_contracts: int,
        runner_tp_price: float,
        breakeven_stop_price: float,
        dry_run: bool = True,
    ) -> dict:
        """
        The follow-up order submitted the moment the partial take-profit leg
        fills (see position_manager.py): a plain OCO (no trigger - both legs
        go live immediately) covering whatever's left of the position -
        a fresh take-profit at runner_tp_price and a stop at exact
        breakeven. There's an unavoidable gap between the partial TP filling
        and this order landing (Tastytrade has no order type that ties a
        stop-relocation to a partial fill), during which the remainder is
        unprotected - by design, per the tradeoff already agreed on.
        """
        option = await self._get_option(
            occ_symbol(signal.symbol, signal.expiration, signal.option_type, signal.strike)
        )
        opening_action, closing_action = _opening_and_closing_actions(signal.action)

        tp_leg = option.build_leg(Decimal(remaining_contracts), closing_action)
        sl_leg = option.build_leg(Decimal(remaining_contracts), closing_action)

        oco = NewComplexOrder(
            type=ComplexOrderType.OCO,
            orders=[
                NewOrder(
                    time_in_force=OrderTimeInForce.GTC,
                    order_type=OrderType.LIMIT,
                    legs=[tp_leg],
                    price=Decimal(str(runner_tp_price)),
                ),
                NewOrder(
                    time_in_force=OrderTimeInForce.GTC,
                    order_type=OrderType.STOP,
                    legs=[sl_leg],
                    stop_trigger=Decimal(str(breakeven_stop_price)),
                ),
            ],
        )

        payload_preview = {
            "symbol": occ_symbol(signal.symbol, signal.expiration, signal.option_type, signal.strike),
            "remaining_contracts": remaining_contracts,
            "runner_take_profit": runner_tp_price,
            "breakeven_stop": breakeven_stop_price,
        }

        if dry_run:
            return {"dry_run": True, **payload_preview}

        result = await self._account.place_complex_order(self._session, oco, dry_run=False)
        placed = result.complex_order
        tp_order, sl_order = _split_tp_sl_orders(placed.orders)
        return {
            "dry_run": False,
            "result": str(result),
            "complex_order_id": str(placed.id),
            "tp2_order_id": str(tp_order.id) if tp_order else None,
            "sl_breakeven_order_id": str(sl_order.id) if sl_order else None,
            **payload_preview,
        }


tastytrade_client = TastytradeClient()
