"""
Pre-warms option quote data for a fixed watchlist of commonly-signaled
symbols, so a live Discord signal's get_live_price() call can be answered
mostly (or entirely) from cache instead of paying two network round trips
to Tastytrade's servers in Chicago - each of which costs roughly 250-350ms
one-way from outside North America. That round-trip cost, paid twice (an
option REST lookup, then a DXLink subscribe-and-wait-for-a-fresh-tick), is
the dominant piece of the ~1600ms quote-fetch time this was built to cut
down - see get_live_price()'s own option_lookup_ms/subscribe_ms/wait_ms
breakdown in tastytrade_client.py for where that time actually goes.

Two independent warm-up actions run every cycle, both PURELY ADDITIVE -
nothing here ever un-subscribes or evicts a previously-warmed contract, it
only ever adds more coverage as strikes come into range:

  1. REST: pre-populates tastytrade_client's Option object cache (keyed
     exactly the same way get_live_price()/_get_option() already key it -
     by occ_symbol()) for every near-the-money strike, both rights, across
     the configured expirations. Once an Option object is cached there, it
     is NEVER re-fetched (see _get_option()'s own docstring) - so after the
     first warm-up pass, later refreshes only need to fetch contracts that
     are NEWLY in range (price moved further than before, or a new
     expiration rolled into the 0-N DTE window at the start of a trading
     day), not the whole set again. This is what removes option_lookup_ms
     from a live signal's timing breakdown.
  2. DXLink: subscribes every one of those same contracts' streamer symbols
     on the SAME persistent quote stream tastytrade_client already
     maintains for live signals - using the streamer symbols the option
     chain endpoint already hands back directly (call_streamer_symbol /
     put_streamer_symbol), so this module never needs to compute or guess
     a streamer symbol format itself. Once subscribed, DXLink keeps
     pushing live ticks into tastytrade_client's own quote cache with no
     further action needed here - a real signal on any of these contracts
     finds an already-fresh quote sitting in cache (used automatically by
     get_live_price()'s own QUOTE_MAX_AGE_SECONDS cache check) instead of
     waiting on a brand-new tick. This is what removes subscribe_ms and
     most/all of wait_ms.

Deliberately scoped to a fixed, configurable symbol list (settings.
prewarm_symbols) and a near-the-money strike window (STRIKES_EACH_SIDE),
NOT the entire chain for each symbol - SPY alone can have 100+ strikes per
expiration, and subscribing all of them across 10 symbols and several
expirations would be tens of thousands of live subscriptions for coverage
that would almost never be used (a signal on a wildly-OTM strike outside
this window just pays the normal per-signal lookup cost, same as before
this module existed - nothing gets WORSE for an out-of-window signal, it
just doesn't get the speed-up).

Nothing in this module ever affects order placement, sizing, or risk
decisions - it only ever pre-populates read caches that get_live_price()
already checks. A bug here can make a signal slower (falls back to the
normal per-signal fetch) but can never make it wrong.
"""
from __future__ import annotations
import asyncio
import logging
import time
from datetime import date

from tastytrade.instruments import NestedOptionChain
from tastytrade.market_data import get_market_data_by_type

from app.config import settings
from app.tastytrade_client import tastytrade_client, occ_symbol

log = logging.getLogger("quote_prewarmer")

# How many strikes on EACH side of the current underlying price to
# pre-warm, per expiration, per symbol - "wide" per explicit choice, since
# real signals in this channel include further-OTM "lotto" plays, not just
# ATM. Total strikes covered per expiration is roughly 2x this (both sides),
# and both a call and a put are warmed for every strike in range, since a
# signal could be either.
STRIKES_EACH_SIDE = 20

REFRESH_INTERVAL_SECONDS = 5 * 60

# Concurrency cap on Option.get() REST calls during a warm-up pass. The
# tastytrade SDK (verified against the exact v13.2.2 source this project
# pins) has no batch endpoint for full Option objects - NestedOptionChain
# only returns strike/symbol strings, not full Option instances - so
# warming the Option cache means one REST call per contract. This caps how
# many run concurrently so the very first warm-up pass (which has the most
# ground to cover) doesn't fire thousands of simultaneous requests at once.
FETCH_CONCURRENCY = 15


def _expirations_within_dte(chain_expirations, max_dte: int):
    """Every expiration Tastytrade's own chain response lists with
    days_to_expiration in [0, max_dte] - reads the real listed expirations
    rather than generating a calendar and guessing one exists for every
    day, which would break on a day with no 0DTE contract listed at all."""
    return [e for e in chain_expirations if 0 <= e.days_to_expiration <= max_dte]


def _nearest_strikes(strikes, underlying_price: float, each_side: int):
    """The `each_side` closest listed strikes below and above the current
    underlying price - distance-based rather than a fixed dollar-width
    band, so this scales correctly for both a cheap underlying and a very
    expensive one (e.g. a name trading near $1900/share needs a much wider
    dollar range for the same strike COUNT than one trading near $20)."""
    sorted_strikes = sorted(strikes, key=lambda s: float(s.strike_price))
    below = [s for s in sorted_strikes if float(s.strike_price) <= underlying_price]
    above = [s for s in sorted_strikes if float(s.strike_price) > underlying_price]
    return below[-each_side:] + above[:each_side]


class QuotePrewarmer:
    def __init__(self):
        self._task: asyncio.Task | None = None
        self._last_run_at: float | None = None
        self._last_run_duration_ms: float | None = None
        self._last_error: str | None = None
        # occ_symbol strings already sent through Option.get() at least
        # once this process's lifetime, and streamer symbols already
        # subscribed - both grow monotonically (see module docstring: this
        # never evicts), so a later pass only needs to act on what's NOT
        # yet in these sets.
        self._option_cache_seeded: set[str] = set()
        self._streamer_symbols_subscribed: set[str] = set()

    def status(self) -> dict:
        return {
            "enabled": settings.prewarm_enabled,
            "symbols": list(settings.prewarm_symbols),
            "max_dte": settings.prewarm_max_dte,
            "last_run_at": self._last_run_at,
            "last_run_duration_ms": self._last_run_duration_ms,
            "last_error": self._last_error,
            "warmed_contracts": len(self._streamer_symbols_subscribed),
        }

    def start(self):
        """Call once at startup, after tastytrade_client.connect() and
        start_streaming() have both succeeded - this needs a live session
        for the REST chain/market-data calls and a live DXLink connection
        for subscriptions to actually take effect immediately (if the
        stream isn't ready yet, subscribe_many() still registers the
        symbols for the stream's own reconnect logic to pick up)."""
        if self._task is None:
            self._task = asyncio.create_task(self._loop())

    async def _loop(self):
        while True:
            if settings.prewarm_enabled:
                try:
                    await self._run_once()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._last_error = str(e)
                    log.exception("Quote prewarming pass failed - live signals still work normally, "
                                  "they just won't benefit from a warm cache until this recovers")
            await asyncio.sleep(REFRESH_INTERVAL_SECONDS)

    async def _run_once(self):
        t_start = time.monotonic()
        symbols = list(settings.prewarm_symbols)
        if not symbols:
            return

        underlying_data = await get_market_data_by_type(tastytrade_client.session, equities=symbols)
        underlying_prices: dict[str, float] = {}
        for md in underlying_data:
            price = md.mark if md.mark is not None else (md.last if md.last is not None else md.mid)
            if price is not None:
                underlying_prices[md.symbol] = float(price)

        missing = [s for s in symbols if s not in underlying_prices]
        if missing:
            log.warning("No underlying price available this pass for %s - skipping their strike "
                        "selection until the next refresh (%ds)", missing, REFRESH_INTERVAL_SECONDS)

        today = date.today()
        new_option_fetches: list[str] = []
        new_streamer_symbols: list[str] = []

        for symbol in symbols:
            underlying_price = underlying_prices.get(symbol)
            if underlying_price is None:
                continue
            try:
                chains = await NestedOptionChain.get(tastytrade_client.session, symbol)
            except Exception:
                log.exception("Couldn't fetch the option chain for %s this pass - skipping it", symbol)
                continue
            if not chains:
                log.warning("Tastytrade returned no option chain at all for %s - check the symbol is correct", symbol)
                continue
            chain = chains[0]

            for expiration in _expirations_within_dte(chain.expirations, settings.prewarm_max_dte):
                for strike in _nearest_strikes(expiration.strikes, underlying_price, STRIKES_EACH_SIDE):
                    for right, streamer_symbol in (("C", strike.call_streamer_symbol), ("P", strike.put_streamer_symbol)):
                        occ = occ_symbol(symbol, expiration.expiration_date, right, float(strike.strike_price))
                        if occ not in self._option_cache_seeded:
                            new_option_fetches.append(occ)
                        if streamer_symbol not in self._streamer_symbols_subscribed:
                            new_streamer_symbols.append(streamer_symbol)

        if new_streamer_symbols:
            await tastytrade_client.subscribe_many(new_streamer_symbols)
            self._streamer_symbols_subscribed.update(new_streamer_symbols)

        if new_option_fetches:
            sem = asyncio.Semaphore(FETCH_CONCURRENCY)

            async def _fetch(occ: str):
                async with sem:
                    try:
                        await tastytrade_client.warm_option_cache(occ)
                        self._option_cache_seeded.add(occ)
                    except Exception:
                        # Non-fatal per-contract - a real signal on THIS
                        # specific contract just pays the normal REST
                        # lookup cost it always would have, same as before
                        # this module existed. Not worth aborting the rest
                        # of a warm-up pass over one bad symbol/expiration.
                        log.warning("Couldn't pre-fetch Option object for %s this pass", occ, exc_info=True)

            await asyncio.gather(*(_fetch(o) for o in new_option_fetches))

        elapsed_ms = round((time.monotonic() - t_start) * 1000)
        self._last_run_at = time.time()
        self._last_run_duration_ms = elapsed_ms
        self._last_error = None
        log.info(
            "Quote prewarm pass complete in %dms - %d new option object(s) cached, %d new subscription(s) "
            "(running totals: %d contracts cached, %d subscriptions live)",
            elapsed_ms, len(new_option_fetches), len(new_streamer_symbols),
            len(self._option_cache_seeded), len(self._streamer_symbols_subscribed),
        )


quote_prewarmer = QuotePrewarmer()
