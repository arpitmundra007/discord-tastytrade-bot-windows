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

from app.config import settings, redact_secrets
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

# Retry delay used specifically when Tastytrade isn't connected yet (see
# _NotConnectedYet below) - deliberately much shorter than
# REFRESH_INTERVAL_SECONDS. This case is expected to resolve within
# seconds during normal startup (see the race condition explained on
# _NotConnectedYet below), not something that benefits from a 5-minute
# wait - using the long interval here would leave the dashboard showing a
# stale "Error" card for up to 5 minutes after the real problem already
# resolved, which is confirmed to have actually happened in practice.
STARTUP_RETRY_DELAY_SECONDS = 15


class _NotConnectedYet(Exception):
    """
    Raised internally by _run_once() when Tastytrade hasn't connected yet.
    Caught separately in _loop() to use STARTUP_RETRY_DELAY_SECONDS instead
    of the full REFRESH_INTERVAL_SECONDS - a distinct exception type rather
    than just checking the message string, so this fast-retry path can't
    accidentally also catch some OTHER, unrelated failure that happens to
    use similar wording.

    Why this case needs special handling: quote_prewarmer.start() is
    deliberately called BEFORE tastytrade_client.connect() in
    discord_selfbot.py's on_ready() (so a connect failure doesn't
    permanently disable the prewarmer for the process's whole life - see
    that file's own comment). But starting the background task early means
    its very first pass can genuinely run WHILE that connect() call is
    still in progress on a real account (confirmed directly, not
    theoretical: the connect() call's own network I/O is exactly the kind
    of await point that lets the newly-scheduled task get its first turn) -
    session/account are still None at that exact moment, even though the
    connection succeeds moments later. Without this fast-retry path, that
    one-time startup race would leave the dashboard showing a stale
    "Error" long after Tastytrade actually connected successfully.
    """


def _expirations_within_dte(chain_expirations, max_dte: int):
    """Every expiration Tastytrade's own chain response lists with
    days_to_expiration in [0, max_dte] - reads the real listed expirations
    rather than generating a calendar and guessing one exists for every
    day, which would break on a day with no 0DTE contract listed at all."""
    return [e for e in chain_expirations if 0 <= e.days_to_expiration <= max_dte]


async def _diagnose_market_data_failure(symbols: list[str]) -> str:
    """
    Bypasses the tastytrade SDK's own response validation to surface the
    RAW HTTP status and body Tastytrade actually sent back - used only as
    a fallback when that validation itself crashes (see the AttributeError
    handling in _run_once() below).

    Why this is needed: the SDK's validate_response() (v13.2.2's utils.py)
    assumes any non-2xx JSON response body is a dict with an "error" key,
    then calls .get("error") on it. Confirmed directly against the SDK
    source that Tastytrade can return an error response whose JSON body is
    instead a plain STRING - triggering exactly "'str' object has no
    attribute 'get'" instead of a real error message. That's a bug in how
    the SDK explains an error, not necessarily evidence of what the
    underlying error actually is - this function re-issues the same
    request directly against the session's own underlying HTTP client
    (bypassing the SDK's broken parsing entirely) so the actual status
    code and message Tastytrade sent can be shown instead of a confusing
    Python crash.

    Mirrors tastytrade.market_data.get_market_data_by_type()'s exact
    request shape (same URL, same params) so this is a faithful
    reproduction of the request that failed, not a different one.
    """
    try:
        response = await tastytrade_client.session._client.get(
            "/market-data/by-type", params={"equity": symbols}
        )
        body_preview = response.text[:300]
        return f"HTTP {response.status_code}: {body_preview}"
    except Exception as diag_e:
        return f"(couldn't get a raw response for diagnosis either: {diag_e})"


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
        """Call once at startup (see discord_selfbot.py's on_ready(),
        which calls this BEFORE attempting the Tastytrade connection, not
        after - so a connect failure can't permanently disable this
        background task for the process's whole life). _run_once() itself
        checks for a live Tastytrade session and handles "not connected
        yet" gracefully with a fast retry (see _NotConnectedYet), so it's
        safe to start this before a connection exists."""
        if self._task is None:
            self._task = asyncio.create_task(self._loop())

    async def _loop(self):
        while True:
            if settings.prewarm_enabled:
                try:
                    await self._run_once()
                except asyncio.CancelledError:
                    raise
                except _NotConnectedYet as e:
                    # Expected during normal startup (see _NotConnectedYet's
                    # own docstring) - retries quickly rather than waiting
                    # the full refresh interval, so a real, successful
                    # connection doesn't leave the dashboard showing a
                    # stale-looking error for minutes after the actual
                    # problem already resolved.
                    self._last_error = redact_secrets(str(e))
                    await asyncio.sleep(STARTUP_RETRY_DELAY_SECONDS)
                    continue
                except Exception as e:
                    self._last_error = redact_secrets(str(e))
                    log.exception("Quote prewarming pass failed - live signals still work normally, "
                                  "they just won't benefit from a warm cache until this recovers")
            await asyncio.sleep(REFRESH_INTERVAL_SECONDS)

    async def _run_once(self):
        t_start = time.monotonic()
        symbols = list(settings.prewarm_symbols)
        if not symbols:
            return

        if tastytrade_client.session is None or tastytrade_client.account is None:
            # Not an error condition worth alarming over - just means
            # Tastytrade hasn't connected yet (startup still in progress,
            # or a connection attempt failed - check the Live tab's own
            # Tastytrade error banner for why). Raised as an exception
            # (rather than silently returning) so it's still visible in
            # status() as a clear, specific last_error instead of the card
            # just looking perpetually stuck on "Warming up..." with no
            # explanation - but as _NotConnectedYet specifically, so
            # _loop() retries quickly instead of waiting the full refresh
            # interval (see _NotConnectedYet's own docstring for why that
            # distinction matters).
            raise _NotConnectedYet(
                "Tastytrade isn't connected yet, so there's no session to fetch quotes with. "
                "This will resolve on its own once Tastytrade connects - check the Live tab's "
                "Tastytrade connection status/error for why it hasn't yet."
            )

        try:
            underlying_data = await get_market_data_by_type(tastytrade_client.session, equities=symbols)
        except AttributeError as e:
            # Confirmed: a bug in the tastytrade SDK's own error-response
            # parsing (see _diagnose_market_data_failure's docstring) -
            # this is the SDK crashing while trying to explain an error,
            # not the error message itself. Falls back to a raw request
            # that bypasses the SDK's broken parsing, so the actual
            # HTTP status/body Tastytrade sent is visible here instead.
            diagnosis = await _diagnose_market_data_failure(symbols)
            raise RuntimeError(
                f"Tastytrade's client library hit a known parsing bug trying to report an "
                f"error for {symbols} (not something wrong in this app's own code) - the "
                f"actual response was {diagnosis}. If that mentions funding, approval, or "
                f"entitlement, Tastytrade is restricting market data until your account "
                f"status is resolved - check tastytrade.com. This will retry automatically "
                f"in {REFRESH_INTERVAL_SECONDS // 60} minutes regardless."
            ) from e
        except Exception as e:
            raise RuntimeError(
                f"Couldn't fetch underlying prices for {symbols} from Tastytrade: {e}. If your "
                f"account is unfunded or not yet approved for market data, Tastytrade may be "
                f"restricting this until that's resolved on their end - check your account "
                f"status at tastytrade.com. This will retry automatically in "
                f"{REFRESH_INTERVAL_SECONDS // 60} minutes regardless."
            ) from e

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
