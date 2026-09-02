"""
Run this BEFORE `python run.py`, especially after any fresh install or
troubleshooting: python check_setup.py

Checks that every dependency actually imports and behaves as this project
expects, one at a time, so a broken environment shows you exactly which
piece is wrong in one line instead of a five-level traceback from deep
inside run.py. Exits non-zero and prints a fix hint on the first failure.
"""
import sys

CHECKS = []


def check(name):
    def decorator(fn):
        CHECKS.append((name, fn))
        return fn
    return decorator


@check("Python version")
def _python_version():
    if sys.version_info < (3, 10):
        raise RuntimeError(
            f"Python {sys.version_info.major}.{sys.version_info.minor} is too old - this project needs 3.10+ "
            f"(3.12 recommended). Recreate your venv with a newer Python."
        )
    if sys.version_info >= (3, 15):
        print(f"  (note: Python {sys.version_info.major}.{sys.version_info.minor} is very new - "
              f"if later checks fail, 3.12 has the widest tested compatibility)")


@check("discord.py-self is installed correctly")
def _discord():
    import importlib.metadata as im

    def _installed(dist_name):
        try:
            return im.version(dist_name)
        except im.PackageNotFoundError:
            return None

    bot_version = _installed("discord.py")
    selfbot_version = _installed("discord.py-self")

    if selfbot_version is None:
        raise RuntimeError(
            f"discord.py-self isn't installed "
            f"(discord.py {bot_version or 'not installed'} is what's actually present instead - "
            f"this project uses self-bot mode exclusively). "
            f"Fix: pip uninstall discord discord.py discord.py-self -y "
            f"then pip install -r requirements.txt"
        )
    if bot_version is not None:
        raise RuntimeError(
            "Both discord.py and discord.py-self are installed together - they share the same "
            "import namespace and will conflict unpredictably. Fix: pip uninstall discord.py -y"
        )


@check("pydantic / pydantic_core")
def _pydantic():
    from pydantic import BaseModel

    class _T(BaseModel):
        x: int

    _T(x=1)


@check("tastytrade SDK (and its numpy/pandas dependency chain)")
def _tastytrade():
    from tastytrade import Session, Account, DXLinkStreamer
    from tastytrade.instruments import Option
    from tastytrade.order import NewOrder, NewComplexOrder, OrderAction, OrderTimeInForce, OrderType, ComplexOrderType
    if not hasattr(Account, "get"):
        raise RuntimeError(
            "tastytrade.Account has no .get() method - this project was built against tastytrade==13.2.2's "
            "API. If you're on a different version, app/tastytrade_client.py's method calls may need updating."
        )


@check("FastAPI / uvicorn")
def _fastapi():
    import fastapi
    import uvicorn


@check("python-dotenv")
def _dotenv():
    from dotenv import load_dotenv


@check("This project's own app/ package")
def _own_app():
    from datetime import date
    from app.config import settings
    from app.signal_parser import parse_signal
    from app.risk_engine import evaluate
    from app.tastytrade_client import tastytrade_client
    from app.llm_parser import parse_signal_with_llm, to_parsed_signal
    from app.db import log_trade, get_recent_trades
    from app.runtime_state import is_paused
    from app.quote_prewarmer import quote_prewarmer
    # a real parse, not just an import, to catch any regex/logic breakage too.
    # Pin `today` to a fixed, known weekday (2026-01-05, a Monday) rather than
    # the real current date - a 0DTE signal is correctly rejected if "today"
    # falls on a weekend (an option can't expire on a Sat/Sun), so leaving
    # this on the real date meant this check would spuriously fail on any
    # actual Saturday or Sunday, regardless of whether anything was actually
    # broken.
    result = parse_signal("Buy To Open\nLOTTO SIZE / SMALL\nSPY 731P  0DTE $1.7", today=date(2026, 1, 5))
    assert result is not None and result.symbol == "SPY", "signal parser produced an unexpected result"


@check("Discord self-bot listener module")
def _listener_module():
    """Mirrors exactly what run.py imports - catches any breakage in the
    listener module itself before run.py does."""
    from app.discord_selfbot import run, process_signal_text


@check("Full FastAPI app (mirrors what run.py actually serves)")
def _full_app():
    from app.main import app
    assert len(app.routes) > 5, "suspiciously few routes registered"


def main():
    print(f"Checking environment (Python {sys.version.split()[0]})...\n")
    failed = False
    for name, fn in CHECKS:
        try:
            fn()
            print(f"  OK   {name}")
        except Exception as e:
            print(f"  FAIL {name}")
            print(f"       {e}")
            failed = True
            break  # stop at first failure - later checks likely cascade from it

    if not failed:
        try:
            from app.config import KEYRING_AVAILABLE, _keyring_set, _keyring_get
            test_key = "_check_setup_roundtrip_test"
            vault_working = KEYRING_AVAILABLE and _keyring_set(test_key, "test") and _keyring_get(test_key) == "test"
            if vault_working:
                print("  (info) Secure credential storage: ACTIVE - secrets saved via the dashboard will be encrypted in your OS credential vault, not plaintext in .env)")
            else:
                print("  (info) Secure credential storage: NOT AVAILABLE on this machine - secrets will be saved as plaintext in .env instead. The app still works fully, just with weaker protection of saved tokens.")
        except Exception:
            pass  # purely informational - never let this block startup

    print()
    if failed:
        print("Environment isn't ready yet - fix the issue above, then run this again.")
        sys.exit(1)
    else:
        print("Everything checks out. Safe to run: python run.py")


if __name__ == "__main__":
    main()
