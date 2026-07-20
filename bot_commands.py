"""
Handles incoming Telegram commands via long-polling (getUpdates), so the
bot can actually respond to /status, /pause, /resume, /history instead of
being notify-only.

This runs as a background asyncio task alongside the hourly scheduler in
main.py. It does NOT use webhooks (no public HTTPS endpoint needed) —
long-polling works fine for a single-user bot like this and needs zero
extra infrastructure.

Pause state is a simple in-memory flag checked by trading_engine.run_cycle()
before it opens any new trades. Pausing does NOT cancel outcome-checks for
already-open trades — those always resolve normally.
"""

import asyncio
import logging
from typing import Optional

import httpx

import config
import state
import telegram_bot

logger = logging.getLogger(__name__)

# Shared pause flag — checked by trading_engine.run_cycle().
# Module-level and process-local: if the bot restarts, it resumes un-paused
# (Railway doesn't persist this across deploys, only within one running process).
is_paused = False

_API_BASE = f"https://api.telegram.org/bot{{token}}"


def _label(symbol: str) -> str:
    return config.INSTRUMENT_LABELS.get(symbol, symbol)


async def _get_updates(client: httpx.AsyncClient, offset: Optional[int]) -> list:
    url = _API_BASE.format(token=config.TELEGRAM_BOT_TOKEN) + "/getUpdates"
    params = {"timeout": 25}
    if offset is not None:
        params["offset"] = offset
    try:
        resp = await client.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            logger.error(f"getUpdates failed: {resp.status_code} {resp.text}")
            return []
        return resp.json().get("result", [])
    except httpx.ReadTimeout:
        return []
    except Exception as e:
        logger.error(f"getUpdates exception: {e}")
        return []


def _format_status() -> str:
    open_trades = state.get_open_trades()
    lines = [
        f"{'⏸️ PAUSED' if is_paused else '▶️ RUNNING'} — new trades "
        f"{'will NOT' if is_paused else 'will'} be opened next cycle.",
        "",
        f"Open positions: {len(open_trades)}",
    ]
    for t in open_trades[:10]:
        lines.append(
            f"  • {_label(t.symbol)} {t.direction.upper()} "
            f"stake ${t.stake:.2f}, opened {t.entry_time.strftime('%H:%M UTC')}"
        )
    if len(open_trades) > 10:
        lines.append(f"  ...and {len(open_trades) - 10} more")
    return "\n".join(lines)


def _format_history(limit: int = 10) -> str:
    recent = state.get_recent_trades(limit=limit)
    closed = [t for t in recent if t.status in ("won", "lost")]
    if not closed:
        return "No closed trades yet."
    lines = [f"📜 <b>Last {len(closed)} closed trade(s)</b>\n"]
    wins = sum(1 for t in closed if t.status == "won")
    total_pnl = sum(t.pnl or 0 for t in closed)
    lines.append(f"Win rate: {wins}/{len(closed)} | Total P&L: ${total_pnl:+.2f}\n")
    for t in closed:
        emoji = "✅" if t.status == "won" else "❌"
        lines.append(
            f"{emoji} {_label(t.symbol)} {t.direction.upper()} "
            f"${t.pnl:+.2f} ({t.exit_time.strftime('%m/%d %H:%M UTC') if t.exit_time else 'n/a'})"
        )
    return "\n".join(lines)


async def _handle_command(client: httpx.AsyncClient, chat_id: str, text: str):
    global is_paused
    command = text.strip().lower().split()[0] if text.strip() else ""

    if command == "/status":
        await telegram_bot.send_message(_format_status())
    elif command == "/pause":
        is_paused = True
        await telegram_bot.send_message("⏸️ Paused. No new trades will open until you send /resume. Any already-open trades will still settle normally.")
    elif command == "/resume":
        is_paused = False
        await telegram_bot.send_message("▶️ Resumed. New trades will open on the next hourly cycle.")
    elif command == "/history":
        await telegram_bot.send_message(_format_history())
    elif command == "/start":
        await telegram_bot.send_message(
            "🤖 Bot commands:\n"
            "/status — show pause state + open positions\n"
            "/pause — stop opening new trades\n"
            "/resume — resume opening new trades\n"
            "/history — last 10 closed trades + win rate"
        )
    # Unrecognized text is silently ignored — no need to spam replies to
    # random messages sent to the bot.


async def poll_commands():
    """
    Long-running background task: polls Telegram for new messages and
    dispatches recognized commands. Runs forever until cancelled.
    """
    offset = None
    logger.info("Telegram command polling started")
    async with httpx.AsyncClient() as client:
        while True:
            try:
                updates = await _get_updates(client, offset)
                for update in updates:
                    offset = update["update_id"] + 1
                    message = update.get("message") or {}
                    chat_id = str(message.get("chat", {}).get("id", ""))
                    text = message.get("text", "")

                    # Only respond to the configured chat — ignore anyone else
                    # who might message the bot (defense in depth; the token
                    # is private, but this costs nothing to add).
                    if chat_id != str(config.TELEGRAM_CHAT_ID):
                        logger.warning(f"Ignored message from unexpected chat_id={chat_id}")
                        continue

                    if text.startswith("/"):
                        await _handle_command(client, chat_id, text)
            except asyncio.CancelledError:
                logger.info("Telegram command polling stopped")
                raise
            except Exception as e:
                logger.error(f"Command polling loop error: {e}")
                await asyncio.sleep(5)  # brief backoff before retrying
