"""
Entrypoint. Wires up logging, state DB, connects to Deriv, recovers any open
positions from a previous run, then starts the hourly scheduler.

Run with:  python main.py
"""

import asyncio
import logging
import sys

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

import config
import state
import telegram_bot
import bot_commands
from deriv_client import client as deriv_client
from trading_engine import run_cycle, recover_open_trades

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")


async def main():
    problems = config.validate()
    hard_fail = [p for p in problems if "TWELVEDATA" not in p]  # missing TwelveData is a soft warning, not fatal
    if hard_fail:
        for p in hard_fail:
            logger.error(f"Config problem: {p}")
        logger.error("Fix the above (check your .env file) before running.")
        sys.exit(1)
    for p in problems:
        if "TWELVEDATA" in p:
            logger.warning(p)

    state.init_db()

    logger.info("Connecting to Deriv API...")
    await deriv_client.connect()

    await recover_open_trades()

    # Start listening for /status, /pause, /resume, /history in the background
    polling_task = asyncio.create_task(bot_commands.poll_commands())

    await telegram_bot.send_message(
        "🤖 <b>Binary options consensus bot started</b>\n"
        f"Universe: {len(config.INSTRUMENTS)} instruments (forex + synthetic indices)\n"
        f"Signals/cycle: {config.NUM_SIGNALS} | Stake: ${config.STAKE_PER_TRADE:.2f}\n"
        f"Min confidence: {config.MIN_CONFIDENCE}% | Contract duration: {config.TRADE_DURATION_MINUTES}min\n"
        f"Broker: Deriv ({'DEMO' if config.DERIV_IS_DEMO else 'REAL ⚠️'})\n"
        f"Send /start to see available commands (/status, /pause, /resume, /history)."
    )

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_cycle,
        trigger=IntervalTrigger(minutes=config.CYCLE_INTERVAL_MINUTES),
        next_run_time=None,
        id="hourly_cycle",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()

    logger.info("Scheduler started. Running first cycle immediately...")
    await run_cycle()

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down...")
        polling_task.cancel()
        scheduler.shutdown()
        await deriv_client.close()


if __name__ == "__main__":
    asyncio.run(main())
