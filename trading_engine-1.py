"""
Orchestrates one full hourly cycle:
  1. fetch real price/news data for the instrument universe (market_data.py)
  2. collect votes from 3 AI models, grounded in that data
  3. build consensus, rank, select top N
  4. for each selected signal: get a proposal, buy the binary contract
  5. schedule an outcome check exactly TRADE_DURATION_MINUTES later
  6. notify Telegram at each step

Also handles checking trades that were opened in a previous cycle (including
recovering open positions after a restart).
"""

import asyncio
import logging
from datetime import datetime, timedelta

import config
import state
import telegram_bot
import bot_commands
from deriv_client import client as deriv_client, calc_binary_pnl
from market_data import build_all_contexts
from signal_engine import collect_all_votes
from consensus import build_consensus, rank_and_select
from models import TradeRecord, ConsensusSignal

logger = logging.getLogger(__name__)


async def run_cycle():
    logger.info("=== Starting hourly cycle ===")

    if bot_commands.is_paused:
        logger.info("Bot is paused via /pause — skipping this cycle entirely (no data fetch, no trades).")
        await telegram_bot.send_message("⏸️ Cycle skipped — bot is paused. Send /resume to continue.")
        return

    # 1. Real market data
    try:
        contexts = await build_all_contexts()
    except Exception as e:
        logger.error(f"Market data fetch failed entirely: {e}")
        await telegram_bot.send_message(telegram_bot.format_error("market_data", str(e)))
        return

    usable_contexts = [c for c in contexts if c.candles or c.last_price]
    if not usable_contexts:
        logger.error("No instruments had usable price data this cycle. Skipping.")
        await telegram_bot.send_message(
            telegram_bot.format_error("market_data", "No usable price data for any instrument — skipped cycle")
        )
        return

    # 2. Collect votes (grounded in real data)
    try:
        votes = await collect_all_votes(usable_contexts)
    except Exception as e:
        logger.error(f"Vote collection failed entirely: {e}")
        await telegram_bot.send_message(telegram_bot.format_error("signal_engine", str(e)))
        return

    # 3. Consensus + ranking
    consensus_signals = build_consensus(votes)
    qualifying = [s for s in consensus_signals if s.qualifies]

    # Filter out instruments whose market is currently closed BEFORE ranking,
    # so a closed forex pair doesn't waste a trade slot — the next-best
    # qualifying signal (e.g. an always-open synthetic index) fills it instead.
    tradeable_qualifying = []
    for s in qualifying:
        try:
            if await deriv_client.is_symbol_open(s.symbol):
                tradeable_qualifying.append(s)
            else:
                logger.info(f"{s.symbol} qualified but market is closed — excluded from selection, not counted as a wasted slot")
        except Exception as e:
            logger.warning(f"Could not check market status for {s.symbol}, excluding to be safe: {e}")

    selected = rank_and_select(tradeable_qualifying)

    await telegram_bot.send_message(telegram_bot.format_cycle_summary(selected, len(qualifying)))

    if not selected:
        logger.info("No tradeable signals selected this cycle (either none qualified, or all qualifying markets were closed).")
        return

    # 4. Execute trades
    for signal in selected:
        await _open_trade(signal)


async def _open_trade(signal: ConsensusSignal):
    symbol = signal.symbol
    contract_type = (
        config.CONTRACT_TYPE_BULLISH if signal.direction.value == "bullish"
        else config.CONTRACT_TYPE_BEARISH
    )

    is_open = await deriv_client.is_symbol_open(symbol)
    if not is_open:
        logger.warning(f"Skipping {symbol}: market currently closed on Deriv")
        await telegram_bot.send_message(
            telegram_bot.format_error(symbol, "Market currently closed on Deriv — skipped")
        )
        return

    proposal = await deriv_client.get_proposal(
        symbol=symbol,
        contract_type=contract_type,
        stake=config.STAKE_PER_TRADE,
        duration_sec=config.TRADE_DURATION_MINUTES * 60,
    )
    if proposal is None:
        await telegram_bot.send_message(
            telegram_bot.format_error(symbol, f"Could not get a price proposal for {contract_type} — skipped")
        )
        return

    proposal_id = proposal.get("id")
    ask_price = float(proposal.get("ask_price", config.STAKE_PER_TRADE))
    payout = float(proposal.get("payout", 0.0))
    entry_spot = float(proposal.get("spot", 0.0))

    buy_result = await deriv_client.buy_contract(proposal_id, ask_price)
    if buy_result is None:
        await telegram_bot.send_message(
            telegram_bot.format_error(symbol, f"Buy order failed for {contract_type} contract — skipped")
        )
        return

    contract_id = str(buy_result.get("contract_id", ""))
    actual_payout = float(buy_result.get("payout", payout))
    actual_entry_spot = float(buy_result.get("start_time_spot", entry_spot)) if buy_result.get("start_time_spot") else entry_spot

    trade = TradeRecord(
        symbol=symbol,
        direction=signal.direction.value,
        contract_type=contract_type,
        deriv_contract_id=contract_id,
        entry_spot=actual_entry_spot or entry_spot,
        entry_time=datetime.utcnow(),
        stake=config.STAKE_PER_TRADE,
        payout=actual_payout,
        avg_confidence=signal.avg_confidence,
        agreeing_models=",".join(signal.agreeing_models),
        status="open",
    )
    trade_id = state.save_trade(trade)
    trade.id = trade_id

    logger.info(f"Opened trade #{trade_id}: {symbol} {contract_type} stake=${config.STAKE_PER_TRADE} "
                f"contract_id={contract_id}")
    await telegram_bot.send_message(telegram_bot.format_entry(trade, signal))

    asyncio.create_task(_schedule_check(trade_id, delay_minutes=config.TRADE_DURATION_MINUTES))


async def _schedule_check(trade_id: int, delay_minutes: float):
    # Small buffer so we check just after Deriv has settled the contract
    await asyncio.sleep(delay_minutes * 60 + 15)
    await _check_trade_outcome(trade_id)


async def _check_trade_outcome(trade_id: int):
    open_trades = {t.id: t for t in state.get_open_trades()}
    trade = open_trades.get(trade_id)
    if trade is None:
        logger.warning(f"Trade #{trade_id} not found or already closed — skipping outcome check")
        return

    status = await deriv_client.get_contract_status(trade.deriv_contract_id)
    if status is None:
        logger.error(f"Could not fetch outcome for trade #{trade_id} (contract {trade.deriv_contract_id}). "
                      f"Will retry once more shortly.")
        await telegram_bot.send_message(
            telegram_bot.format_error(
                trade.symbol,
                f"Could not fetch outcome for contract {trade.deriv_contract_id} (trade #{trade_id}). Retrying..."
            )
        )
        await asyncio.sleep(60)
        status = await deriv_client.get_contract_status(trade.deriv_contract_id)
        if status is None:
            logger.error(f"Outcome fetch failed twice for trade #{trade_id} — leaving open for manual review")
            return

    is_expired = bool(status.get("is_expired") or status.get("is_sold"))
    if not is_expired:
        # Contract hasn't settled yet (rare — clock skew, delayed settlement). Retry shortly.
        logger.info(f"Trade #{trade_id} not yet settled, rechecking in 60s")
        await asyncio.sleep(60)
        await _check_trade_outcome(trade_id)
        return

    won = bool(status.get("status") == "won") or float(status.get("profit", -1)) > 0
    exit_spot = float(status.get("exit_tick") or status.get("sell_spot") or 0.0) or None
    payout = float(status.get("payout", trade.payout or 0.0))
    pnl = float(status.get("profit", calc_binary_pnl(trade.stake, payout, won)))
    pnl_pct = (pnl / trade.stake * 100) if trade.stake else 0.0

    state.close_trade(
        trade_id=trade_id,
        exit_spot=exit_spot,
        exit_time=datetime.utcnow(),
        pnl=pnl,
        pnl_pct=pnl_pct,
        status="won" if won else "lost",
    )
    trade.exit_spot = exit_spot
    trade.pnl = pnl
    trade.pnl_pct = pnl_pct
    trade.status = "won" if won else "lost"

    logger.info(f"Settled trade #{trade_id}: {trade.symbol} {'WON' if won else 'LOST'} P&L=${pnl:.2f}")
    await telegram_bot.send_message(telegram_bot.format_exit(trade))


async def recover_open_trades():
    """
    On startup, re-arm outcome checks for any trades that were opened before
    a restart. If a trade's expiry time has already passed, check it
    immediately.
    """
    open_trades = state.get_open_trades()
    if not open_trades:
        return
    logger.info(f"Recovering {len(open_trades)} open trade(s) from previous session...")
    for trade in open_trades:
        scheduled_check = trade.entry_time + timedelta(minutes=config.TRADE_DURATION_MINUTES)
        remaining = (scheduled_check - datetime.utcnow()).total_seconds()
        if remaining <= 0:
            logger.info(f"Trade #{trade.id} should have settled already — checking now")
            asyncio.create_task(_check_trade_outcome(trade.id))
        else:
            logger.info(f"Trade #{trade.id} re-armed, checks in {remaining/60:.1f} min")
            asyncio.create_task(_schedule_check(trade.id, delay_minutes=remaining / 60))
