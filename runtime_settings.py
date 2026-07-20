"""
Runtime-adjustable settings, separate from config.py's static defaults.

config.py values are the defaults/fallback (and what a fresh restart uses).
Anything changed via Telegram (/setstake, /setsignals) lives here instead,
so a single hourly cycle always reads the CURRENT values rather than
whatever was frozen in at process startup.

Process-local only: a Railway restart/redeploy resets these back to
config.py's defaults. That's an intentional, simple tradeoff — persisting
this to the state DB would be easy to add later if you want settings to
survive restarts too.
"""

import config

# Start as copies of the static config defaults; /setstake and /setsignals
# mutate these, trading_engine.py reads these (not config.py directly).
stake_per_trade: float = config.STAKE_PER_TRADE
num_signals: int = config.NUM_SIGNALS


def set_stake(value: float):
    global stake_per_trade
    stake_per_trade = value


def set_num_signals(value: int):
    global num_signals
    num_signals = value


def reset_to_defaults():
    global stake_per_trade, num_signals
    stake_per_trade = config.STAKE_PER_TRADE
    num_signals = config.NUM_SIGNALS
