#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import argparse
import json
import math
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
try:
    import yfinance as yf
except ModuleNotFoundError:
    yf = None

try:
    from ib_insync import IB, Option, Stock
except ModuleNotFoundError:
    IB = None
    Option = None
    Stock = None

try:
    from tabulate import tabulate
except ModuleNotFoundError:
    tabulate = None

import yaml_compat as yaml


DEFAULT_CONFIG: dict[str, Any] = {
    "data": {
        "as_of_date": None,
        "history_period": "4y",
        "benchmark": "SPY",
        "market_data_source": "yahoo",
        "cache_dir": "out",
        "metrics_filename": "latest_metrics.csv",
        "screened_filename": "latest_screen.csv",
        "resume_existing_metrics": True,
        "progress_every": 10,
        "checkpoint_every": 10,
        "ibkr": {
            "host": "127.0.0.1",
            "port": 4001,
            "client_id_base": 19000,
            "readonly": True,
            "timeout": 10,
            "market_data_wait_seconds": 2.0,
            "option_data_wait_seconds": 3.0,
        },
    },
    "universe": {
        "mode": "file",
        "path": "starter_universe.txt",
        "tickers": [],
        "limit": None,
    },
    "screen": {
        "option_dte_min": 25,
        "option_dte_max": 45,
        "option_target_dte": 35,
        "option_monthly_only": False,
        "min_otm_pct": 0.05,
        "max_otm_pct": 0.15,
        "preferred_otm_pct": 0.08,
        "min_size_metric": 10_000_000_000,
        "min_dividend_yield": 0.03,
        "min_call_annualized_yield": 0.06,
        "min_call_open_interest": 500,
        "min_call_volume": 10,
        "max_option_spread_pct": 0.08,
        "min_beta_1y": -0.25,
        "max_beta_1y": 1.20,
        "min_realized_vol_1y": 0.15,
        "max_realized_vol_1y": 0.45,
        "min_total_return_3y": -0.05,
        "max_drawdown_3y": 0.45,
        "allowed_asset_types": None,
    },
    "scoring": {
        "weights": {
            "dividend_yield": 0.25,
            "call_yield": 0.30,
            "liquidity": 0.20,
            "quality": 0.15,
            "risk_fit": 0.10,
        },
        "caps": {
            "dividend_yield": 0.08,
            "call_annualized_yield": 0.18,
            "call_open_interest": 5000,
            "total_return_3y": 0.50,
            "max_drawdown_3y": 0.45,
        },
        "tolerances": {
            "beta_1y": 0.25,
            "realized_vol_1y": 0.10,
            "option_spread_pct": 0.04,
        },
    },
    "output": {
        "top_n": 20,
        "show_failed": False,
    },
}


METRIC_COLUMNS = [
    "ticker",
    "asset_type",
    "size_metric_name",
    "size_metric",
    "price",
    "ttm_dividends",
    "dividend_yield",
    "beta_1y",
    "realized_vol_1y",
    "total_return_3y",
    "max_drawdown_3y",
    "selected_expiration",
    "selected_dte",
    "selected_strike",
    "selected_call_bid",
    "selected_call_ask",
    "selected_call_mid",
    "selected_call_open_interest",
    "selected_call_volume",
    "expiration_total_call_open_interest",
    "selected_call_otm_pct",
    "selected_call_spread_pct",
    "selected_call_yield",
    "annualized_call_yield",
    "metrics_signature",
    "as_of_date",
    "error",
]


@dataclass
class OptionSelection:
    expiration: str | None
    dte: int | None
    strike: float | None
    bid: float | None
    ask: float | None
    mid: float | None
    open_interest: float | None
    volume: float | None
    total_call_open_interest: float | None
    otm_pct: float | None
    spread_pct: float | None
    call_yield: float | None
    annualized_call_yield: float | None


def deep_merge(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        user_config = yaml.safe_load(handle) or {}
    return deep_merge(DEFAULT_CONFIG, user_config)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh market data and screen US covered-call candidates."
    )
    parser.add_argument(
        "command",
        choices=["refresh", "screen", "run"],
        help="refresh downloads fresh market metrics, screen applies filters to a saved CSV, run does both.",
    )
    parser.add_argument(
        "--config",
        default="covered_call_config.yaml",
        help="Path to the YAML config file.",
    )
    parser.add_argument(
        "--input",
        help="Override the metrics CSV used by the screen command.",
    )
    parser.add_argument(
        "--output",
        help="Override the output CSV path for the selected command.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        help="Override how many rows to print to the terminal.",
    )
    return parser.parse_args()


def parse_as_of_date(value: str | None) -> date:
    if not value:
        return datetime.now(timezone.utc).date()
    if isinstance(value, date):
        return value if not isinstance(value, datetime) else value.date()
    return date.fromisoformat(value)


def load_universe(config: dict[str, Any], base_dir: Path) -> list[str]:
    universe_cfg = config["universe"]
    mode = universe_cfg.get("mode", "file")
    limit = universe_cfg.get("limit")

    if mode == "inline":
        tickers = universe_cfg.get("tickers", [])
    elif mode == "sp500":
        tickers = load_sp500_tickers()
    elif mode == "file":
        path = base_dir / universe_cfg.get("path", "starter_universe.txt")
        tickers = load_tickers_from_file(path)
    else:
        raise ValueError(f"Unsupported universe mode: {mode}")

    normalized = normalize_tickers(tickers)
    if limit:
        normalized = normalized[: int(limit)]
    return normalized


def load_sp500_tickers() -> list[str]:
    tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    if not tables:
        raise RuntimeError("Could not load S&P 500 constituents.")
    table = tables[0]
    return [str(symbol).replace(".", "-") for symbol in table["Symbol"].tolist()]


def load_tickers_from_file(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Universe file not found: {path}")
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
        if "ticker" in df.columns:
            return df["ticker"].dropna().astype(str).tolist()
        return df.iloc[:, 0].dropna().astype(str).tolist()
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def safe_market_float(value: Any, *, allow_zero: bool = True) -> float | None:
    numeric = safe_float(value)
    if numeric is None:
        return None
    if numeric < 0:
        return None
    if not allow_zero and numeric == 0:
        return None
    return numeric


def has_data_error(row: dict[str, Any] | pd.Series) -> bool:
    error = row.get("error")
    return isinstance(error, str) and bool(error.strip())


def resolve_otm_preferences(screen_cfg: dict[str, Any]) -> tuple[float, float, float]:
    legacy_target = safe_float(screen_cfg.get("target_otm_pct"))
    min_otm = safe_float(screen_cfg.get("min_otm_pct"))
    max_otm = safe_float(screen_cfg.get("max_otm_pct"))
    preferred_otm = safe_float(screen_cfg.get("preferred_otm_pct"))

    if min_otm is None and max_otm is None:
        if legacy_target is not None:
            min_otm = legacy_target
            max_otm = legacy_target
        else:
            min_otm = 0.05
            max_otm = 0.15
    elif min_otm is None:
        min_otm = legacy_target if legacy_target is not None else max_otm
    elif max_otm is None:
        max_otm = legacy_target if legacy_target is not None else min_otm

    if min_otm is None:
        min_otm = 0.05
    if max_otm is None:
        max_otm = 0.15
    if min_otm > max_otm:
        min_otm, max_otm = max_otm, min_otm

    if preferred_otm is None:
        if legacy_target is not None and min_otm <= legacy_target <= max_otm:
            preferred_otm = legacy_target
        else:
            preferred_otm = (min_otm + max_otm) / 2.0
    preferred_otm = min(max(preferred_otm, min_otm), max_otm)
    return float(min_otm), float(max_otm), float(preferred_otm)


def build_metrics_signature(config: dict[str, Any]) -> str:
    min_otm_pct, max_otm_pct, preferred_otm_pct = resolve_otm_preferences(config["screen"])
    signature_payload = {
        "history_period": config["data"]["history_period"],
        "benchmark": config["data"]["benchmark"],
        "market_data_source": get_market_data_source(config),
        "option_dte_min": config["screen"]["option_dte_min"],
        "option_dte_max": config["screen"]["option_dte_max"],
        "option_target_dte": config["screen"]["option_target_dte"],
        "option_monthly_only": config["screen"].get("option_monthly_only", False),
        "min_otm_pct": min_otm_pct,
        "max_otm_pct": max_otm_pct,
        "preferred_otm_pct": preferred_otm_pct,
    }
    return json.dumps(signature_payload, sort_keys=True, separators=(",", ":"))


def normalize_tickers(tickers: list[str]) -> list[str]:
    normalized: list[str] = []
    for ticker in tickers:
        cleaned = str(ticker).strip().upper()
        if cleaned and cleaned not in normalized:
            normalized.append(cleaned)
    return normalized


def format_seconds(total_seconds: float) -> str:
    seconds = max(0, int(round(total_seconds)))
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def persist_metrics_rows(rows: list[dict[str, Any]], metrics_path: Path) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df.to_csv(metrics_path, index=False)
    return df


def load_reusable_metrics_rows(
    metrics_paths: list[Path],
    *,
    as_of: date,
    metrics_signature: str,
) -> dict[str, dict[str, Any]]:
    reusable_rows: dict[str, dict[str, Any]] = {}
    seen_paths: set[Path] = set()

    for metrics_path in metrics_paths:
        if metrics_path in seen_paths or not metrics_path.exists():
            continue
        seen_paths.add(metrics_path)
        existing_df = pd.read_csv(metrics_path)
        if existing_df.empty or "ticker" not in existing_df.columns:
            continue
        for _, row in existing_df.iterrows():
            ticker = str(row.get("ticker") or "").strip().upper()
            if not ticker or ticker in reusable_rows:
                continue
            if str(row.get("as_of_date") or "").strip() != as_of.isoformat():
                continue
            if str(row.get("metrics_signature") or "").strip() != metrics_signature:
                continue
            if has_data_error(row):
                continue
            reusable_rows[ticker] = row.to_dict()
    return reusable_rows


def call_with_retries(func: Any, *args: Any, **kwargs: Any) -> Any:
    delays = [5, 15, 30]
    last_error: Exception | None = None
    for attempt in range(len(delays) + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            last_error = exc
            if exc.__class__.__name__ != "YFRateLimitError" or attempt >= len(delays):
                raise
            time.sleep(delays[attempt])
    if last_error is not None:
        raise last_error
    raise RuntimeError("Unknown retry failure.")


def require_yfinance() -> Any:
    if yf is None:
        raise ModuleNotFoundError(
            "yfinance is required for refresh/market-data actions. "
            "Install it in the active environment, or use screen mode with existing metrics CSVs."
        )
    return yf


def require_ibkr() -> Any:
    if IB is None or Stock is None or Option is None:
        raise ModuleNotFoundError(
            "ib_insync is required for IBKR market-data actions. "
            "Install it in the active environment, or switch market_data_source back to yahoo."
        )
    return IB


def get_market_data_source(config: dict[str, Any]) -> str:
    source = str(config["data"].get("market_data_source", "yahoo") or "yahoo").strip().lower()
    if source not in {"yahoo", "ibkr", "hybrid"}:
        return "yahoo"
    return source


def parse_ib_duration(period: str) -> str:
    normalized = str(period or "").strip().lower()
    if not normalized:
        return "1 Y"
    if normalized.endswith("yr"):
        return f"{int(normalized[:-2])} Y"
    if normalized.endswith("y"):
        return f"{int(normalized[:-1])} Y"
    if normalized.endswith("mo"):
        return f"{int(normalized[:-2])} M"
    if normalized.endswith("w"):
        return f"{int(normalized[:-1])} W"
    if normalized.endswith("d"):
        return f"{int(normalized[:-1])} D"
    raise ValueError(f"Unsupported IB duration period: {period}")


def normalize_ib_expiration(value: str) -> str:
    cleaned = str(value).strip()
    if len(cleaned) == 8 and cleaned.isdigit():
        return f"{cleaned[:4]}-{cleaned[4:6]}-{cleaned[6:8]}"
    return cleaned


def get_ib_market_data_types(config: dict[str, Any]) -> list[int]:
    configured = config.get("data", {}).get("ibkr", {}).get("market_data_types")
    if isinstance(configured, list):
        values: list[int] = []
        for item in configured:
            try:
                value = int(item)
            except (TypeError, ValueError):
                continue
            if value not in values and value in {1, 2, 3, 4}:
                values.append(value)
        if values:
            return values
    return [1, 2, 3, 4]


def resolve_ib_option_market_data_type(
    ib: Any,
    contracts: list[Any],
    *,
    wait_seconds: float,
    market_data_types: list[int],
) -> int:
    probe_contracts = contracts[: min(3, len(contracts))]
    fallback_type = market_data_types[0] if market_data_types else 1
    for market_data_type in market_data_types or [1]:
        ib.reqMarketDataType(market_data_type)
        saw_any_data = False
        for contract in probe_contracts:
            ticker = ib.reqMktData(contract, "100,101,104,106", False, False)
            ib.sleep(wait_seconds)
            bid = safe_market_float(getattr(ticker, "bid", None), allow_zero=False)
            ask = safe_market_float(getattr(ticker, "ask", None), allow_zero=False)
            last_price = safe_market_float(getattr(ticker, "last", None), allow_zero=False)
            close_price = safe_market_float(getattr(ticker, "close", None), allow_zero=False)
            open_interest = safe_market_float(getattr(ticker, "callOpenInterest", None))
            ib.cancelMktData(contract)
            if bid is not None or ask is not None:
                return market_data_type
            if last_price is not None or close_price is not None or open_interest is not None:
                saw_any_data = True
        if saw_any_data:
            fallback_type = market_data_type
    return fallback_type


def ensure_asyncio_event_loop() -> Any:
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def connect_ibkr(config: dict[str, Any], *, client_id_offset: int = 0) -> Any:
    ib_class = require_ibkr()
    ensure_asyncio_event_loop()
    settings = config["data"].get("ibkr", {})
    client_id_base = int(settings.get("client_id_base", 19000))
    client_id = client_id_base + client_id_offset + (int(time.time() * 1000) % 1000)
    ib = ib_class()
    ib.connect(
        str(settings.get("host", "127.0.0.1")),
        int(settings.get("port", 4001)),
        clientId=client_id,
        readonly=bool(settings.get("readonly", True)),
        timeout=float(settings.get("timeout", 10)),
    )
    return ib


def normalize(value: float | None, floor: float, cap: float) -> float:
    if value is None:
        return 0.0
    if cap <= floor:
        return float(value >= floor)
    return max(0.0, min(1.0, (value - floor) / (cap - floor)))


def inverse_normalize(value: float | None, floor: float, cap: float) -> float:
    if value is None:
        return 0.0
    if cap <= floor:
        return float(value <= floor)
    return max(0.0, min(1.0, 1.0 - ((value - floor) / (cap - floor))))


def range_fit(value: float | None, lower: float, upper: float, tolerance: float) -> float:
    if value is None:
        return 0.0
    if lower <= value <= upper:
        return 1.0
    if value < lower:
        return max(0.0, 1.0 - ((lower - value) / max(tolerance, 1e-9)))
    return max(0.0, 1.0 - ((value - upper) / max(tolerance, 1e-9)))


def compute_beta(stock_returns: pd.Series, benchmark_returns: pd.Series) -> float | None:
    joined = pd.concat([stock_returns, benchmark_returns], axis=1, join="inner").dropna()
    if len(joined) < 126:
        return None
    variance = joined.iloc[:, 1].var()
    if variance == 0 or pd.isna(variance):
        return None
    return safe_float(joined.iloc[:, 0].cov(joined.iloc[:, 1]) / variance)


def compute_max_drawdown(prices: pd.Series) -> float | None:
    cleaned = prices.dropna()
    if cleaned.empty:
        return None
    drawdown = cleaned / cleaned.cummax() - 1.0
    return safe_float(abs(drawdown.min()))


def choose_expiration(
    expirations: list[str],
    as_of: date,
    min_dte: int,
    max_dte: int,
    target_dte: int,
    monthly_only: bool = False,
) -> tuple[str | None, int | None]:
    choices: list[tuple[int, str]] = []
    fallback: list[tuple[int, str]] = []
    for expiration in expirations:
        expiry_date = date.fromisoformat(expiration)
        if monthly_only and not is_standard_monthly_expiration(expiry_date):
            continue
        dte = (expiry_date - as_of).days
        if dte <= 0:
            continue
        fallback.append((dte, expiration))
        if min_dte <= dte <= max_dte:
            choices.append((dte, expiration))

    if choices:
        selected = min(choices, key=lambda item: abs(item[0] - target_dte))
        return selected[1], selected[0]
    if fallback:
        selected = min(fallback, key=lambda item: abs(item[0] - target_dte))
        return selected[1], selected[0]
    return None, None


def is_standard_monthly_expiration(expiry_date: date) -> bool:
    if expiry_date.weekday() == 4:
        return 15 <= expiry_date.day <= 21
    if expiry_date.weekday() == 3:
        shifted_friday = expiry_date + timedelta(days=1)
        return shifted_friday.weekday() == 4 and 15 <= shifted_friday.day <= 21
    return False


def select_call_option(
    calls: pd.DataFrame,
    spot: float,
    min_otm_pct: float,
    max_otm_pct: float,
    preferred_otm_pct: float,
    dte: int,
) -> OptionSelection:
    if calls.empty:
        return OptionSelection(None, None, None, None, None, None, None, None, None, None, None, None, None)

    data = calls.copy()
    data["bid"] = pd.to_numeric(data["bid"], errors="coerce")
    data["ask"] = pd.to_numeric(data["ask"], errors="coerce")
    data["lastPrice"] = pd.to_numeric(data["lastPrice"], errors="coerce")
    data["openInterest"] = pd.to_numeric(data["openInterest"], errors="coerce")
    data["volume"] = pd.to_numeric(data["volume"], errors="coerce")
    data["strike"] = pd.to_numeric(data["strike"], errors="coerce")
    data["mid"] = np.where(
        (data["bid"].fillna(0) > 0) & (data["ask"].fillna(0) > 0),
        (data["bid"] + data["ask"]) / 2.0,
        data["lastPrice"],
    )
    data["otm_pct"] = data["strike"] / spot - 1.0
    data["spread_pct"] = np.where(
        data["mid"] > 0,
        (data["ask"] - data["bid"]) / data["mid"],
        np.nan,
    )
    data["range_distance"] = np.where(
        data["otm_pct"] < min_otm_pct,
        min_otm_pct - data["otm_pct"],
        np.where(data["otm_pct"] > max_otm_pct, data["otm_pct"] - max_otm_pct, 0.0),
    )
    data["preferred_distance"] = (data["otm_pct"] - preferred_otm_pct).abs()

    candidates = data[(data["mid"] > 0) & (data["strike"] >= spot)]
    if candidates.empty:
        candidates = data[data["mid"] > 0]
    if candidates.empty:
        return OptionSelection(None, None, None, None, None, None, None, None, None, None, None, None, None)

    liquid_candidates = candidates[
        (candidates["openInterest"].fillna(0) > 0) | (candidates["volume"].fillna(0) > 0)
    ]
    if not liquid_candidates.empty:
        candidates = liquid_candidates

    selected = candidates.sort_values(
        by=["range_distance", "preferred_distance", "openInterest", "volume"],
        ascending=[True, True, False, False],
        na_position="last",
    ).iloc[0]

    call_yield = safe_float(selected["mid"] / spot)
    annualized = safe_float(call_yield * 365.0 / dte) if call_yield is not None and dte else None

    return OptionSelection(
        expiration=None,
        dte=dte,
        strike=safe_float(selected["strike"]),
        bid=safe_float(selected["bid"]),
        ask=safe_float(selected["ask"]),
        mid=safe_float(selected["mid"]),
        open_interest=safe_float(selected["openInterest"]),
        volume=safe_float(selected["volume"]),
        total_call_open_interest=safe_float(data["openInterest"].fillna(0).sum()),
        otm_pct=safe_float(selected["otm_pct"]),
        spread_pct=safe_float(selected["spread_pct"]),
        call_yield=call_yield,
        annualized_call_yield=annualized,
    )


def fetch_history(ticker: str, period: str) -> pd.DataFrame:
    yf_module = require_yfinance()
    history = call_with_retries(
        yf_module.Ticker(ticker).history,
        period=period,
        auto_adjust=False,
    )
    if history.empty:
        raise RuntimeError("No price history returned.")
    return history


def fetch_history_ib(ib: Any, ticker: str, period: str) -> pd.DataFrame:
    require_ibkr()
    contract = Stock(ticker, "SMART", "USD")
    qualified = ib.qualifyContracts(contract)
    if not qualified:
        raise RuntimeError(f"IBKR could not qualify stock contract for {ticker}.")
    bars = ib.reqHistoricalData(
        qualified[0],
        endDateTime="",
        durationStr=parse_ib_duration(period),
        barSizeSetting="1 day",
        whatToShow="TRADES",
        useRTH=True,
        formatDate=1,
    )
    if not bars:
        raise RuntimeError(f"IBKR returned no price history for {ticker}.")
    rows = []
    for bar in bars:
        rows.append(
            {
                "Date": pd.Timestamp(bar.date),
                "Close": safe_float(bar.close),
            }
        )
    history = pd.DataFrame(rows).set_index("Date")
    if history.empty or history["Close"].dropna().empty:
        raise RuntimeError(f"IBKR returned no usable close prices for {ticker}.")
    return history


def fetch_ticker_info_yahoo(ticker: str) -> dict[str, Any]:
    if yf is None:
        return {}
    try:
        return call_with_retries(require_yfinance().Ticker(ticker).get_info) or {}
    except Exception:
        return {}


def choose_ib_option_chain(chains: list[Any], symbol: str) -> Any:
    if not chains:
        raise RuntimeError(f"IBKR returned no option chains for {symbol}.")
    preferred = [chain for chain in chains if chain.exchange == "SMART" and chain.tradingClass == symbol]
    if not preferred:
        preferred = [chain for chain in chains if chain.exchange == "SMART"]
    if not preferred:
        preferred = list(chains)
    return max(
        preferred,
        key=lambda chain: (len(getattr(chain, "expirations", [])), len(getattr(chain, "strikes", []))),
    )


def fetch_stock_snapshot_ib(
    ib: Any,
    ticker: str,
    *,
    config: dict[str, Any],
    wait_seconds: float,
) -> tuple[Any, float | None, float | None]:
    require_ibkr()
    contract = Stock(ticker, "SMART", "USD")
    qualified = ib.qualifyContracts(contract)
    if not qualified:
        raise RuntimeError(f"IBKR could not qualify stock contract for {ticker}.")
    stock_contract = qualified[0]
    market_price = None
    ttm_dividends = None
    for market_data_type in get_ib_market_data_types(config):
        ib.reqMarketDataType(market_data_type)
        market_data = ib.reqMktData(stock_contract, "456", False, False)
        ib.sleep(wait_seconds)
        market_price = safe_market_float(market_data.marketPrice(), allow_zero=False)
        if market_price is None:
            market_price = safe_market_float(market_data.last, allow_zero=False)
        if market_price is None:
            market_price = safe_market_float(market_data.close, allow_zero=False)
        dividends = getattr(market_data, "dividends", None)
        ttm_dividends = safe_float(getattr(dividends, "past12Months", None)) if dividends is not None else None
        ib.cancelMktData(stock_contract)
        if market_price is not None:
            break
    return stock_contract, market_price, ttm_dividends


def fetch_option_selection_ib(
    ib: Any,
    stock_contract: Any,
    spot: float,
    as_of: date,
    screen_cfg: dict[str, Any],
    *,
    config: dict[str, Any],
    wait_seconds: float,
) -> OptionSelection:
    require_ibkr()
    min_otm_pct, max_otm_pct, preferred_otm_pct = resolve_otm_preferences(screen_cfg)
    chains = ib.reqSecDefOptParams(
        stock_contract.symbol,
        "",
        stock_contract.secType,
        stock_contract.conId,
    )
    option_chain = choose_ib_option_chain(list(chains), stock_contract.symbol)
    expirations = [normalize_ib_expiration(value) for value in sorted(option_chain.expirations)]
    expiration, dte = choose_expiration(
        expirations=expirations,
        as_of=as_of,
        min_dte=int(screen_cfg["option_dte_min"]),
        max_dte=int(screen_cfg["option_dte_max"]),
        target_dte=int(screen_cfg["option_target_dte"]),
        monthly_only=bool(screen_cfg.get("option_monthly_only", False)),
    )
    if not expiration or not dte:
        return OptionSelection(None, None, None, None, None, None, None, None, None, None, None, None, None)

    ib_expiration = expiration.replace("-", "")
    option_template = Option(
        stock_contract.symbol,
        ib_expiration,
        0.0,
        "C",
        "SMART",
        tradingClass=getattr(option_chain, "tradingClass", stock_contract.symbol),
        multiplier=str(getattr(option_chain, "multiplier", "100") or "100"),
    )
    contract_details = list(ib.reqContractDetails(option_template))
    valid_contracts = [
        detail.contract
        for detail in contract_details
        if getattr(detail.contract, "right", None) == "C" and safe_float(getattr(detail.contract, "strike", None))
    ]
    if not valid_contracts:
        return OptionSelection(None, None, None, None, None, None, None, None, None, None, None, None, None)

    contracts_above_spot = [
        contract for contract in valid_contracts if safe_float(getattr(contract, "strike", None)) is not None and contract.strike >= spot
    ]
    candidate_contracts = contracts_above_spot or valid_contracts
    candidate_contracts = sorted(
        candidate_contracts,
        key=lambda contract: (
            max(
                0.0,
                min_otm_pct - (float(contract.strike) / spot - 1.0),
                (float(contract.strike) / spot - 1.0) - max_otm_pct,
            ),
            abs((float(contract.strike) / spot - 1.0) - preferred_otm_pct),
            float(contract.strike),
        ),
    )[:12]

    chosen_market_data_type = resolve_ib_option_market_data_type(
        ib,
        candidate_contracts,
        wait_seconds=wait_seconds,
        market_data_types=get_ib_market_data_types(config),
    )
    ib.reqMarketDataType(chosen_market_data_type)
    tickers = [ib.reqMktData(contract, "100,101", False, False) for contract in candidate_contracts]
    ib.sleep(wait_seconds)
    rows: list[dict[str, Any]] = []
    for contract, ticker in zip(candidate_contracts, tickers):
        bid = safe_market_float(getattr(ticker, "bid", None), allow_zero=False)
        ask = safe_market_float(getattr(ticker, "ask", None), allow_zero=False)
        last_price = safe_market_float(getattr(ticker, "last", None), allow_zero=False)
        if last_price is None:
            last_price = safe_market_float(getattr(ticker, "close", None), allow_zero=False)
        rows.append(
            {
                "strike": safe_float(getattr(contract, "strike", None)),
                "bid": bid,
                "ask": ask,
                "lastPrice": last_price,
                "openInterest": safe_market_float(getattr(ticker, "callOpenInterest", None)),
                "volume": safe_market_float(getattr(ticker, "volume", None)),
            }
        )
        ib.cancelMktData(contract)

    calls = pd.DataFrame(rows)
    selection = select_call_option(
        calls=calls,
        spot=spot,
        min_otm_pct=min_otm_pct,
        max_otm_pct=max_otm_pct,
        preferred_otm_pct=preferred_otm_pct,
        dte=dte,
    )
    selection.expiration = expiration
    return selection


def fetch_option_selection(
    ticker_obj: yf.Ticker,
    spot: float,
    as_of: date,
    screen_cfg: dict[str, Any],
) -> OptionSelection:
    min_otm_pct, max_otm_pct, preferred_otm_pct = resolve_otm_preferences(screen_cfg)
    expirations = list(call_with_retries(lambda: ticker_obj.options) or [])
    expiration, dte = choose_expiration(
        expirations=expirations,
        as_of=as_of,
        min_dte=int(screen_cfg["option_dte_min"]),
        max_dte=int(screen_cfg["option_dte_max"]),
        target_dte=int(screen_cfg["option_target_dte"]),
        monthly_only=bool(screen_cfg.get("option_monthly_only", False)),
    )
    if not expiration or not dte:
        return OptionSelection(None, None, None, None, None, None, None, None, None, None, None, None, None)

    chain = call_with_retries(ticker_obj.option_chain, expiration)
    selection = select_call_option(
        calls=chain.calls,
        spot=spot,
        min_otm_pct=min_otm_pct,
        max_otm_pct=max_otm_pct,
        preferred_otm_pct=preferred_otm_pct,
        dte=dte,
    )
    selection.expiration = expiration
    return selection


def fetch_ticker_metrics_yahoo(
    ticker: str,
    benchmark_prices: pd.Series,
    config: dict[str, Any],
    as_of: date,
    metrics_signature: str,
) -> dict[str, Any]:
    yf_module = require_yfinance()
    ticker_obj = yf_module.Ticker(ticker)
    info: dict[str, Any] = {}
    try:
        info = call_with_retries(ticker_obj.get_info) or {}
    except Exception:
        info = {}

    history = fetch_history(ticker, period=config["data"]["history_period"])
    price_series = history["Adj Close"] if "Adj Close" in history.columns else history["Close"]
    price_series = price_series.dropna()
    if price_series.empty:
        raise RuntimeError("No usable close prices returned.")

    latest_price = safe_float(price_series.iloc[-1])
    if latest_price is None:
        raise RuntimeError("Could not compute latest price.")

    one_year_prices = price_series.tail(252)
    one_year_returns = one_year_prices.pct_change().dropna()
    benchmark_returns = benchmark_prices.tail(252).pct_change().dropna()
    realized_vol = safe_float(one_year_returns.std() * math.sqrt(252))
    beta_1y = compute_beta(one_year_returns, benchmark_returns)

    three_year_prices = price_series.tail(756)
    total_return_3y = None
    if len(three_year_prices) >= 200:
        total_return_3y = safe_float((three_year_prices.iloc[-1] / three_year_prices.iloc[0]) - 1.0)
    max_drawdown_3y = compute_max_drawdown(three_year_prices)

    dividends = history.get("Dividends", pd.Series(dtype=float)).dropna()
    if not dividends.empty and getattr(dividends.index, "tz", None) is not None:
        dividends = pd.Series(dividends.to_numpy(), index=dividends.index.tz_localize(None))
    trailing_cutoff = pd.Timestamp(as_of) - pd.Timedelta(days=365)
    ttm_dividends = safe_float(dividends[dividends.index >= trailing_cutoff].sum())
    dividend_yield = safe_float(ttm_dividends / latest_price) if ttm_dividends is not None else None

    size_metric_name = "marketCap"
    size_metric = safe_float(info.get("marketCap"))
    if size_metric is None:
        size_metric_name = "totalAssets"
        size_metric = safe_float(info.get("totalAssets"))
    if size_metric is None:
        size_metric_name = "enterpriseValue"
        size_metric = safe_float(info.get("enterpriseValue"))

    option_selection = fetch_option_selection(
        ticker_obj=ticker_obj,
        spot=latest_price,
        as_of=as_of,
        screen_cfg=config["screen"],
    )

    return {
        "ticker": ticker,
        "asset_type": info.get("quoteType") or "UNKNOWN",
        "size_metric_name": size_metric_name,
        "size_metric": size_metric,
        "price": latest_price,
        "ttm_dividends": ttm_dividends,
        "dividend_yield": dividend_yield,
        "beta_1y": beta_1y,
        "realized_vol_1y": realized_vol,
        "total_return_3y": total_return_3y,
        "max_drawdown_3y": max_drawdown_3y,
        "selected_expiration": option_selection.expiration,
        "selected_dte": option_selection.dte,
        "selected_strike": option_selection.strike,
        "selected_call_bid": option_selection.bid,
        "selected_call_ask": option_selection.ask,
        "selected_call_mid": option_selection.mid,
        "selected_call_open_interest": option_selection.open_interest,
        "selected_call_volume": option_selection.volume,
        "expiration_total_call_open_interest": option_selection.total_call_open_interest,
        "selected_call_otm_pct": option_selection.otm_pct,
        "selected_call_spread_pct": option_selection.spread_pct,
        "selected_call_yield": option_selection.call_yield,
        "annualized_call_yield": option_selection.annualized_call_yield,
        "metrics_signature": metrics_signature,
        "as_of_date": as_of.isoformat(),
        "error": None,
    }


def fetch_ticker_metrics(
    ticker: str,
    benchmark_prices: pd.Series,
    config: dict[str, Any],
    as_of: date,
    metrics_signature: str,
    *,
    ib: Any | None = None,
    seed_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    market_data_source = get_market_data_source(config)
    if market_data_source == "yahoo" or ib is None:
        return fetch_ticker_metrics_yahoo(
            ticker=ticker,
            benchmark_prices=benchmark_prices,
            config=config,
            as_of=as_of,
            metrics_signature=metrics_signature,
        )

    metadata = dict(seed_metadata or {})
    info: dict[str, Any] = {}
    if market_data_source == "hybrid" or not metadata:
        info = fetch_ticker_info_yahoo(ticker)
    try:
        history = fetch_history_ib(ib, ticker, period=config["data"]["history_period"])
        price_series = history["Close"].dropna()
        if price_series.empty:
            raise RuntimeError("No usable IB close prices returned.")

        stock_contract, market_price, ttm_dividends = fetch_stock_snapshot_ib(
            ib,
            ticker,
            config=config,
            wait_seconds=float(config["data"].get("ibkr", {}).get("market_data_wait_seconds", 2.0)),
        )
        latest_price = market_price if market_price is not None else safe_float(price_series.iloc[-1])
        if latest_price is None:
            raise RuntimeError("Could not compute latest IB price.")

        one_year_prices = price_series.tail(252)
        one_year_returns = one_year_prices.pct_change().dropna()
        benchmark_returns = benchmark_prices.tail(252).pct_change().dropna()
        realized_vol = safe_float(one_year_returns.std() * math.sqrt(252))
        beta_1y = compute_beta(one_year_returns, benchmark_returns)

        three_year_prices = price_series.tail(756)
        total_return_3y = None
        if len(three_year_prices) >= 200:
            total_return_3y = safe_float((three_year_prices.iloc[-1] / three_year_prices.iloc[0]) - 1.0)
        max_drawdown_3y = compute_max_drawdown(three_year_prices)
        dividend_yield = safe_float(ttm_dividends / latest_price) if ttm_dividends is not None else None

        size_metric_name = str(metadata.get("size_metric_name") or "").strip() or "marketCap"
        size_metric = safe_float(metadata.get("size_metric"))
        if size_metric is None:
            size_metric_name = "marketCap"
            size_metric = safe_float(info.get("marketCap"))
        if size_metric is None:
            size_metric_name = "totalAssets"
            size_metric = safe_float(info.get("totalAssets"))
        if size_metric is None:
            size_metric_name = "enterpriseValue"
            size_metric = safe_float(info.get("enterpriseValue"))

        option_selection = fetch_option_selection_ib(
            ib=ib,
            stock_contract=stock_contract,
            spot=latest_price,
            as_of=as_of,
            screen_cfg=config["screen"],
            config=config,
            wait_seconds=float(config["data"].get("ibkr", {}).get("option_data_wait_seconds", 3.0)),
        )
        if market_data_source == "hybrid" and (
            option_selection.expiration is None
            or option_selection.mid is None
            or option_selection.spread_pct is None
        ):
            ticker_obj = require_yfinance().Ticker(ticker)
            yahoo_option = fetch_option_selection(
                ticker_obj=ticker_obj,
                spot=latest_price,
                as_of=as_of,
                screen_cfg=config["screen"],
            )
            if yahoo_option.expiration is not None:
                option_selection = yahoo_option

        return {
            "ticker": ticker,
            "asset_type": metadata.get("asset_type") or info.get("quoteType") or "UNKNOWN",
            "size_metric_name": size_metric_name,
            "size_metric": size_metric,
            "price": latest_price,
            "ttm_dividends": ttm_dividends,
            "dividend_yield": dividend_yield,
            "beta_1y": beta_1y,
            "realized_vol_1y": realized_vol,
            "total_return_3y": total_return_3y,
            "max_drawdown_3y": max_drawdown_3y,
            "selected_expiration": option_selection.expiration,
            "selected_dte": option_selection.dte,
            "selected_strike": option_selection.strike,
            "selected_call_bid": option_selection.bid,
            "selected_call_ask": option_selection.ask,
            "selected_call_mid": option_selection.mid,
            "selected_call_open_interest": option_selection.open_interest,
            "selected_call_volume": option_selection.volume,
            "expiration_total_call_open_interest": option_selection.total_call_open_interest,
            "selected_call_otm_pct": option_selection.otm_pct,
            "selected_call_spread_pct": option_selection.spread_pct,
            "selected_call_yield": option_selection.call_yield,
            "annualized_call_yield": option_selection.annualized_call_yield,
            "metrics_signature": metrics_signature,
            "as_of_date": as_of.isoformat(),
            "error": None,
        }
    except Exception:
        if market_data_source == "hybrid":
            return fetch_ticker_metrics_yahoo(
                ticker=ticker,
                benchmark_prices=benchmark_prices,
                config=config,
                as_of=as_of,
                metrics_signature=metrics_signature,
            )
        raise


def refresh_metrics(config: dict[str, Any], config_path: Path) -> pd.DataFrame:
    metrics_path = resolve_metrics_path(config, config_path, None)
    tickers = load_universe(config, config_path.parent)
    return refresh_metrics_for_tickers(
        config=config,
        config_path=config_path,
        tickers=tickers,
        output_path=metrics_path,
    )


def refresh_metrics_for_tickers(
    config: dict[str, Any],
    config_path: Path,
    tickers: list[str],
    output_path: Path,
    reusable_metrics_paths: list[Path] | None = None,
    metadata_by_ticker: dict[str, dict[str, Any]] | None = None,
) -> pd.DataFrame:
    base_dir = config_path.parent
    output_dir = (base_dir / config["data"]["cache_dir"]).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    normalized_tickers = normalize_tickers(tickers)
    if not normalized_tickers:
        raise RuntimeError("Universe is empty.")

    metrics_path = output_path.resolve()
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    market_data_source = get_market_data_source(config)
    if market_data_source == "yahoo":
        require_yfinance()
    ib: Any | None = None
    if market_data_source in {"ibkr", "hybrid"}:
        ib = connect_ibkr(config)

    benchmark = config["data"]["benchmark"]
    benchmark_history = (
        fetch_history_ib(ib, benchmark, period=config["data"]["history_period"])
        if ib is not None
        else fetch_history(benchmark, period=config["data"]["history_period"])
    )
    benchmark_prices = (
        benchmark_history["Adj Close"]
        if "Adj Close" in benchmark_history.columns
        else benchmark_history["Close"]
    ).dropna()
    as_of = parse_as_of_date(config["data"].get("as_of_date"))
    metrics_signature = build_metrics_signature(config)
    progress_every = max(1, int(config["data"].get("progress_every", 10)))
    checkpoint_every = max(1, int(config["data"].get("checkpoint_every", 10)))

    reusable_paths = [metrics_path]
    for candidate in reusable_metrics_paths or []:
        candidate_path = Path(candidate).resolve()
        if candidate_path != metrics_path:
            reusable_paths.append(candidate_path)

    existing_rows_by_ticker: dict[str, dict[str, Any]] = {}
    if bool(config["data"].get("resume_existing_metrics", True)):
        existing_rows_by_ticker = load_reusable_metrics_rows(
            reusable_paths,
            as_of=as_of,
            metrics_signature=metrics_signature,
        )

    print(
        f"Starting refresh for {len(normalized_tickers)} tickers | "
        f"benchmark={benchmark} | reusable same-day rows={len(existing_rows_by_ticker)}",
        flush=True,
    )

    rows: list[dict[str, Any]] = []
    reused_count = 0
    fetched_count = 0
    fetched_success_count = 0
    fetched_error_count = 0
    started_at = time.monotonic()
    latest_df = pd.DataFrame()
    try:
        for index, ticker in enumerate(normalized_tickers, start=1):
            cached_row = existing_rows_by_ticker.get(ticker)
            if cached_row is not None:
                rows.append(cached_row)
                reused_count += 1
            else:
                print(
                    f"[{index}/{len(normalized_tickers)}] fetching {ticker} via {market_data_source}",
                    flush=True,
                )
                try:
                    metrics = fetch_ticker_metrics(
                        ticker=ticker,
                        benchmark_prices=benchmark_prices,
                        config=config,
                        as_of=as_of,
                        metrics_signature=metrics_signature,
                        ib=ib,
                        seed_metadata=(metadata_by_ticker or {}).get(ticker),
                    )
                except Exception as exc:
                    metrics = {column: None for column in METRIC_COLUMNS}
                    metrics["ticker"] = ticker
                    metrics["metrics_signature"] = metrics_signature
                    metrics["as_of_date"] = as_of.isoformat()
                    metrics["error"] = str(exc)
                rows.append(metrics)
                fetched_count += 1
                if has_data_error(metrics):
                    fetched_error_count += 1
                else:
                    fetched_success_count += 1

            if index % checkpoint_every == 0 or index == len(normalized_tickers):
                latest_df = persist_metrics_rows(rows, metrics_path)

            if index % progress_every == 0 or index == len(normalized_tickers):
                elapsed = time.monotonic() - started_at
                avg_seconds = elapsed / max(index, 1)
                remaining = len(normalized_tickers) - index
                eta = format_seconds(avg_seconds * remaining)
                print(
                    f"[{index}/{len(normalized_tickers)}] {index / len(normalized_tickers):.1%} | "
                    f"reused={reused_count} | fetched_ok={fetched_success_count} | "
                    f"fetched_error={fetched_error_count} | elapsed={format_seconds(elapsed)} | eta={eta}",
                    flush=True,
                )
    finally:
        if ib is not None and getattr(ib, "isConnected", lambda: False)():
            ib.disconnect()

    if latest_df.empty:
        latest_df = persist_metrics_rows(rows, metrics_path)
    print(
        f"Universe size: {len(normalized_tickers)} | reused same-day rows: {reused_count} | "
        f"fetched this run: {fetched_count} | fetched ok: {fetched_success_count} | "
        f"fetched errors: {fetched_error_count}"
    )
    return latest_df


def add_screen_columns(df: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    screen_cfg = config["screen"]
    caps = config["scoring"]["caps"]
    tolerances = config["scoring"]["tolerances"]
    weights = config["scoring"]["weights"]
    total_weight = sum(weights.values()) or 1.0

    statuses: list[str] = []
    fail_reasons: list[str] = []
    total_scores: list[float] = []
    dividend_scores: list[float] = []
    call_scores: list[float] = []
    liquidity_scores: list[float] = []
    quality_scores: list[float] = []
    risk_scores: list[float] = []
    combined_yields: list[float | None] = []

    for _, row in df.iterrows():
        failures: list[str] = []

        numeric = {column: safe_float(row.get(column)) for column in df.columns}
        allowed_asset_types = screen_cfg.get("allowed_asset_types") or []
        normalized_allowed_asset_types = {
            str(asset_type).strip().upper()
            for asset_type in allowed_asset_types
            if str(asset_type).strip()
        }
        asset_type = str(row.get("asset_type") or "").strip().upper()
        if has_data_error(row):
            failures.append("data_error")
        if normalized_allowed_asset_types:
            if not asset_type:
                failures.append("asset_type_missing")
            elif asset_type not in normalized_allowed_asset_types:
                allowed_text = "|".join(sorted(normalized_allowed_asset_types))
                failures.append(f"asset_type_not_allowed:{asset_type}->{allowed_text}")

        checks = [
            ("size_metric", numeric.get("size_metric"), ">=", screen_cfg["min_size_metric"]),
            ("dividend_yield", numeric.get("dividend_yield"), ">=", screen_cfg["min_dividend_yield"]),
            (
                "annualized_call_yield",
                numeric.get("annualized_call_yield"),
                ">=",
                screen_cfg["min_call_annualized_yield"],
            ),
            (
                "selected_call_spread_pct",
                numeric.get("selected_call_spread_pct"),
                "<=",
                screen_cfg["max_option_spread_pct"],
            ),
            ("beta_1y", numeric.get("beta_1y"), ">=", screen_cfg["min_beta_1y"]),
            ("beta_1y", numeric.get("beta_1y"), "<=", screen_cfg["max_beta_1y"]),
            (
                "realized_vol_1y",
                numeric.get("realized_vol_1y"),
                ">=",
                screen_cfg["min_realized_vol_1y"],
            ),
            (
                "realized_vol_1y",
                numeric.get("realized_vol_1y"),
                "<=",
                screen_cfg["max_realized_vol_1y"],
            ),
            ("total_return_3y", numeric.get("total_return_3y"), ">=", screen_cfg["min_total_return_3y"]),
            ("max_drawdown_3y", numeric.get("max_drawdown_3y"), "<=", screen_cfg["max_drawdown_3y"]),
        ]

        for label, value, operator, threshold in checks:
            if value is None:
                failures.append(f"{label}_missing")
                continue
            if operator == ">=" and value < threshold:
                failures.append(f"{label}<{threshold}")
            if operator == "<=" and value > threshold:
                failures.append(f"{label}>{threshold}")

        open_interest = numeric.get("selected_call_open_interest")
        volume = numeric.get("selected_call_volume")
        min_open_interest = screen_cfg["min_call_open_interest"]
        min_volume = screen_cfg["min_call_volume"]
        if open_interest is None and volume is None:
            failures.append("option_liquidity_missing")
        else:
            oi_ok = open_interest is not None and open_interest >= min_open_interest
            volume_ok = volume is not None and volume >= min_volume
            if not (oi_ok or volume_ok):
                failures.append(f"option_liquidity<oi:{min_open_interest}|vol:{min_volume}")

        dividend_score = normalize(
            numeric.get("dividend_yield"),
            screen_cfg["min_dividend_yield"],
            caps["dividend_yield"],
        )
        call_score = normalize(
            numeric.get("annualized_call_yield"),
            screen_cfg["min_call_annualized_yield"],
            caps["call_annualized_yield"],
        )
        oi_score = normalize(
            numeric.get("selected_call_open_interest"),
            screen_cfg["min_call_open_interest"],
            caps["call_open_interest"],
        )
        spread_score = inverse_normalize(
            numeric.get("selected_call_spread_pct"),
            tolerances["option_spread_pct"],
            screen_cfg["max_option_spread_pct"],
        )
        liquidity_score = 0.7 * oi_score + 0.3 * spread_score

        upside_score = normalize(
            numeric.get("total_return_3y"),
            screen_cfg["min_total_return_3y"],
            caps["total_return_3y"],
        )
        drawdown_score = inverse_normalize(
            numeric.get("max_drawdown_3y"),
            screen_cfg["max_drawdown_3y"] * 0.5,
            caps["max_drawdown_3y"],
        )
        quality_score = 0.6 * upside_score + 0.4 * drawdown_score

        beta_fit = range_fit(
            numeric.get("beta_1y"),
            screen_cfg["min_beta_1y"],
            screen_cfg["max_beta_1y"],
            tolerances["beta_1y"],
        )
        vol_fit = range_fit(
            numeric.get("realized_vol_1y"),
            screen_cfg["min_realized_vol_1y"],
            screen_cfg["max_realized_vol_1y"],
            tolerances["realized_vol_1y"],
        )
        risk_fit = (beta_fit + vol_fit) / 2.0

        total_score = (
            weights["dividend_yield"] * dividend_score
            + weights["call_yield"] * call_score
            + weights["liquidity"] * liquidity_score
            + weights["quality"] * quality_score
            + weights["risk_fit"] * risk_fit
        ) / total_weight

        if has_data_error(row):
            status = "ERROR"
        else:
            status = "PASS" if not failures else "FAIL"

        statuses.append(status)
        fail_reasons.append(";".join(failures))
        total_scores.append(round(total_score, 6))
        dividend_scores.append(round(dividend_score, 6))
        call_scores.append(round(call_score, 6))
        liquidity_scores.append(round(liquidity_score, 6))
        quality_scores.append(round(quality_score, 6))
        risk_scores.append(round(risk_fit, 6))

        dividend_yield = numeric.get("dividend_yield")
        annualized_call_yield = numeric.get("annualized_call_yield")
        if dividend_yield is None or annualized_call_yield is None:
            combined_yields.append(None)
        else:
            combined_yields.append(dividend_yield + annualized_call_yield)

    result = df.copy()
    result["status"] = statuses
    result["fail_reasons"] = fail_reasons
    result["score_total"] = total_scores
    result["score_dividend"] = dividend_scores
    result["score_call"] = call_scores
    result["score_liquidity"] = liquidity_scores
    result["score_quality"] = quality_scores
    result["score_risk_fit"] = risk_scores
    result["combined_income_score_proxy"] = combined_yields
    result["_status_rank"] = result["status"].map({"PASS": 0, "FAIL": 1, "ERROR": 2}).fillna(3)
    result = result.sort_values(
        by=["_status_rank", "score_total", "combined_income_score_proxy"],
        ascending=[True, False, False],
        na_position="last",
    )
    result = result.drop(columns=["_status_rank"])
    return result


def print_screen(df: pd.DataFrame, top_n: int, show_failed: bool) -> None:
    display = df if show_failed else df[df["status"] == "PASS"]
    display = display.head(top_n)
    if display.empty:
        print("No rows matched the current screen.")
        return

    table = display[
        [
            "ticker",
            "status",
            "score_total",
            "dividend_yield",
            "annualized_call_yield",
            "combined_income_score_proxy",
            "selected_call_open_interest",
            "selected_call_spread_pct",
            "beta_1y",
            "realized_vol_1y",
            "total_return_3y",
            "max_drawdown_3y",
            "fail_reasons",
        ]
    ].copy()

    pct_columns = [
        "dividend_yield",
        "annualized_call_yield",
        "combined_income_score_proxy",
        "selected_call_spread_pct",
        "realized_vol_1y",
        "total_return_3y",
        "max_drawdown_3y",
    ]
    for column in pct_columns:
        table[column] = table[column].apply(
            lambda value: "" if pd.isna(value) else f"{float(value):.2%}"
        )
    table["score_total"] = table["score_total"].apply(
        lambda value: "" if pd.isna(value) else f"{float(value):.3f}"
    )
    table["selected_call_open_interest"] = table["selected_call_open_interest"].apply(
        lambda value: "" if pd.isna(value) else f"{int(float(value)):,}"
    )
    table["beta_1y"] = table["beta_1y"].apply(
        lambda value: "" if pd.isna(value) else f"{float(value):.2f}"
    )
    if tabulate is not None:
        print(tabulate(table, headers="keys", tablefmt="github", showindex=False))
    else:
        print(table.to_string(index=False))


def resolve_metrics_path(config: dict[str, Any], config_path: Path, override: str | None) -> Path:
    if override:
        return Path(override).resolve()
    return (config_path.parent / config["data"]["cache_dir"] / config["data"]["metrics_filename"]).resolve()


def resolve_screen_path(config: dict[str, Any], config_path: Path, override: str | None) -> Path:
    if override:
        return Path(override).resolve()
    return (config_path.parent / config["data"]["cache_dir"] / config["data"]["screened_filename"]).resolve()


def run_screen(
    config: dict[str, Any],
    config_path: Path,
    input_override: str | None,
    output_override: str | None,
    top_n_override: int | None,
) -> pd.DataFrame:
    metrics_path = resolve_metrics_path(config, config_path, input_override)
    if not metrics_path.exists():
        raise FileNotFoundError(
            f"Metrics CSV not found: {metrics_path}. Run the refresh command first."
        )
    df = pd.read_csv(metrics_path)
    screened = add_screen_columns(df, config)
    output_path = resolve_screen_path(config, config_path, output_override)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    screened.to_csv(output_path, index=False)
    print_screen(
        screened,
        top_n=top_n_override or int(config["output"]["top_n"]),
        show_failed=bool(config["output"]["show_failed"]),
    )
    counts = screened["status"].value_counts().to_dict()
    pass_count = counts.get("PASS", 0)
    fail_count = counts.get("FAIL", 0)
    error_count = counts.get("ERROR", 0)
    print(f"Status counts: PASS={pass_count} | FAIL={fail_count} | ERROR={error_count}")
    print(f"\nSaved screened results to {output_path}")
    return screened


def main() -> int:
    args = parse_args()
    config_path = Path(args.config).resolve()
    config = load_config(config_path)

    if args.command == "refresh":
        refreshed = refresh_metrics(config, config_path)
        metrics_path = resolve_metrics_path(config, config_path, args.output)
        if args.output:
            metrics_path.parent.mkdir(parents=True, exist_ok=True)
            refreshed.to_csv(metrics_path, index=False)
        print(f"Saved raw metrics to {metrics_path}")
        return 0

    if args.command == "screen":
        run_screen(
            config=config,
            config_path=config_path,
            input_override=args.input,
            output_override=args.output,
            top_n_override=args.top_n,
        )
        return 0

    refreshed = refresh_metrics(config, config_path)
    metrics_path = resolve_metrics_path(config, config_path, None)
    print(f"Saved raw metrics to {metrics_path}")
    if args.output:
        screen_output = args.output
    else:
        screen_output = None
    run_screen(
        config=config,
        config_path=config_path,
        input_override=str(metrics_path),
        output_override=screen_output,
        top_n_override=args.top_n,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
