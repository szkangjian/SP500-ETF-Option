#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd
import yfinance as yf


SOURCE_URL = "https://raw.githubusercontent.com/JerBouma/FinanceDatabase/main/database/etfs.csv"
DEFAULT_SOURCE = "finance_etfs.csv"
DEFAULT_OUTPUT_CSV = "etf_broad_universe.csv"
DEFAULT_OUTPUT_TXT = "etf_broad_universe.txt"

US_MAJOR_EXCHANGES = {"NIM", "NMS", "NGM", "ASE", "PCX", "BTS"}
SYMBOL_PATTERN = re.compile(r"[A-Z]{1,5}")
EXCLUDE_NAME_PATTERN = re.compile(
    r"(?:ultra|2x|3x|4x|leverag|inverse|short|bear|buffer|hedged|daily|"
    r"-1x|-2x|-3x|-4x|vix|volatility|futures strategy|single stock)",
    re.IGNORECASE,
)
EQUITY_CATEGORY_GROUPS = {
    "Equities",
    "Information Technology",
    "Financials",
    "Industrials",
    "Energy",
    "Materials",
    "Real Estate",
    "Health Care",
    "Consumer Discretionary",
    "Utilities",
    "Consumer Staples",
    "Communication Services",
}
NON_EQUITY_CATEGORY_GROUPS = {"Commodities", "Currencies", "Derivatives"}
EQUITY_TEXT_PATTERN = re.compile(
    r"(?:\bequity\b|\bequities\b|\bstock\b|\bstocks\b|\bcommon stock(?:s)?\b|"
    r"publicly traded companies|securities of companies|companies in the|companies domiciled)",
    re.IGNORECASE,
)
NON_EQUITY_TEXT_PATTERN = re.compile(
    r"(?:\bbond(?:s)?\b|\btreasury\b|\bmunicipal\b|\bmuni\b|\bpreferred\b|"
    r"\bmortgage\b|\bloan\b|\bcredit\b|\bdebt\b|\bfixed income\b|"
    r"\binvestment[- ]grade\b|\bhigh yield\b|\bcommodity\b|\bgold\b|\bsilver\b|"
    r"\bbullion\b|\bcurrency\b|\bforex\b|\bbitcoin\b|\bcrypto\b|\bfutures\b)",
    re.IGNORECASE,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a broader, liquid US ETF universe for covered-call screening."
    )
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help="Local ETF metadata CSV. If missing, the script downloads the default source.",
    )
    parser.add_argument(
        "--history-period",
        default="6mo",
        help="History period used to estimate liquidity.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100,
        help="How many tickers to request from Yahoo Finance per batch.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=None,
        help="Optional cap on the final ETF universe. Omit to keep every ETF that passes the prefilter.",
    )
    parser.add_argument(
        "--min-price",
        type=float,
        default=10.0,
        help="Minimum latest price for the prefilter.",
    )
    parser.add_argument(
        "--min-trading-days",
        type=int,
        default=40,
        help="Minimum number of non-null close observations in the history window.",
    )
    parser.add_argument(
        "--min-avg-dollar-volume",
        type=float,
        default=5_000_000.0,
        help="Minimum average daily dollar volume for the prefilter.",
    )
    parser.add_argument(
        "--output-csv",
        default=DEFAULT_OUTPUT_CSV,
        help="CSV file containing the ranked ETF universe.",
    )
    parser.add_argument(
        "--output-txt",
        default=DEFAULT_OUTPUT_TXT,
        help="Text file containing one ticker per line for the screener.",
    )
    parser.add_argument(
        "--market-scope",
        choices=["all", "equity", "equity_commodity"],
        default="all",
        help=(
            "Keep all liquid ETFs, only USD-traded stock-market ETFs, or stock-market ETFs plus commodity ETFs "
            "(including non-US equity ETFs such as EWJ)."
        ),
    )
    return parser.parse_args()


def load_source(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path)

    df = pd.read_csv(SOURCE_URL)
    df.to_csv(path, index=False)
    return df


def apply_metadata_filters(df: pd.DataFrame) -> pd.DataFrame:
    filtered = df.copy()
    filtered["symbol"] = filtered["symbol"].astype(str).str.upper().str.strip()
    filtered["name"] = filtered["name"].fillna("")
    filtered["currency"] = filtered["currency"].fillna("")
    filtered["exchange"] = filtered["exchange"].fillna("")

    mask = (
        filtered["currency"].eq("USD")
        & filtered["exchange"].isin(US_MAJOR_EXCHANGES)
        & filtered["symbol"].map(lambda value: bool(SYMBOL_PATTERN.fullmatch(value)))
        & ~filtered["symbol"].str.startswith("^")
        & ~filtered["name"].str.contains(EXCLUDE_NAME_PATTERN, na=False)
    )
    filtered = filtered[mask].drop_duplicates(subset=["symbol"]).copy()
    return filtered


def is_equity_market_etf(row: pd.Series) -> bool:
    category_group = str(row.get("category_group") or "").strip()
    text = f"{row.get('name', '')} {row.get('summary', '')}"
    has_equity_text = bool(EQUITY_TEXT_PATTERN.search(text))
    has_non_equity_text = bool(NON_EQUITY_TEXT_PATTERN.search(text))

    if category_group in NON_EQUITY_CATEGORY_GROUPS:
        return False
    if category_group in EQUITY_CATEGORY_GROUPS:
        return not has_non_equity_text or has_equity_text
    return has_equity_text and not has_non_equity_text


def apply_market_scope_filters(df: pd.DataFrame, scope: str) -> pd.DataFrame:
    if scope == "all":
        return df.copy()
    if scope == "equity":
        return df[df.apply(is_equity_market_etf, axis=1)].copy()
    if scope == "equity_commodity":
        commodity_mask = df["category_group"].fillna("").eq("Commodities")
        equity_mask = df.apply(is_equity_market_etf, axis=1)
        return df[equity_mask | commodity_mask].copy()
    raise ValueError(f"Unsupported market scope: {scope}")


def extract_history_slice(history: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame()
    if isinstance(history.columns, pd.MultiIndex):
        level0 = history.columns.get_level_values(0)
        level1 = history.columns.get_level_values(1)
        if ticker in level0:
            return history[ticker].copy()
        if ticker in level1:
            return history.xs(ticker, axis=1, level=1, drop_level=True).copy()
        return pd.DataFrame()
    return history.copy()


def fetch_liquidity_metrics(
    tickers: list[str],
    history_period: str,
    chunk_size: int,
) -> pd.DataFrame:
    rows: list[dict[str, float | int | str | None]] = []
    for start in range(0, len(tickers), chunk_size):
        chunk = tickers[start : start + chunk_size]
        history = yf.download(
            tickers=chunk,
            period=history_period,
            auto_adjust=False,
            progress=False,
            threads=True,
            group_by="ticker",
        )

        for ticker in chunk:
            ticker_history = extract_history_slice(history, ticker)
            if ticker_history.empty:
                rows.append(
                    {
                        "symbol": ticker,
                        "trading_days": 0,
                        "last_price": None,
                        "avg_volume": None,
                        "avg_dollar_volume": None,
                    }
                )
                continue

            close_column = "Adj Close" if "Adj Close" in ticker_history.columns else "Close"
            if close_column not in ticker_history.columns or "Volume" not in ticker_history.columns:
                rows.append(
                    {
                        "symbol": ticker,
                        "trading_days": 0,
                        "last_price": None,
                        "avg_volume": None,
                        "avg_dollar_volume": None,
                    }
                )
                continue

            close = pd.to_numeric(ticker_history[close_column], errors="coerce")
            volume = pd.to_numeric(ticker_history["Volume"], errors="coerce")
            merged = pd.DataFrame({"close": close, "volume": volume}).dropna()
            if merged.empty:
                rows.append(
                    {
                        "symbol": ticker,
                        "trading_days": 0,
                        "last_price": None,
                        "avg_volume": None,
                        "avg_dollar_volume": None,
                    }
                )
                continue

            merged["dollar_volume"] = merged["close"] * merged["volume"]
            rows.append(
                {
                    "symbol": ticker,
                    "trading_days": int(len(merged)),
                    "last_price": float(merged["close"].iloc[-1]),
                    "avg_volume": float(merged["volume"].mean()),
                    "avg_dollar_volume": float(merged["dollar_volume"].mean()),
                }
            )

    return pd.DataFrame(rows)


def main() -> int:
    args = parse_args()
    base_dir = Path.cwd()

    source_path = (base_dir / args.source).resolve()
    metadata = load_source(source_path)
    metadata_filtered = apply_metadata_filters(metadata)
    metadata_filtered = apply_market_scope_filters(metadata_filtered, args.market_scope)

    liquidity = fetch_liquidity_metrics(
        tickers=metadata_filtered["symbol"].tolist(),
        history_period=args.history_period,
        chunk_size=args.chunk_size,
    )

    merged = metadata_filtered.merge(liquidity, on="symbol", how="left")
    merged = merged[
        (merged["trading_days"].fillna(0) >= args.min_trading_days)
        & (merged["last_price"].fillna(0) >= args.min_price)
        & (merged["avg_dollar_volume"].fillna(0) >= args.min_avg_dollar_volume)
    ].copy()
    merged = merged.sort_values(
        by=["avg_dollar_volume", "avg_volume", "symbol"],
        ascending=[False, False, True],
        na_position="last",
    )
    merged["liquidity_rank"] = range(1, len(merged) + 1)

    top = merged.copy()
    if args.top_n is not None and args.top_n > 0:
        top = merged.head(args.top_n).copy()
    output_csv = (base_dir / args.output_csv).resolve()
    output_txt = (base_dir / args.output_txt).resolve()
    top.to_csv(output_csv, index=False)
    output_txt.write_text("\n".join(top["symbol"].tolist()) + "\n", encoding="utf-8")

    print(
        f"ETF metadata rows: {len(metadata)} | after basic filters: {len(metadata_filtered)} | "
        f"after liquidity filters: {len(merged)} | saved {len(top)}"
    )
    print(f"Saved ranked ETF universe to {output_csv}")
    print(f"Saved ETF ticker list to {output_txt}")
    preview = top[
        ["liquidity_rank", "symbol", "name", "exchange", "avg_dollar_volume", "avg_volume", "last_price"]
    ].head(20)
    print(preview.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
