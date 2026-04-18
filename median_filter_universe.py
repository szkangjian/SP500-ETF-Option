#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Filter a universe by keeping only rows whose size metric and average dollar volume "
            "are both at or above their respective medians."
        )
    )
    parser.add_argument("--universe-csv", required=True, help="Universe CSV containing avg_dollar_volume.")
    parser.add_argument("--metrics-csv", required=True, help="Metrics CSV containing size_metric.")
    parser.add_argument("--screen-csv", help="Optional screened CSV to filter with the same ticker set.")
    parser.add_argument(
        "--universe-ticker-col",
        default="symbol",
        help="Ticker column name inside the universe CSV.",
    )
    parser.add_argument(
        "--metrics-ticker-col",
        default="ticker",
        help="Ticker column name inside the metrics/screen CSVs.",
    )
    parser.add_argument(
        "--size-col",
        default="size_metric",
        help="Column used for the size-median gate.",
    )
    parser.add_argument(
        "--liquidity-col",
        default="avg_dollar_volume",
        help="Column used for the liquidity-median gate.",
    )
    parser.add_argument("--output-universe-csv", required=True, help="Filtered universe CSV output path.")
    parser.add_argument("--output-universe-txt", required=True, help="Filtered universe TXT output path.")
    parser.add_argument("--output-metrics-csv", required=True, help="Filtered metrics CSV output path.")
    parser.add_argument("--output-screen-csv", help="Filtered screen CSV output path.")
    return parser.parse_args()


def format_number(value: float) -> str:
    for suffix, scale in (("T", 1e12), ("B", 1e9), ("M", 1e6)):
        if abs(value) >= scale:
            return f"{value / scale:.2f}{suffix}"
    return f"{value:.0f}"


def main() -> int:
    args = parse_args()

    universe_path = Path(args.universe_csv).resolve()
    metrics_path = Path(args.metrics_csv).resolve()
    screen_path = Path(args.screen_csv).resolve() if args.screen_csv else None

    universe_df = pd.read_csv(universe_path)
    metrics_df = pd.read_csv(metrics_path)

    universe_ticker_col = args.universe_ticker_col
    metrics_ticker_col = args.metrics_ticker_col
    size_col = args.size_col
    liquidity_col = args.liquidity_col

    merged = universe_df[[universe_ticker_col, liquidity_col]].merge(
        metrics_df[[metrics_ticker_col, size_col]],
        left_on=universe_ticker_col,
        right_on=metrics_ticker_col,
        how="left",
    )

    size_median = merged[size_col].dropna().median()
    liquidity_median = merged[liquidity_col].dropna().median()

    keep_mask = (
        merged[size_col].notna()
        & merged[liquidity_col].notna()
        & (merged[size_col] >= size_median)
        & (merged[liquidity_col] >= liquidity_median)
    )
    kept_tickers = set(merged.loc[keep_mask, universe_ticker_col].astype(str))

    filtered_universe = universe_df[
        universe_df[universe_ticker_col].astype(str).isin(kept_tickers)
    ].copy()
    filtered_metrics = metrics_df[
        metrics_df[metrics_ticker_col].astype(str).isin(kept_tickers)
    ].copy()

    output_universe_csv = Path(args.output_universe_csv).resolve()
    output_universe_txt = Path(args.output_universe_txt).resolve()
    output_metrics_csv = Path(args.output_metrics_csv).resolve()
    output_universe_csv.parent.mkdir(parents=True, exist_ok=True)
    output_metrics_csv.parent.mkdir(parents=True, exist_ok=True)

    filtered_universe.to_csv(output_universe_csv, index=False)
    output_universe_txt.write_text(
        "\n".join(filtered_universe[universe_ticker_col].astype(str).tolist()) + "\n",
        encoding="utf-8",
    )
    filtered_metrics.to_csv(output_metrics_csv, index=False)

    if screen_path and args.output_screen_csv:
        screen_df = pd.read_csv(screen_path)
        filtered_screen = screen_df[
            screen_df[metrics_ticker_col].astype(str).isin(kept_tickers)
        ].copy()
        output_screen_csv = Path(args.output_screen_csv).resolve()
        output_screen_csv.parent.mkdir(parents=True, exist_ok=True)
        filtered_screen.to_csv(output_screen_csv, index=False)

    print(
        f"Universe rows: {len(universe_df)} -> {len(filtered_universe)} | "
        f"size median: {format_number(float(size_median))} | "
        f"avg dollar volume median: {format_number(float(liquidity_median))}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
