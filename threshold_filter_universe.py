#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Filter a universe by applying explicit minimum thresholds such as size metric "
            "or average daily dollar volume."
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
        help="Column used for size-based thresholding.",
    )
    parser.add_argument(
        "--liquidity-col",
        default="avg_dollar_volume",
        help="Column used for liquidity-based thresholding.",
    )
    parser.add_argument(
        "--min-size-metric",
        type=float,
        help="Minimum size metric required to keep a ticker.",
    )
    parser.add_argument(
        "--min-avg-dollar-volume",
        type=float,
        help="Minimum average daily dollar volume required to keep a ticker.",
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

    universe_df = pd.read_csv(Path(args.universe_csv).resolve())
    metrics_df = pd.read_csv(Path(args.metrics_csv).resolve())
    screen_df = pd.read_csv(Path(args.screen_csv).resolve()) if args.screen_csv else None

    merged = universe_df[[args.universe_ticker_col, args.liquidity_col]].merge(
        metrics_df[[args.metrics_ticker_col, args.size_col]],
        left_on=args.universe_ticker_col,
        right_on=args.metrics_ticker_col,
        how="left",
    )

    keep_mask = pd.Series(True, index=merged.index)
    applied_parts: list[str] = []

    if args.min_size_metric is not None:
        keep_mask &= merged[args.size_col].notna() & (merged[args.size_col] >= args.min_size_metric)
        applied_parts.append(f"{args.size_col}>={format_number(float(args.min_size_metric))}")

    if args.min_avg_dollar_volume is not None:
        keep_mask &= merged[args.liquidity_col].notna() & (merged[args.liquidity_col] >= args.min_avg_dollar_volume)
        applied_parts.append(
            f"{args.liquidity_col}>={format_number(float(args.min_avg_dollar_volume))}"
        )

    kept_tickers = set(merged.loc[keep_mask, args.universe_ticker_col].astype(str))

    filtered_universe = universe_df[
        universe_df[args.universe_ticker_col].astype(str).isin(kept_tickers)
    ].copy()
    filtered_metrics = metrics_df[
        metrics_df[args.metrics_ticker_col].astype(str).isin(kept_tickers)
    ].copy()

    output_universe_csv = Path(args.output_universe_csv).resolve()
    output_universe_txt = Path(args.output_universe_txt).resolve()
    output_metrics_csv = Path(args.output_metrics_csv).resolve()
    output_universe_csv.parent.mkdir(parents=True, exist_ok=True)
    output_metrics_csv.parent.mkdir(parents=True, exist_ok=True)

    filtered_universe.to_csv(output_universe_csv, index=False)
    output_universe_txt.write_text(
        "\n".join(filtered_universe[args.universe_ticker_col].astype(str).tolist()) + "\n",
        encoding="utf-8",
    )
    filtered_metrics.to_csv(output_metrics_csv, index=False)

    if screen_df is not None and args.output_screen_csv:
        filtered_screen = screen_df[
            screen_df[args.metrics_ticker_col].astype(str).isin(kept_tickers)
        ].copy()
        output_screen_csv = Path(args.output_screen_csv).resolve()
        output_screen_csv.parent.mkdir(parents=True, exist_ok=True)
        filtered_screen.to_csv(output_screen_csv, index=False)

    applied_text = ", ".join(applied_parts) if applied_parts else "no thresholds"
    print(f"Universe rows: {len(universe_df)} -> {len(filtered_universe)} | applied: {applied_text}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
