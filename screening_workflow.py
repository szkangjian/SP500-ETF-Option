from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from covered_call_screener import (
    METRIC_COLUMNS,
    add_screen_columns,
    deep_merge,
    refresh_metrics_for_tickers,
    resolve_metrics_path,
)


US_MAJOR_EXCHANGES = {"NIM", "NMS", "NGM", "ASE", "PCX", "BTS"}
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
COMMODITY_TEXT_PATTERN = re.compile(
    r"(?:\bcommodity\b|\bgold\b|\bsilver\b|\boil\b|\benergy\b|\bbullion\b|\bmetals?\b|\bagriculture\b)",
    re.IGNORECASE,
)


DEFAULT_WORKFLOW_SETTINGS: dict[str, Any] = {
    "source_universe_csv": "",
    "allowed_market_classes": ["equity", "commodity"],
    "require_usd": True,
    "require_us_listing": True,
    "require_etf_asset_type": True,
    "min_avg_dollar_volume": 100_000_000.0,
    "min_size_metric": None,
    "step2_reuse_same_day_cache": False,
}


def infer_source_universe_csv(config: dict[str, Any], base_dir: Path | None = None) -> str:
    universe_path = str(config.get("universe", {}).get("path") or "").strip()
    if not universe_path:
        return _first_existing_universe_csv(base_dir)
    path = Path(universe_path)
    if path.suffix.lower() == ".csv":
        return path.name if _exists_in_base_dir(path.name, base_dir) else _first_existing_universe_csv(base_dir)
    sibling_csv = path.with_suffix(".csv")
    if _exists_in_base_dir(sibling_csv.name, base_dir):
        return sibling_csv.name
    return _first_existing_universe_csv(base_dir)


def _exists_in_base_dir(filename: str, base_dir: Path | None) -> bool:
    if base_dir is None:
        return False
    return (base_dir / filename).exists()


def _first_existing_universe_csv(base_dir: Path | None) -> str:
    preferred = [
        "etf_broad_universe.csv",
        "sp500_constituents.csv",
        "finance_etfs.csv",
    ]
    if base_dir is not None:
        for name in preferred:
            if (base_dir / name).exists():
                return name
        matches = sorted(path.name for path in base_dir.glob("*.csv"))
        if matches:
            return matches[0]
    return "etf_equity_commodity_universe.csv"


def infer_local_metrics_csv(
    source_universe_csv: str,
    base_dir: Path | None = None,
) -> str:
    preferred: list[str] = []
    universe_name = Path(source_universe_csv).name

    if universe_name == "finance_etfs.csv":
        preferred.append("out/etf_broad_metrics.csv")
    elif universe_name == "sp500_constituents.csv":
        preferred.append("out/sp500_metrics.csv")
    elif universe_name.endswith("_universe.csv"):
        preferred.append(f"out/{universe_name.replace('_universe.csv', '_metrics.csv')}")
    elif universe_name.endswith("candidate_universe.csv"):
        preferred.append(str(Path(source_universe_csv).with_name("step2_candidate_metrics.csv")))
        preferred.append(str(Path(source_universe_csv).with_name("step1_candidate_metrics.csv")))

    if base_dir is not None:
        for candidate in preferred:
            if (base_dir / candidate).exists():
                return candidate
    return preferred[0] if preferred else ""


def default_workflow_settings(config: dict[str, Any], base_dir: Path | None = None) -> dict[str, Any]:
    source_universe_csv = infer_source_universe_csv(config, base_dir=base_dir)
    min_avg_dollar_volume = DEFAULT_WORKFLOW_SETTINGS["min_avg_dollar_volume"]
    allowed_market_classes = DEFAULT_WORKFLOW_SETTINGS["allowed_market_classes"]
    require_etf_asset_type = DEFAULT_WORKFLOW_SETTINGS["require_etf_asset_type"]
    if Path(source_universe_csv).name == "sp500_constituents.csv":
        min_avg_dollar_volume = None
        allowed_market_classes = ["equity"]
        require_etf_asset_type = False
    return deep_merge(
        DEFAULT_WORKFLOW_SETTINGS,
        {
            "source_universe_csv": source_universe_csv,
            "allowed_market_classes": allowed_market_classes,
            "require_etf_asset_type": require_etf_asset_type,
            "min_avg_dollar_volume": min_avg_dollar_volume,
            "min_size_metric": config.get("screen", {}).get("min_size_metric"),
        },
    )


def format_number(value: float | int | None) -> str:
    if value is None:
        return "n/a"
    numeric = float(value)
    for suffix, scale in (("T", 1e12), ("B", 1e9), ("M", 1e6)):
        if abs(numeric) >= scale:
            return f"{numeric / scale:.2f}{suffix}"
    if numeric.is_integer():
        return f"{int(numeric):,}"
    return f"{numeric:.2f}"


def preview_rows(
    df: pd.DataFrame,
    columns: list[str],
    limit: int | None = 20,
) -> list[dict[str, Any]]:
    if df.empty:
        return []
    output: list[dict[str, Any]] = []
    selected = [column for column in columns if column in df.columns]
    rows = df if limit is None else df.head(limit)
    for _, row in rows.iterrows():
        output.append({column: _json_value(row.get(column)) for column in selected})
    return output


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, (str, bool, int, float)):
        return value
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def resolve_data_path(base_dir: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (base_dir / value).resolve()


def classify_market_bucket(row: pd.Series) -> str:
    category_group = str(row.get("category_group") or "").strip()
    text = f"{row.get('name', '')} {row.get('summary', '')}"
    asset_type = str(row.get("asset_type") or "").strip().upper()
    has_equity_text = bool(EQUITY_TEXT_PATTERN.search(text))
    has_non_equity_text = bool(NON_EQUITY_TEXT_PATTERN.search(text))
    has_commodity_text = bool(COMMODITY_TEXT_PATTERN.search(text))

    if asset_type == "EQUITY":
        return "equity"
    if asset_type == "ETF" and category_group in EQUITY_CATEGORY_GROUPS:
        return "equity"
    if category_group == "Commodities" or has_commodity_text:
        return "commodity"
    if "fixed income" in category_group.lower() or any(
        token in text.lower() for token in ["bond", "treasury", "fixed income", "credit", "muni", "municipal"]
    ):
        return "fixed_income"
    if category_group == "Currencies" or any(token in text.lower() for token in ["currency", "forex"]):
        return "currency"
    if any(token in text.lower() for token in ["bitcoin", "crypto"]):
        return "crypto"
    if category_group in {"Alternatives", "Derivatives"}:
        return "alternative"
    if category_group in NON_EQUITY_CATEGORY_GROUPS:
        return "other"
    if category_group in EQUITY_CATEGORY_GROUPS:
        return "equity" if (not has_non_equity_text or has_equity_text) else "other"
    if has_equity_text and not has_non_equity_text:
        return "equity"
    return "other"


def infer_universe_asset_type(source_universe_csv: str) -> str | None:
    universe_name = Path(source_universe_csv).name.lower()
    if "etf" in universe_name:
        return "ETF"
    if "sp500" in universe_name:
        return "EQUITY"
    return None


def enrich_candidate_universe_from_local_cache(
    merged: pd.DataFrame,
    *,
    base_dir: Path,
    source_universe_csv: str,
) -> tuple[pd.DataFrame, str | None]:
    enrichment_csv = infer_local_metrics_csv(source_universe_csv, base_dir=base_dir)
    if not enrichment_csv:
        return merged, None

    enrichment_path = resolve_data_path(base_dir, enrichment_csv)
    if not enrichment_path.exists():
        return merged, None

    enrichment_df = pd.read_csv(enrichment_path)
    if enrichment_df.empty or "ticker" not in enrichment_df.columns:
        return merged, None

    enrichment_columns = [
        column
        for column in ["ticker", "asset_type", "size_metric", "avg_dollar_volume"]
        if column in enrichment_df.columns
    ]
    if len(enrichment_columns) <= 1:
        return merged, None

    enriched = merged.merge(
        enrichment_df[enrichment_columns].drop_duplicates(subset=["ticker"]),
        left_on="symbol",
        right_on="ticker",
        how="left",
        suffixes=("", "__enriched"),
    )
    for column in ["asset_type", "size_metric", "avg_dollar_volume"]:
        enriched_name = f"{column}__enriched"
        if enriched_name not in enriched.columns:
            continue
        if column in enriched.columns:
            enriched[column] = enriched[column].combine_first(enriched[enriched_name])
            enriched = enriched.drop(columns=[enriched_name])
        else:
            enriched = enriched.rename(columns={enriched_name: column})
    if "ticker" in enriched.columns:
        enriched = enriched.drop(columns=["ticker"])
    return enriched, enrichment_csv


def allowed_market_buckets(workflow_settings: dict[str, Any]) -> set[str]:
    values = workflow_settings.get("allowed_market_classes")
    if isinstance(values, str):
        parsed = [item.strip() for item in values.split(",") if item.strip()]
        if parsed:
            return set(parsed)
    if isinstance(values, list):
        parsed = [str(item).strip() for item in values if str(item).strip()]
        if parsed:
            return set(parsed)

    scope = str(workflow_settings.get("market_scope") or "all")
    if scope == "equity":
        return {"equity"}
    if scope == "commodity":
        return {"commodity"}
    if scope == "equity_commodity":
        return {"equity", "commodity"}
    return {"equity", "commodity", "fixed_income", "currency", "alternative", "crypto", "other"}


def create_workflow_dir(base_dir: Path, config_name: str) -> Path:
    slug = Path(config_name).stem.replace("_config", "")
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    workflow_dir = (base_dir / "out" / "workflow_runs" / f"{timestamp}-{slug}").resolve()
    workflow_dir.mkdir(parents=True, exist_ok=True)
    return workflow_dir


def read_workflow_state(workflow_dir: Path) -> dict[str, Any]:
    state_path = workflow_dir / "workflow_state.json"
    if not state_path.exists():
        return {}
    return json.loads(state_path.read_text(encoding="utf-8"))


def write_workflow_state(workflow_dir: Path, state: dict[str, Any]) -> None:
    state_path = workflow_dir / "workflow_state.json"
    state_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def candidate_columns() -> list[str]:
    return [
        "symbol",
        "name",
        "market_bucket",
        "avg_dollar_volume",
        "size_metric",
        "candidate_fail_reasons",
    ]


def candidate_review_columns() -> list[str]:
    return [
        "symbol",
        "name",
        "candidate_status",
        "candidate_fail_reasons",
        "market_bucket",
        "category_group",
        "avg_dollar_volume",
        "size_metric",
        "asset_type",
    ]


def hard_filter_columns() -> list[str]:
    return [
        "ticker",
        "status",
        "fail_reasons",
        "selected_expiration",
        "selected_dte",
        "price",
        "selected_strike",
        "selected_call_otm_pct",
        "selected_call_bid",
        "selected_call_ask",
        "dividend_yield",
        "annualized_call_yield",
        "selected_call_open_interest",
        "selected_call_volume",
        "selected_call_spread_pct",
        "beta_1y",
        "realized_vol_1y",
        "score_total",
    ]


def rank_columns() -> list[str]:
    return [
        "ticker",
        "score_total",
        "selected_expiration",
        "selected_dte",
        "price",
        "selected_strike",
        "selected_call_otm_pct",
        "selected_call_bid",
        "selected_call_ask",
        "dividend_yield",
        "annualized_call_yield",
        "combined_income_score_proxy",
        "selected_call_open_interest",
        "selected_call_volume",
        "selected_call_spread_pct",
        "score_dividend",
        "score_call",
        "score_liquidity",
        "score_quality",
        "score_risk_fit",
    ]


def summarize_reason_counts(series: pd.Series, limit: int = 8) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for raw_value in series.fillna("").astype(str):
        for item in raw_value.split(";"):
            reason = item.strip()
            if not reason:
                continue
            counts[reason] = counts.get(reason, 0) + 1
    ranked = sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))
    return [{"reason": reason, "count": count} for reason, count in ranked[:limit]]


def run_candidate_pool_step(
    base_dir: Path,
    workflow_dir: Path,
    workflow_settings: dict[str, Any],
    config_name: str,
) -> dict[str, Any]:
    universe_path = resolve_data_path(base_dir, workflow_settings["source_universe_csv"])
    universe_df = pd.read_csv(universe_path)
    merged = universe_df.copy()
    if "symbol" not in merged.columns:
        raise ValueError("Candidate step expects a universe CSV with a 'symbol' column.")
    enrichment_csv: str | None = None

    if "asset_type" not in merged.columns or "size_metric" not in merged.columns:
        merged, enrichment_csv = enrich_candidate_universe_from_local_cache(
            merged,
            base_dir=base_dir,
            source_universe_csv=workflow_settings["source_universe_csv"],
        )

    inferred_asset_type = infer_universe_asset_type(workflow_settings["source_universe_csv"])
    if inferred_asset_type:
        if "asset_type" not in merged.columns:
            merged["asset_type"] = inferred_asset_type
        else:
            merged["asset_type"] = merged["asset_type"].fillna(inferred_asset_type)

    merged["market_bucket"] = merged.apply(classify_market_bucket, axis=1)
    size_median = (
        float(merged["size_metric"].dropna().median())
        if "size_metric" in merged.columns and merged["size_metric"].dropna().any()
        else None
    )
    liquidity_median = (
        float(merged["avg_dollar_volume"].dropna().median())
        if "avg_dollar_volume" in merged.columns and merged["avg_dollar_volume"].dropna().any()
        else None
    )
    has_size_data = "size_metric" in merged.columns and merged["size_metric"].notna().any()
    has_adv_data = "avg_dollar_volume" in merged.columns and merged["avg_dollar_volume"].notna().any()
    warnings: list[str] = []
    if workflow_settings.get("min_size_metric") not in ("", None) and not has_size_data:
        warnings.append("当前 Universe 不含可用的 size_metric，已自动忽略“最小规模”条件。")
    if workflow_settings.get("min_avg_dollar_volume") not in ("", None) and not has_adv_data:
        warnings.append("当前 Universe 不含可用的日均成交额，已自动忽略“最低日均成交额”条件。")

    allowed_buckets = allowed_market_buckets(workflow_settings)
    statuses: list[str] = []
    reasons: list[str] = []

    for _, row in merged.iterrows():
        failures: list[str] = []
        if workflow_settings.get("require_usd") and "currency" in merged.columns:
            if str(row.get("currency") or "").strip().upper() != "USD":
                failures.append("currency!=USD")
        if workflow_settings.get("require_us_listing") and "exchange" in merged.columns:
            if str(row.get("exchange") or "").strip().upper() not in US_MAJOR_EXCHANGES:
                failures.append("exchange_not_us_major")
        if row.get("market_bucket") not in allowed_buckets:
            failures.append(f"market_bucket_not_allowed:{row.get('market_bucket')}")
        if workflow_settings.get("require_etf_asset_type"):
            asset_type = str(row.get("asset_type") or "").strip().upper()
            if asset_type and asset_type != "ETF":
                failures.append(f"asset_type_not_etf:{asset_type}")
        size_metric = row.get("size_metric")
        avg_dollar_volume = row.get("avg_dollar_volume")
        min_size = workflow_settings.get("min_size_metric")
        if min_size not in ("", None) and has_size_data:
            min_size_value = float(min_size)
            if pd.isna(size_metric) or float(size_metric) < min_size_value:
                failures.append(f"size_metric<{min_size_value}")
        min_adv = workflow_settings.get("min_avg_dollar_volume")
        if min_adv not in ("", None) and has_adv_data:
            min_adv_value = float(min_adv)
            if pd.isna(avg_dollar_volume) or float(avg_dollar_volume) < min_adv_value:
                failures.append(f"avg_dollar_volume<{min_adv_value}")

        statuses.append("PASS" if not failures else "FAIL")
        reasons.append(";".join(failures))

    merged["candidate_status"] = statuses
    merged["candidate_fail_reasons"] = reasons

    candidate_df = merged[merged["candidate_status"] == "PASS"].copy()
    sort_columns = [column for column in ["avg_dollar_volume", "size_metric"] if column in candidate_df.columns]
    if sort_columns:
        candidate_df = candidate_df.sort_values(
            by=sort_columns,
            ascending=[False] * len(sort_columns),
            na_position="last",
        )
    candidate_tickers = candidate_df["symbol"].astype(str).tolist()

    review_path = workflow_dir / "step1_candidate_review.csv"
    universe_out_path = workflow_dir / "step1_candidate_universe.csv"
    tickers_out_path = workflow_dir / "step1_candidate_tickers.txt"
    settings_out_path = workflow_dir / "step1_candidate_settings.json"

    merged.to_csv(review_path, index=False)
    candidate_df.to_csv(universe_out_path, index=False)
    tickers_out_path.write_text("\n".join(candidate_tickers) + "\n", encoding="utf-8")
    settings_out_path.write_text(
        json.dumps(
            {
                "config_name": config_name,
                "workflow_settings": workflow_settings,
                "size_median": size_median,
                "liquidity_median": liquidity_median,
                "local_enrichment_csv": enrichment_csv,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    summary = {
        "source_universe_rows": int(len(universe_df)),
        "candidate_rows": int(len(candidate_df)),
        "status_counts": {
            str(key): int(value)
            for key, value in merged["candidate_status"].value_counts().to_dict().items()
        },
        "size_median": size_median,
        "liquidity_median": liquidity_median,
        "warnings": warnings,
        "artifacts": {
            "review_csv": str(review_path),
            "candidate_universe_csv": str(universe_out_path),
            "candidate_tickers_txt": str(tickers_out_path),
            "settings_json": str(settings_out_path),
            "local_enrichment_csv": enrichment_csv,
        },
        "preview_rows": preview_rows(candidate_df, candidate_columns(), limit=None),
        "review_preview_rows": preview_rows(merged, candidate_review_columns(), limit=None),
        "top_fail_reasons": summarize_reason_counts(merged["candidate_fail_reasons"]),
    }

    state = read_workflow_state(workflow_dir)
    state["workflow_dir"] = str(workflow_dir)
    state["config_name"] = config_name
    state["settings"] = workflow_settings
    state["candidate_pool"] = summary
    write_workflow_state(workflow_dir, state)
    return summary


def run_hard_filter_step(
    base_dir: Path,
    workflow_dir: Path,
    config: dict[str, Any],
    config_path: Path,
    workflow_settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    tickers_path = workflow_dir / "step1_candidate_tickers.txt"
    if not tickers_path.exists():
        raise FileNotFoundError("Run candidate-pool step first.")

    tickers = [
        line.strip().upper()
        for line in tickers_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    metrics_path = workflow_dir / "step2_candidate_metrics.csv"
    candidate_universe_path = workflow_dir / "step1_candidate_universe.csv"
    step2_reuse_cache = bool((workflow_settings or {}).get("step2_reuse_same_day_cache", False))
    market_data_source = str(config.get("data", {}).get("market_data_source", "yahoo") or "yahoo").strip().lower()
    ibkr_cfg = config.get("data", {}).get("ibkr", {}) or {}
    ibkr_connection = None
    if market_data_source in {"ibkr", "hybrid"}:
        ibkr_connection = f'{ibkr_cfg.get("host", "127.0.0.1")}:{ibkr_cfg.get("port", 4001)}'
    reusable_metrics_paths = [resolve_metrics_path(config, config_path, None)] if step2_reuse_cache else []
    metadata_by_ticker: dict[str, dict[str, Any]] = {}
    if candidate_universe_path.exists():
        candidate_universe_df = pd.read_csv(candidate_universe_path)
        if "symbol" in candidate_universe_df.columns:
            for _, row in candidate_universe_df.iterrows():
                ticker = str(row.get("symbol") or "").strip().upper()
                if not ticker:
                    continue
                metadata_by_ticker[ticker] = {
                    "asset_type": row.get("asset_type"),
                    "size_metric_name": row.get("size_metric_name"),
                    "size_metric": row.get("size_metric"),
                }
    step2_config = deep_merge(
        config,
        {
            "data": {
                "resume_existing_metrics": step2_reuse_cache,
            }
        },
    )

    if tickers:
        metrics_df = refresh_metrics_for_tickers(
            config=step2_config,
            config_path=config_path,
            tickers=tickers,
            output_path=metrics_path,
            reusable_metrics_paths=reusable_metrics_paths,
            metadata_by_ticker=metadata_by_ticker,
        )
    else:
        metrics_df = pd.DataFrame(columns=METRIC_COLUMNS)
        metrics_df.to_csv(metrics_path, index=False)

    screened = add_screen_columns(metrics_df, config)

    screen_out_path = workflow_dir / "step2_hard_filter.csv"
    pass_out_path = workflow_dir / "step2_hard_filter_pass.csv"
    config_out_path = workflow_dir / "step2_hard_filter_config.json"
    screened.to_csv(screen_out_path, index=False)
    screened[screened["status"] == "PASS"].to_csv(pass_out_path, index=False)
    config_out_path.write_text(
        json.dumps(
            {
                "data": {
                    "market_data_source": market_data_source,
                    "ibkr": {
                        "host": ibkr_cfg.get("host", "127.0.0.1"),
                        "port": ibkr_cfg.get("port", 4001),
                        "readonly": bool(ibkr_cfg.get("readonly", True)),
                        "timeout": ibkr_cfg.get("timeout", 10),
                    },
                },
                "screen": config["screen"],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    counts = {
        str(key): int(value)
        for key, value in screened["status"].value_counts().to_dict().items()
    }
    summary = {
        "rows": int(len(screened)),
        "status_counts": counts,
        "artifacts": {
            "candidate_metrics_csv": str(metrics_path),
            "hard_filter_csv": str(screen_out_path),
            "hard_filter_pass_csv": str(pass_out_path),
            "screen_config_json": str(config_out_path),
        },
        "market_data_source": market_data_source,
        "ibkr_connection": ibkr_connection,
        "step2_reuse_same_day_cache": step2_reuse_cache,
        "preview_rows": preview_rows(screened, hard_filter_columns(), limit=None),
        "top_fail_reasons": summarize_reason_counts(screened["fail_reasons"]),
    }

    state = read_workflow_state(workflow_dir)
    if workflow_settings:
        state["settings"] = workflow_settings
    state["hard_filter"] = summary
    write_workflow_state(workflow_dir, state)
    return summary


def run_ranking_step(
    workflow_dir: Path,
    config: dict[str, Any],
) -> dict[str, Any]:
    metrics_path = workflow_dir / "step2_candidate_metrics.csv"
    hard_filter_config_path = workflow_dir / "step2_hard_filter_config.json"
    if not metrics_path.exists():
        raise FileNotFoundError("Run hard-filter step first.")
    if not hard_filter_config_path.exists():
        raise FileNotFoundError("Run hard-filter step first.")

    hard_filter_config = json.loads(hard_filter_config_path.read_text(encoding="utf-8"))
    ranking_config = deep_merge(config, {"screen": hard_filter_config["screen"]})
    metrics_df = pd.read_csv(metrics_path)
    rescored = add_screen_columns(metrics_df, ranking_config)
    ranked = rescored[rescored["status"] == "PASS"].copy()

    ranked = ranked.sort_values(
        by=["score_total", "combined_income_score_proxy", "annualized_call_yield", "dividend_yield"],
        ascending=[False, False, False, False],
        na_position="last",
    )

    ranked_out_path = workflow_dir / "step3_ranked.csv"
    scoring_out_path = workflow_dir / "step3_scoring_config.json"
    ranked.to_csv(ranked_out_path, index=False)
    scoring_out_path.write_text(
        json.dumps(
            {"scoring": config["scoring"]},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    summary = {
        "rows": int(len(ranked)),
        "artifacts": {
            "ranked_csv": str(ranked_out_path),
            "scoring_config_json": str(scoring_out_path),
        },
        "preview_rows": preview_rows(ranked, rank_columns(), limit=None),
    }
    state = read_workflow_state(workflow_dir)
    state["ranking"] = summary
    write_workflow_state(workflow_dir, state)
    return summary
