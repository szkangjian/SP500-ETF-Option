#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import io
import json
import numbers
import os
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd

from covered_call_screener import (
    deep_merge,
    load_config,
    refresh_metrics,
    resolve_otm_preferences,
    resolve_metrics_path,
    resolve_screen_path,
    run_screen,
)
from screening_workflow import (
    create_workflow_dir,
    default_workflow_settings,
    read_workflow_state,
    run_candidate_pool_step,
    run_hard_filter_step,
    run_ranking_step,
)
import yaml_compat as yaml


BASE_DIR = Path(__file__).resolve().parent
MAX_LOG_CHARS = 80_000
EXCLUDED_DATA_FILENAMES = {"requirements.txt"}
PREVIEW_COLUMNS = [
    "ticker",
    "status",
    "score_total",
    "dividend_yield",
    "annualized_call_yield",
    "combined_income_score_proxy",
    "selected_call_open_interest",
    "selected_call_volume",
    "selected_call_spread_pct",
    "beta_1y",
    "realized_vol_1y",
    "fail_reasons",
]


@dataclass
class JobState:
    job_id: str
    mode: str
    config_name: str
    status: str = "running"
    started_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    log_text: str = ""
    result: dict[str, Any] | None = None
    error: str | None = None


class JobLogStream(io.TextIOBase):
    def __init__(self, job: JobState, lock: threading.Lock) -> None:
        self.job = job
        self.lock = lock

    def write(self, text: str) -> int:
        if not text:
            return 0
        with self.lock:
            self.job.log_text = (self.job.log_text + text)[-MAX_LOG_CHARS:]
            self.job.updated_at = time.time()
        return len(text)

    def flush(self) -> None:
        return None


JOB_LOCK = threading.Lock()
JOBS: dict[str, JobState] = {}
JOB_COUNTER = 0


def env_flag(name: str, default: bool = False) -> bool:
    value = str(os.getenv(name, "")).strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


RENDER_ENV = env_flag("RENDER", default=False)
READ_ONLY_MODE = env_flag("COVERED_CALL_READ_ONLY", default=RENDER_ENV)
DEPLOY_TARGET = "render" if RENDER_ENV else "local"

PRIMARY_CONFIGS = ["etf_broad_config.yaml", "sp500_config.yaml"]
PRIMARY_UNIVERSE_FILES = [
    "finance_etfs.csv",
    "etf_broad_universe.csv",
    "sp500_constituents.csv",
]
PRIMARY_OUTPUT_FILES = [
    "out/etf_broad_metrics.csv",
    "out/etf_broad_screen.csv",
    "out/sp500_metrics.csv",
    "out/sp500_screen.csv",
]


def cloud_job_block_reason(mode: str, config_name: str, config_payload: dict[str, Any] | None) -> str | None:
    if not READ_ONLY_MODE:
        return None
    if mode in {"workflow_step1", "workflow_step3"}:
        return None
    if mode == "workflow_step2":
        runtime_config = normalize_runtime_config(config_payload or {}, config_name)
        source = str(runtime_config.get("data", {}).get("market_data_source", "yahoo") or "yahoo").strip().lower()
        if source == "yahoo":
            return None
        return "Render 云端模式只允许 Yahoo 数据源的 Step 2；IB / hybrid 请在本地运行。"
    return "Render 云端模式只开放正式三步流程里的 Step 1、Yahoo Step 2 和 Step 3。"
VISIBLE_WORKFLOW_FILES = {
    "step1_candidate_review.csv",
    "step1_candidate_universe.csv",
    "step2_candidate_metrics.csv",
    "step2_hard_filter.csv",
    "step2_hard_filter_pass.csv",
    "step3_ranked.csv",
}


def discover_configs() -> list[str]:
    available = {path.name for path in BASE_DIR.glob("*.yaml")}
    return [name for name in PRIMARY_CONFIGS if name in available]


def _existing_relative_path(name: str) -> str | None:
    path = (BASE_DIR / name).resolve()
    if path.exists():
        return str(path.relative_to(BASE_DIR))
    return None


def _config_linked_files(config: dict[str, Any] | None) -> set[str]:
    names: set[str] = set()
    if not config:
        return names

    universe_value = str(config.get("universe", {}).get("path") or "").strip()
    if universe_value:
        path = Path(universe_value)
        if not path.is_absolute():
            candidate = (BASE_DIR / universe_value).resolve()
            if candidate.exists():
                names.add(str(candidate.relative_to(BASE_DIR)))

    cache_dir = str(config.get("data", {}).get("cache_dir") or "").strip()
    metrics_filename = str(config.get("data", {}).get("metrics_filename") or "").strip()
    screened_filename = str(config.get("data", {}).get("screened_filename") or "").strip()
    if cache_dir and metrics_filename:
        candidate = (BASE_DIR / cache_dir / metrics_filename).resolve()
        if candidate.exists():
            names.add(str(candidate.relative_to(BASE_DIR)))
    if cache_dir and screened_filename:
        candidate = (BASE_DIR / cache_dir / screened_filename).resolve()
        if candidate.exists():
            names.add(str(candidate.relative_to(BASE_DIR)))
    return names


def _is_valid_workflow_artifact(path: Path) -> bool:
    parts = path.relative_to(BASE_DIR).parts
    if len(parts) < 3 or parts[:2] != ("out", "workflow_runs"):
        return False
    for index in range(2, len(parts) - 1):
        if parts[index] == "out" and parts[index + 1] == "workflow_runs":
            return False
    return True


def _workflow_run_key(config_name: str | None) -> str | None:
    if not config_name:
        return None
    stem = Path(config_name).stem
    if stem.endswith("_config"):
        stem = stem[:-7]
    return stem or None


def _latest_workflow_run_dir(config_name: str | None) -> Path | None:
    run_key = _workflow_run_key(config_name)
    if not run_key:
        return None
    workflow_root = BASE_DIR / "out" / "workflow_runs"
    if not workflow_root.exists():
        return None
    candidates = [
        path
        for path in workflow_root.iterdir()
        if path.is_dir() and path.name.endswith(f"-{run_key}")
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.name, path.stat().st_mtime))


def _workflow_artifact_files(config_name: str | None = None) -> list[str]:
    run_dir = _latest_workflow_run_dir(config_name)
    if run_dir is None:
        return []
    names: set[str] = set()
    for path in run_dir.rglob("*.csv"):
        if _is_valid_workflow_artifact(path) and path.name in VISIBLE_WORKFLOW_FILES:
            names.add(str(path.relative_to(BASE_DIR)))
    return sorted(names)


def discover_universe_files(config: dict[str, Any] | None = None) -> list[str]:
    names: set[str] = set()
    for name in PRIMARY_UNIVERSE_FILES:
        existing = _existing_relative_path(name)
        if existing:
            names.add(existing)
    names.update(
        name
        for name in _config_linked_files(config)
        if Path(name).suffix.lower() == ".csv" and not name.startswith("out/")
    )
    return sorted(names)


def discover_data_files(
    config: dict[str, Any] | None = None,
    config_name: str | None = None,
) -> list[str]:
    names: set[str] = set()
    for name in PRIMARY_UNIVERSE_FILES + PRIMARY_OUTPUT_FILES:
        existing = _existing_relative_path(name)
        if existing:
            names.add(existing)
    names.update(
        name
        for name in _config_linked_files(config)
        if Path(name).suffix.lower() == ".csv"
    )
    names.update(_workflow_artifact_files(config_name=config_name))
    return sorted(names)


def describe_data_file(name: str) -> dict[str, str]:
    path = Path(name)
    filename = path.name
    stem = path.stem
    suffix = path.suffix.lower()

    category = "other"
    label = "其他数据文件"
    description = "项目里的数据文件。"

    if filename == "finance_etfs.csv":
        category = "universe_source"
        label = "ETF 主数据库快照"
        description = "广覆盖 ETF 扩池的源数据，包含代码、名称、交易所、币种、成交额等基础字段。"
    elif filename == "sp500_constituents.csv":
        category = "universe_source"
        label = "S&P 500 成分股快照"
        description = "本地保存的 S&P 500 成分股列表，用来生成美股个股候选池。"
    elif path.parts[:2] == ("out", "workflow_runs"):
        category = "workflow"
        workflow_map = {
            "step1_candidate_review.csv": ("Step 1 审查表", "Step 1 每只标的为什么保留或剔除。"),
            "step1_candidate_universe.csv": ("Step 1 候选池", "Step 1 通过后的候选池。"),
            "step2_candidate_metrics.csv": ("Step 2 抓取结果", "只对 Step 1 候选池抓取后的每标的一行结果。"),
            "step2_hard_filter.csv": ("Step 2 全量结果", "Step 2 的 PASS / FAIL / ERROR 全表。"),
            "step2_hard_filter_pass.csv": ("Step 2 通过名单", "仅保留 Step 2 通过样本。"),
            "step3_ranked.csv": ("Step 3 排序结果", "最终排序榜单。"),
        }
        if filename in workflow_map:
            label, description = workflow_map[filename]
        else:
            label = "Workflow 过程文件"
            description = "三步流程运行时保存的中间产物。"
    elif suffix == ".csv" and filename.endswith("_universe.csv"):
        category = "universe"
        label = "候选池 CSV"
        notes: list[str] = []
        if "broad" in stem:
            notes.append("广覆盖 ETF 扩池版本")
        if "equity_commodity" in stem:
            notes.append("美元交易的股票 ETF + 商品 ETF")
        elif "equity" in stem:
            notes.append("美元交易的股票 ETF")
        elif "commodity" in stem:
            notes.append("商品 ETF")
        if "median_100m" in stem:
            notes.append("在中位数过滤后，再要求日均成交额至少 1 亿美元")
        elif "median" in stem:
            notes.append("只保留 size_metric 和日均成交额都不低于中位数")
        description = "候选池清单。" + ("；".join(notes) if notes else "列出当前阶段保留的标的和基础字段。")
    elif suffix == ".csv" and filename.endswith("_metrics.csv"):
        category = "metrics"
        label = "计算结果 CSV"
        description = "某一批 ticker 的每标的一行计算结果，包含价格、分红、波动率、期权链等字段。"
    elif suffix == ".csv" and filename.endswith("_screen.csv"):
        category = "screen"
        label = "筛选结果 CSV"
        description = "硬过滤加打分后的结果表。通常包含 status、fail_reasons、score_total 和前排候选。"
    elif suffix == ".csv" and "sample" in stem:
        category = "sample"
        label = "样例数据"
        description = "小样本示例文件，用来快速验证筛选逻辑。"
    elif suffix == ".csv" and "test" in stem:
        category = "sample"
        label = "测试数据"
        description = "用于自动化测试或手动小范围验证的数据文件。"
    elif suffix == ".txt" and filename == "starter_universe.txt":
        category = "universe"
        label = "起步候选池"
        description = "项目最初的小型 ticker 列表，适合最小范围调试，不适合拿来做 Step 1 的广覆盖候选池。"

    return {
        "name": name,
        "category": category,
        "label": label,
        "description": description,
    }


def discover_data_file_catalog(
    config: dict[str, Any] | None = None,
    config_name: str | None = None,
) -> list[dict[str, str]]:
    return [describe_data_file(name) for name in discover_data_files(config=config, config_name=config_name)]


def resolve_local_config_path(name: str) -> Path:
    candidate = Path(name).name
    if not candidate.endswith((".yaml", ".yml")):
        raise ValueError("Config filename must end with .yaml or .yml")
    config_path = (BASE_DIR / candidate).resolve()
    if config_path.parent != BASE_DIR:
        raise ValueError("Config must live in the project root")
    return config_path


def to_json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, numbers.Number):
        if isinstance(value, float) and pd.isna(value):
            return None
        return value.item() if hasattr(value, "item") else value
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return [to_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): to_json_value(item) for key, item in value.items()}
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError:
            pass
    return str(value)


def build_preview_rows(df: pd.DataFrame, limit: int = 30) -> list[dict[str, Any]]:
    if df.empty:
        return []
    columns = [column for column in PREVIEW_COLUMNS if column in df.columns]
    if not columns:
        return []
    rows: list[dict[str, Any]] = []
    for _, row in df.head(limit).iterrows():
        rows.append({column: to_json_value(row.get(column)) for column in columns})
    return rows


def summarize_metrics(metrics_df: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "rows": int(len(metrics_df)),
        "error_rows": 0,
        "as_of_dates": [],
    }
    if "error" in metrics_df.columns:
        summary["error_rows"] = int(metrics_df["error"].fillna("").astype(str).str.strip().ne("").sum())
    if "as_of_date" in metrics_df.columns:
        dates = (
            metrics_df["as_of_date"]
            .dropna()
            .astype(str)
            .sort_values()
            .unique()
            .tolist()
        )
        summary["as_of_dates"] = dates[-5:]
    return summary


def summarize_screen(screen_df: pd.DataFrame) -> dict[str, Any]:
    counts = (
        screen_df["status"].value_counts().to_dict()
        if "status" in screen_df.columns
        else {}
    )
    return {
        "rows": int(len(screen_df)),
        "status_counts": {str(key): int(value) for key, value in counts.items()},
        "preview_rows": build_preview_rows(screen_df, limit=30),
    }


def file_metadata(path: Path) -> dict[str, Any]:
    exists = path.exists()
    payload: dict[str, Any] = {
        "path": str(path),
        "exists": exists,
        "updated_at": None,
    }
    if exists:
        payload["updated_at"] = datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
    return payload


def collect_config_state(config_name: str, runtime_config: dict[str, Any] | None = None) -> dict[str, Any]:
    config_path = resolve_local_config_path(config_name)
    config = runtime_config or load_config(config_path)
    metrics_path = resolve_metrics_path(config, config_path, None)
    screen_path = resolve_screen_path(config, config_path, None)

    artifacts: dict[str, Any] = {
        "config_path": str(config_path),
        "metrics": file_metadata(metrics_path),
        "screen": file_metadata(screen_path),
    }

    preview_rows: list[dict[str, Any]] = []
    if metrics_path.exists():
        metrics_df = pd.read_csv(metrics_path)
        artifacts["metrics"]["summary"] = summarize_metrics(metrics_df)
    if screen_path.exists():
        screen_df = pd.read_csv(screen_path)
        screen_summary = summarize_screen(screen_df)
        artifacts["screen"]["summary"] = {
            "rows": screen_summary["rows"],
            "status_counts": screen_summary["status_counts"],
        }
        preview_rows = screen_summary["preview_rows"]

    return {
        "config_name": config_name,
        "config": to_json_value(config),
        "artifacts": artifacts,
        "preview_rows": preview_rows,
        "deployment": {
            "target": DEPLOY_TARGET,
            "read_only_mode": READ_ONLY_MODE,
            "job_policy": "cloud_safe" if READ_ONLY_MODE else "full",
        },
        "config_options": discover_configs(),
        "universe_options": discover_universe_files(config=config),
        "data_file_options": discover_data_files(config=config, config_name=config_name),
        "data_file_catalog": discover_data_file_catalog(config=config, config_name=config_name),
        "workflow_defaults": default_workflow_settings(config, base_dir=BASE_DIR),
    }


def normalize_runtime_config(payload: dict[str, Any], config_name: str) -> dict[str, Any]:
    base_config = load_config(resolve_local_config_path(config_name))
    merged = deep_merge(base_config, payload)
    return sanitize_config_payload(merged)


def sanitize_config_payload(config: dict[str, Any]) -> dict[str, Any]:
    merged = deep_merge({}, config)

    as_of_date = str(merged["data"].get("as_of_date") or "").strip()
    merged["data"]["as_of_date"] = as_of_date or None

    limit = merged["universe"].get("limit")
    if limit in ("", None):
        merged["universe"]["limit"] = None
    else:
        merged["universe"]["limit"] = int(limit)

    allowed_asset_types = merged["screen"].get("allowed_asset_types")
    if isinstance(allowed_asset_types, str):
        merged["screen"]["allowed_asset_types"] = [
            item.strip() for item in allowed_asset_types.split(",") if item.strip()
        ]
    elif not allowed_asset_types:
        merged["screen"]["allowed_asset_types"] = []

    min_otm_pct, max_otm_pct, preferred_otm_pct = resolve_otm_preferences(merged["screen"])
    merged["screen"]["min_otm_pct"] = min_otm_pct
    merged["screen"]["max_otm_pct"] = max_otm_pct
    merged["screen"]["preferred_otm_pct"] = preferred_otm_pct

    return merged


def next_job_id() -> str:
    global JOB_COUNTER
    with JOB_LOCK:
        JOB_COUNTER += 1
        return str(JOB_COUNTER)


def snapshot_job(job: JobState) -> dict[str, Any]:
    return {
        "job_id": job.job_id,
        "mode": job.mode,
        "config_name": job.config_name,
        "status": job.status,
        "started_at": datetime.fromtimestamp(job.started_at).isoformat(timespec="seconds"),
        "updated_at": datetime.fromtimestamp(job.updated_at).isoformat(timespec="seconds"),
        "finished_at": (
            datetime.fromtimestamp(job.finished_at).isoformat(timespec="seconds")
            if job.finished_at
            else None
        ),
        "log_text": job.log_text,
        "result": job.result,
        "error": job.error,
    }


def run_job(job_id: str, mode: str, config_name: str, payload_config: dict[str, Any]) -> None:
    with JOB_LOCK:
        job = JOBS[job_id]
    config_path = resolve_local_config_path(config_name)
    workflow_payload = payload_config.pop("workflow", None) if isinstance(payload_config, dict) else None
    runtime_config = normalize_runtime_config(payload_config, config_name)
    stream = JobLogStream(job, JOB_LOCK)
    workflow_dir: Path | None = None

    try:
        with contextlib.redirect_stdout(stream), contextlib.redirect_stderr(stream):
            print(f"Starting {mode} with {config_name}", flush=True)
            if mode == "screen":
                run_screen(
                    config=runtime_config,
                    config_path=config_path,
                    input_override=None,
                    output_override=None,
                    top_n_override=None,
                )
            elif mode == "refresh":
                refresh_metrics(runtime_config, config_path)
                metrics_path = resolve_metrics_path(runtime_config, config_path, None)
                print(f"Saved raw metrics to {metrics_path}", flush=True)
            elif mode == "run":
                refreshed = refresh_metrics(runtime_config, config_path)
                metrics_path = resolve_metrics_path(runtime_config, config_path, None)
                print(f"Saved raw metrics to {metrics_path}", flush=True)
                if refreshed.empty:
                    print("Metrics dataframe is empty after refresh.", flush=True)
                run_screen(
                    config=runtime_config,
                    config_path=config_path,
                    input_override=str(metrics_path),
                    output_override=None,
                    top_n_override=None,
                )
            elif mode == "workflow_step1":
                workflow_settings = workflow_payload or default_workflow_settings(runtime_config, base_dir=BASE_DIR)
                workflow_dir = create_workflow_dir(BASE_DIR, config_name)
                print(f"Workflow dir: {workflow_dir}", flush=True)
                run_candidate_pool_step(
                    base_dir=BASE_DIR,
                    workflow_dir=workflow_dir,
                    workflow_settings=workflow_settings,
                    config_name=config_name,
                )
            elif mode == "workflow_step2":
                workflow_dir_value = str((workflow_payload or {}).get("workflow_dir") or "").strip()
                if not workflow_dir_value:
                    raise ValueError("workflow_dir is required for workflow_step2")
                workflow_dir = Path(workflow_dir_value).resolve()
                print(f"Workflow dir: {workflow_dir}", flush=True)
                run_hard_filter_step(
                    base_dir=BASE_DIR,
                    workflow_dir=workflow_dir,
                    config=runtime_config,
                    config_path=config_path,
                    workflow_settings=(workflow_payload or {}).get("settings"),
                )
            elif mode == "workflow_step3":
                workflow_dir_value = str((workflow_payload or {}).get("workflow_dir") or "").strip()
                if not workflow_dir_value:
                    raise ValueError("workflow_dir is required for workflow_step3")
                workflow_dir = Path(workflow_dir_value).resolve()
                print(f"Workflow dir: {workflow_dir}", flush=True)
                run_ranking_step(
                    workflow_dir=workflow_dir,
                    config=runtime_config,
                )
            else:
                raise ValueError(f"Unsupported mode: {mode}")

        result = collect_config_state(config_name, runtime_config=runtime_config)
        if mode.startswith("workflow_step"):
            if workflow_dir is None:
                raise RuntimeError("Workflow job finished without a workflow_dir.")
            result["workflow_state"] = read_workflow_state(workflow_dir)
        with JOB_LOCK:
            job.status = "completed"
            job.updated_at = time.time()
            job.finished_at = job.updated_at
            job.result = result
    except Exception as exc:
        error_text = f"{exc}\n\n{traceback.format_exc()}"
        with JOB_LOCK:
            job.status = "failed"
            job.updated_at = time.time()
            job.finished_at = job.updated_at
            job.error = str(exc)
            job.log_text = (job.log_text + "\n" + error_text)[-MAX_LOG_CHARS:]


HTML_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Covered Call 调参界面</title>
  <style>
    :root {
      --bg: #f6f1e7;
      --panel: #fffdf8;
      --panel-strong: #f5efe0;
      --ink: #1c2333;
      --muted: #5f6778;
      --line: #d8cfbf;
      --brand: #1d6b5f;
      --brand-soft: #d9efe5;
      --warn: #9a5e16;
      --warn-soft: #f6ead7;
      --danger: #8f2f2f;
      --danger-soft: #f7dddd;
      --ok: #1f6a3b;
      --ok-soft: #dff2e6;
      --shadow: 0 18px 40px rgba(25, 35, 50, 0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "SF Pro Text", "PingFang SC", "Noto Sans SC", sans-serif;
      background:
        radial-gradient(circle at top left, #fff7df 0, transparent 35%),
        radial-gradient(circle at top right, #dff1ea 0, transparent 30%),
        var(--bg);
      color: var(--ink);
    }
    .page {
      max-width: 1480px;
      margin: 0 auto;
      padding: 24px;
    }
    .hero, .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: var(--shadow);
    }
    .hero {
      padding: 22px 24px;
      margin-bottom: 18px;
      display: grid;
      gap: 16px;
    }
    .hero-head {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      flex-wrap: wrap;
      align-items: center;
    }
    h1 {
      margin: 0;
      font-size: 30px;
      line-height: 1.1;
    }
    .sub {
      color: var(--muted);
      margin: 6px 0 0 0;
      max-width: 900px;
      line-height: 1.5;
    }
    .toolbar {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      align-items: end;
    }
    .workflow-panel {
      margin-bottom: 18px;
    }
    .workflow-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 14px;
    }
    .workflow-stack {
      display: grid;
      gap: 16px;
    }
    .workflow-card {
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 16px;
      background: linear-gradient(180deg, #fffdf8 0%, #f8f2e6 100%);
    }
    .workflow-card h3 {
      margin: 0 0 8px 0;
      font-size: 18px;
    }
    .workflow-card p {
      margin: 0 0 10px 0;
      color: var(--muted);
      line-height: 1.45;
      font-size: 14px;
    }
    .workflow-card.locked {
      opacity: 0.7;
    }
    .workflow-fields {
      display: grid;
      gap: 10px;
      margin: 12px 0;
    }
    .workflow-fields.two-col {
      grid-template-columns: repeat(2, minmax(0, 1fr));
      align-items: start;
    }
    .checkbox-group {
      display: flex;
      flex-wrap: wrap;
      gap: 8px 12px;
      margin-top: 4px;
    }
    .workflow-actions {
      display: flex;
      gap: 10px;
      margin-top: 12px;
      flex-wrap: wrap;
    }
    .workflow-summary {
      margin-top: 12px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.72);
      padding: 12px;
    }
    .workflow-summary h4 {
      margin: 0 0 8px 0;
      font-size: 14px;
    }
    .workflow-summary .row {
      font-size: 13px;
      color: var(--muted);
      margin: 4px 0;
      word-break: break-word;
    }
    .workflow-detail-panel {
      border: 1px solid var(--line);
      border-radius: 20px;
      background: rgba(255, 255, 255, 0.72);
      padding: 16px;
    }
    .workflow-detail-panel h3 {
      margin: 0 0 8px 0;
      font-size: 18px;
    }
    .workflow-detail-panel p {
      margin: 0 0 10px 0;
      color: var(--muted);
      line-height: 1.45;
      font-size: 14px;
    }
    .workflow-section {
      margin-top: 16px;
      padding-top: 16px;
      border-top: 1px solid #eadfcd;
    }
    .workflow-section:first-of-type {
      margin-top: 0;
      padding-top: 0;
      border-top: none;
    }
    .reason-list {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 10px 0 0 0;
    }
    .reason-pill {
      border-radius: 999px;
      border: 1px solid #eadfcd;
      background: #f8f2e6;
      padding: 6px 10px;
      font-size: 12px;
      color: var(--ink);
    }
    .workflow-next {
      font-size: 12px;
      color: var(--brand);
      font-weight: 700;
      letter-spacing: 0.02em;
      text-transform: uppercase;
    }
    .grid {
      display: grid;
      grid-template-columns: minmax(360px, 460px) minmax(0, 1fr);
      gap: 18px;
      align-items: start;
    }
    .panel {
      padding: 18px;
    }
    .panel h2 {
      margin: 0 0 8px 0;
      font-size: 20px;
    }
    .panel .hint {
      margin: 0 0 16px 0;
      color: var(--muted);
      line-height: 1.5;
    }
    details {
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 12px 14px;
      background: var(--panel-strong);
      margin-bottom: 12px;
    }
    details[open] {
      background: var(--panel);
    }
    summary {
      cursor: pointer;
      font-weight: 700;
      color: var(--ink);
      list-style: none;
    }
    summary::-webkit-details-marker {
      display: none;
    }
    .fields {
      display: grid;
      gap: 12px;
      margin-top: 12px;
    }
    .field {
      display: grid;
      gap: 6px;
    }
    .field.inline {
      grid-template-columns: 1fr auto;
      align-items: center;
    }
    .field.checkbox-field {
      gap: 8px;
    }
    .workflow-fields.two-col .field.checkbox-field.checkbox-full-row {
      grid-column: 1 / -1;
    }
    .checkbox-field-head {
      display: flex;
      justify-content: flex-start;
      align-items: center;
      flex-wrap: wrap;
      gap: 12px;
    }
    .checkbox-field-label {
      font-size: 13px;
      color: var(--muted);
      line-height: 1.45;
    }
    .checkbox-field .checkbox {
      flex-shrink: 0;
      white-space: nowrap;
    }
    label {
      font-size: 13px;
      color: var(--muted);
    }
    input, select, textarea, button {
      font: inherit;
    }
    input[type="text"],
    input[type="number"],
    input[type="date"],
    select,
    textarea {
      width: 100%;
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: white;
      color: var(--ink);
    }
    textarea {
      min-height: 110px;
      resize: vertical;
    }
    .checkbox {
      display: flex;
      align-items: center;
      gap: 8px;
      color: var(--ink);
      font-size: 14px;
    }
    button {
      border: none;
      border-radius: 999px;
      padding: 11px 16px;
      cursor: pointer;
      background: var(--ink);
      color: white;
      transition: transform 120ms ease, opacity 120ms ease;
    }
    button:hover {
      transform: translateY(-1px);
    }
    button.secondary {
      background: var(--brand);
    }
    button.soft {
      background: #e5e0d3;
      color: var(--ink);
    }
    button:disabled {
      opacity: 0.55;
      cursor: wait;
      transform: none;
    }
    .status-banner {
      margin-bottom: 14px;
      padding: 12px 14px;
      border-radius: 16px;
      background: var(--brand-soft);
      color: var(--ink);
      border: 1px solid #b5dccf;
    }
    .badges {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin: 10px 0 16px 0;
    }
    .badge {
      padding: 8px 12px;
      border-radius: 999px;
      font-weight: 700;
      font-size: 14px;
      border: 1px solid transparent;
    }
    .badge.ok {
      color: var(--ok);
      background: var(--ok-soft);
      border-color: #b6dfc4;
    }
    .badge.fail {
      color: var(--warn);
      background: var(--warn-soft);
      border-color: #ebd6b8;
    }
    .badge.error {
      color: var(--danger);
      background: var(--danger-soft);
      border-color: #ecc2c2;
    }
    .cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }
    .card {
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px;
      background: var(--panel-strong);
    }
    .card h3 {
      margin: 0 0 8px 0;
      font-size: 15px;
    }
    .card p {
      margin: 4px 0;
      color: var(--muted);
      font-size: 14px;
      word-break: break-all;
    }
    .table-wrap {
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: white;
      margin-top: 12px;
    }
    .table-toolbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 10px;
    }
    .table-limit-group {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }
    .table-page-group {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }
    .table-limit-button {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 6px 10px;
      background: #f8f2e6;
      color: var(--ink);
      font-size: 12px;
      cursor: pointer;
    }
    .table-limit-button.active {
      background: var(--brand-soft);
      border-color: #b5dccf;
      color: var(--brand);
      font-weight: 700;
    }
    .table-page-button {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 6px 10px;
      background: #fffdf8;
      color: var(--ink);
      font-size: 12px;
      cursor: pointer;
    }
    .table-page-button:disabled {
      opacity: 0.45;
      cursor: not-allowed;
      transform: none;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 960px;
    }
    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid #eee5d6;
      text-align: left;
      font-size: 14px;
      vertical-align: top;
    }
    th {
      background: #fcf7ee;
      position: sticky;
      top: 0;
      z-index: 1;
    }
    .table-sort {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border: none;
      background: transparent;
      color: inherit;
      padding: 0;
      border-radius: 0;
      cursor: pointer;
      font-weight: 700;
    }
    .table-sort:hover {
      transform: none;
      color: var(--brand);
    }
    .table-sort.active {
      color: var(--brand);
    }
    .sort-indicator {
      font-size: 11px;
      color: var(--muted);
    }
    pre {
      margin: 0;
      padding: 14px;
      white-space: pre-wrap;
      word-break: break-word;
      background: #171d2b;
      color: #f7f8fc;
      border-radius: 18px;
      min-height: 180px;
      max-height: 360px;
      overflow: auto;
      font-size: 13px;
      line-height: 1.45;
    }
    .empty {
      color: var(--muted);
      padding: 12px 0;
    }
    .tiny {
      color: var(--muted);
      font-size: 12px;
    }
    .tiny.dynamic {
      color: var(--brand);
    }
    @media (max-width: 1080px) {
      .grid {
        grid-template-columns: 1fr;
      }
      .workflow-fields.two-col {
        grid-template-columns: 1fr;
      }
      .checkbox-field-head {
        flex-direction: column;
        align-items: flex-start;
      }
    }
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <div class="hero-head">
        <div>
          <h1>Covered Call 调参界面</h1>
          <p class="sub">正式主路径只走三步：Step 1 先用轻量基础数据缩小范围，Step 2 只对候选池抓更深的数据并硬过滤，Step 3 只在合格样本里排序。顶部不再提供一步式全量刷新入口，避免一开始就对大池子抓最重的数据。</p>
        </div>
      </div>
      <div class="toolbar">
        <div class="field">
          <label for="configSelect">加载配置</label>
          <select id="configSelect"></select>
        </div>
        <div class="field">
          <label for="saveName">另存为 YAML</label>
          <input id="saveName" type="text" placeholder="例如 etf_ui_config.yaml">
        </div>
        <div class="field">
          <label>&nbsp;</label>
          <button class="soft" id="reloadButton">重新载入配置</button>
        </div>
        <div class="field">
          <label>&nbsp;</label>
          <button class="soft" id="saveButton">保存 YAML</button>
        </div>
      </div>
    </section>

    <section class="panel workflow-panel">
      <h2>三步流程</h2>
      <p class="hint">先定义研究范围，再剔除明显不合格，最后只在合格样本里排序。每一步都会把中间结果落到 `out/workflow_runs/...`，而且只有前一步完成后，下一步才会开放。</p>
      <div id="workflowRoot"></div>
    </section>

    <div class="grid">
      <section class="panel">
        <h2>参数区</h2>
        <p class="hint">这里改的是当前页面里的运行参数。你可以先试跑，再决定要不要保存回 YAML。</p>
        <div id="formRoot"></div>
      </section>

      <section class="panel">
        <h2>执行状态</h2>
        <div id="jobBanner" class="status-banner">准备就绪。先加载配置，再从 Step 1 开始。</div>

        <p class="hint">这个区域现在只负责告诉你三步流程当前跑到哪一步、后台有没有继续推进、以及日志里正在抓什么。候选池、硬过滤和排序结果都直接看上方各步自己的结果面板。</p>

        <div style="margin-top: 18px;">
          <h2>运行日志</h2>
          <p class="hint">Step 2 抓数据时，这里会持续显示当前 ticker、已完成数量、错误数量和 ETA。正式主路径不再在这里展示旧的一步式 screen 结果。</p>
          <pre id="logBox">还没有运行任务。</pre>
        </div>
      </section>
    </div>
  </div>

  <script>
    const SECTION_SCHEMA = [
      {
        title: "数据与输出",
        open: true,
        fields: [
          { path: "data.as_of_date", label: "As Of Date", type: "date", nullable: true },
          { path: "data.benchmark", label: "Benchmark", type: "text" },
          { path: "data.history_period", label: "History Period", type: "text" },
          { path: "data.cache_dir", label: "输出目录", type: "text" },
          { path: "data.metrics_filename", label: "Metrics 文件名", type: "text" },
          { path: "data.screened_filename", label: "Screen 文件名", type: "text" },
          { path: "data.resume_existing_metrics", label: "同日续跑补抓", type: "checkbox" },
          { path: "data.progress_every", label: "进度打印间隔", type: "number", step: "1" },
          { path: "data.checkpoint_every", label: "Checkpoint 间隔", type: "number", step: "1" },
          { path: "output.top_n", label: "终端 Top N", type: "number", step: "1" },
          { path: "output.show_failed", label: "终端显示 FAIL", type: "checkbox" }
        ]
      },
      {
        title: "候选池",
        open: true,
        fields: [
          { path: "universe.mode", label: "候选池模式", type: "select", options: ["file", "inline", "sp500"] },
          { path: "universe.path", label: "候选池文件", type: "text", datalist: "universeOptions" },
          { path: "universe.limit", label: "候选池上限", type: "number", step: "1", nullable: true },
          { path: "screen.allowed_asset_types", label: "允许的资产类型", type: "comma-list", placeholder: "例如 ETF, EQUITY" }
        ]
      },
      {
        title: "Step 2 数据源",
        open: true,
        fields: [
          { path: "data.market_data_source", label: "Step 2 数据源", type: "select", options: ["yahoo", "hybrid", "ibkr"] },
          { path: "data.ibkr.host", label: "IB Host", type: "text" },
          { path: "data.ibkr.port", label: "IB Port", type: "number", step: "1" },
          { path: "data.ibkr.readonly", label: "IB 只读连接", type: "checkbox" },
          { path: "data.ibkr.timeout", label: "IB Timeout", type: "number", step: "1" }
        ]
      },
      {
        title: "Covered Call 条件",
        open: true,
        fields: [
          { path: "screen.option_dte_min", label: "DTE 最小值", type: "number", step: "1" },
          { path: "screen.option_dte_max", label: "DTE 最大值", type: "number", step: "1" },
          { path: "screen.option_target_dte", label: "目标 DTE", type: "number", step: "1", help: "仅看月度时，这个参数只在多个月度到期同时落入窗口时才更有意义。" },
          { path: "screen.option_monthly_only", label: "仅看月度到期", type: "checkbox", help: "勾上后，如果 DTE 最大值还停在默认的 45，会自动放宽到 56。" },
          { path: "screen.min_otm_pct", label: "最小 OTM %", type: "percent", step: "0.1" },
          { path: "screen.max_otm_pct", label: "最大 OTM %", type: "percent", step: "0.1" },
          { path: "screen.preferred_otm_pct", label: "偏好 OTM %", type: "percent", step: "0.1", nullable: true, help: "只在最小/最大 OTM 区间内有多档可选时，优先靠近这个中心点。" },
          { path: "screen.min_size_metric", label: "最低 size metric", type: "number", step: "1" },
          { path: "screen.min_dividend_yield", label: "最低股息率 %", type: "percent", step: "0.1" },
          { path: "screen.min_call_annualized_yield", label: "最低年化 call 收益率 %", type: "percent", step: "0.1" },
          { path: "screen.min_call_open_interest", label: "最低 OI", type: "number", step: "1" },
          { path: "screen.min_call_volume", label: "最低期权成交量", type: "number", step: "1" },
          { path: "screen.max_option_spread_pct", label: "最大点差比例 %", type: "percent", step: "0.1" },
          { path: "screen.min_beta_1y", label: "Beta 下限", type: "number", step: "0.01" },
          { path: "screen.max_beta_1y", label: "Beta 上限", type: "number", step: "0.01" },
          { path: "screen.min_realized_vol_1y", label: "已实现波动率下限 %", type: "percent", step: "0.1" },
          { path: "screen.max_realized_vol_1y", label: "已实现波动率上限 %", type: "percent", step: "0.1" },
          { path: "screen.min_total_return_3y", label: "3 年总回报下限 %", type: "percent", step: "0.1" },
          { path: "screen.max_drawdown_3y", label: "3 年最大回撤上限 %", type: "percent", step: "0.1" }
        ]
      },
      {
        title: "打分权重",
        open: false,
        fields: [
          { path: "scoring.weights.dividend_yield", label: "股息率权重", type: "number", step: "0.01" },
          { path: "scoring.weights.call_yield", label: "Call 收益率权重", type: "number", step: "0.01" },
          { path: "scoring.weights.liquidity", label: "流动性权重", type: "number", step: "0.01" },
          { path: "scoring.weights.quality", label: "质量权重", type: "number", step: "0.01" },
          { path: "scoring.weights.risk_fit", label: "风险匹配权重", type: "number", step: "0.01" }
        ]
      },
      {
        title: "打分上限与容忍度",
        open: false,
        fields: [
          { path: "scoring.caps.dividend_yield", label: "股息率分数上限", type: "number", step: "0.001" },
          { path: "scoring.caps.call_annualized_yield", label: "Call 收益率分数上限", type: "number", step: "0.001" },
          { path: "scoring.caps.call_open_interest", label: "OI 分数上限", type: "number", step: "1" },
          { path: "scoring.caps.total_return_3y", label: "3 年回报分数上限", type: "number", step: "0.001" },
          { path: "scoring.caps.max_drawdown_3y", label: "回撤分数上限", type: "number", step: "0.001" },
          { path: "scoring.tolerances.beta_1y", label: "Beta 容忍度", type: "number", step: "0.001" },
          { path: "scoring.tolerances.realized_vol_1y", label: "波动率容忍度", type: "number", step: "0.001" },
          { path: "scoring.tolerances.option_spread_pct", label: "点差容忍度", type: "number", step: "0.001" }
        ]
      }
    ];
    const SIDEBAR_SECTION_TITLES = new Set(["数据与输出", "候选池"]);

    let bootState = null;
    let currentConfigName = null;
    let currentConfig = null;
    let currentWorkflow = null;
    let currentDataFileOptions = [];
    let currentDataFileCatalog = [];
    let currentJobId = null;
    let currentJobMode = null;
    let pollFailureCount = 0;
    let pollTimer = null;
    let workflowTableState = {};
    let deploymentInfo = { target: "local", read_only_mode: false, job_policy: "full" };

    const WORKFLOW_TABLE_LIMITS = [20, 50, 100, "all"];

    const configSelect = document.getElementById("configSelect");
    const saveNameInput = document.getElementById("saveName");
    const workflowRoot = document.getElementById("workflowRoot");
    const formRoot = document.getElementById("formRoot");
    const artifactCards = document.getElementById("artifactCards");
    const statusBadges = document.getElementById("statusBadges");
    const previewWrap = document.getElementById("previewWrap");
    const logBox = document.getElementById("logBox");
    const jobBanner = document.getElementById("jobBanner");
    const screenButton = document.getElementById("screenButton");
    const runButton = document.getElementById("runButton");
    const refreshButton = document.getElementById("refreshButton");
    const saveButton = document.getElementById("saveButton");
    const reloadButton = document.getElementById("reloadButton");

    function readOnlyMessage() {
      return deploymentInfo.target === "render"
        ? "当前是 Render 云端安全模式：允许 Step 1、Yahoo Step 2 和 Step 3；IB / hybrid 与保存 YAML 请在本地运行。"
        : "当前是受限模式。请在本地运行 Step 并保存配置。";
    }

    function currentMarketDataSource(config) {
      return String(config?.data?.market_data_source || "yahoo").trim().toLowerCase() || "yahoo";
    }

    function workflowStepBlockedReason(mode, runtimeConfig = null) {
      if (!deploymentInfo.read_only_mode) return "";
      if (mode === "workflow_step1" || mode === "workflow_step3") return "";
      if (mode === "workflow_step2") {
        const source = currentMarketDataSource(runtimeConfig || currentConfig || {});
        if (source === "yahoo") return "";
        return "当前是 Render 云端安全模式。Step 2 只允许 Yahoo 数据源；IB / hybrid 请在本地运行。";
      }
      return readOnlyMessage();
    }

    const COLUMN_LABELS = {
      ticker: "代码",
      symbol: "代码",
      name: "名称",
      status: "状态",
      candidate_status: "状态",
      fail_reasons: "失败原因",
      candidate_fail_reasons: "剔除原因",
      market_bucket: "市场类型",
      category_group: "分类",
      avg_dollar_volume: "日均成交额",
      size_metric: "规模",
      asset_type: "资产类型",
      score_total: "综合分",
      selected_expiration: "到期日",
      selected_dte: "DTE",
      price: "当前股价",
      selected_strike: "命中 Strike",
      selected_call_otm_pct: "命中 OTM %",
      selected_call_bid: "Call 买价",
      selected_call_ask: "Call 卖价",
      selected_call_mid: "Call 中间价",
      dividend_yield: "股息率 %",
      annualized_call_yield: "年化 Call 收益率 %",
      combined_income_score_proxy: "收益合计参考 %",
      selected_call_open_interest: "Call OI",
      selected_call_volume: "Call 成交量",
      selected_call_spread_pct: "Call 点差 %",
      beta_1y: "Beta(1Y)",
      realized_vol_1y: "历史波动率(1Y) %",
      total_return_3y: "3年回报 %",
      max_drawdown_3y: "3年最大回撤 %",
      score_dividend: "股息得分",
      score_call: "Call 得分",
      score_liquidity: "流动性得分",
      score_quality: "质量得分",
      score_risk_fit: "风险匹配得分",
    };

    const MARKET_BUCKET_LABELS = {
      equity: "股票",
      commodity: "商品",
      fixed_income: "债券/固定收益",
      currency: "货币",
      alternative: "另类",
      crypto: "加密资产",
      other: "其他",
    };

    const WORKFLOW_MARKET_CLASS_OPTIONS = [
      { value: "equity", label: "股票" },
      { value: "commodity", label: "商品" },
      { value: "fixed_income", label: "债券/固定收益" },
      { value: "currency", label: "货币" },
      { value: "alternative", label: "另类" },
      { value: "crypto", label: "加密资产" },
      { value: "other", label: "其他" },
    ];

    function getByPath(obj, path) {
      return path.split(".").reduce((acc, key) => (acc == null ? undefined : acc[key]), obj);
    }

    function setByPath(obj, path, value) {
      const parts = path.split(".");
      let cursor = obj;
      while (parts.length > 1) {
        const part = parts.shift();
        if (!(part in cursor) || typeof cursor[part] !== "object" || cursor[part] == null) {
          cursor[part] = {};
        }
        cursor = cursor[part];
      }
      cursor[parts[0]] = value;
    }

    function deepClone(value) {
      return JSON.parse(JSON.stringify(value));
    }

    function escapeHtml(value) {
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function filteredCsvOptions(predicate, selectedValue) {
      const values = (currentDataFileCatalog || [])
        .filter((item) => predicate(item))
        .map((item) => item.name)
        .filter((name) => String(name).toLowerCase().endsWith(".csv"));
      if (selectedValue && !values.includes(selectedValue)) {
        values.unshift(selectedValue);
      }
      return [...new Set(values)];
    }

    function universeCsvOptions(selectedValue) {
      return filteredCsvOptions((item) => ["universe", "universe_source"].includes(item.category), selectedValue);
    }

    function csvFileCatalog() {
      const items = (currentDataFileCatalog || []).filter((item) => String(item.name || "").toLowerCase().endsWith(".csv"));
      return items.sort((a, b) => String(a.name).localeCompare(String(b.name)));
    }

    function fileCatalogEntry(name) {
      return (currentDataFileCatalog || []).find((item) => item.name === name) || null;
    }

    function selectOptionsHtml(options, selectedValue) {
      return options.map((value) => `
        <option value="${escapeHtml(value)}" ${value === selectedValue ? "selected" : ""}>${escapeHtml(value)}</option>
      `).join("");
    }

    function formatMoneyHuman(value) {
      if (value == null || value === "" || Number.isNaN(Number(value))) return "";
      const numeric = Number(value);
      if (Math.abs(numeric) >= 1e9) return `${(numeric / 1e8).toFixed(2)} 亿美元/日`;
      if (Math.abs(numeric) >= 1e6) return `${(numeric / 1e6).toFixed(2)} 百万美元/日`;
      return `${numeric.toLocaleString()} 美元/日`;
    }

    function formatIntegerInput(value) {
      if (value == null || value === "" || Number.isNaN(Number(value))) return "";
      return Number(value).toLocaleString("en-US", { maximumFractionDigits: 0 });
    }

    function parseNumericText(value) {
      const text = String(value || "").replaceAll(",", "").trim();
      if (!text) return null;
      const numeric = Number(text);
      return Number.isFinite(numeric) ? numeric : null;
    }

    function formatUsdCompact(value, daily = false) {
      if (value == null || value === "" || Number.isNaN(Number(value))) return "";
      const numeric = Number(value);
      const suffix = daily ? "/日" : "";
      if (Math.abs(numeric) >= 1e8) return `${(numeric / 1e8).toFixed(2)} 亿美元${suffix}`;
      if (Math.abs(numeric) >= 1e6) return `${(numeric / 1e6).toFixed(2)} 百万美元${suffix}`;
      return `${numeric.toLocaleString()} 美元${suffix}`;
    }

    function formatPercentMaybe(value) {
      if (value == null || value === "" || Number.isNaN(Number(value))) return "";
      return (Number(value) * 100).toFixed(2) + "%";
    }

    function humanReason(reason) {
      const text = String(reason || "").trim();
      if (!text) return "";
      if (text === "currency!=USD") return "不是 USD 交易";
      if (text === "exchange_not_us_major") return "不是美国主交易所";
      if (text === "data_error") return "抓数报错";
      if (text === "asset_type_missing") return "缺少资产类型";
      if (text === "option_liquidity_missing") return "缺少期权流动性数据";

      let match = text.match(/^market_bucket_not_allowed:([^:]+)$/);
      if (match) {
        const bucket = MARKET_BUCKET_LABELS[match[1]] || match[1];
        return `不在当前市场范围（${bucket}）`;
      }

      match = text.match(/^asset_type_not_etf:(.+)$/);
      if (match) return `资产类型不是 ETF（${match[1]}）`;

      match = text.match(/^asset_type_not_allowed:([^>]+)->(.+)$/);
      if (match) return `资产类型不在允许范围（${match[1]}；允许 ${match[2]}）`;

      match = text.match(/^option_liquidity<oi:(\\d+)\\|vol:(\\d+)$/);
      if (match) return `期权流动性不足（OI < ${match[1]} 且成交量 < ${match[2]}）`;

      match = text.match(/^avg_dollar_volume<([0-9.eE+-]+)$/);
      if (match) return `日均成交额低于门槛（< ${formatUsdCompact(match[1], true)}）`;

      match = text.match(/^size_metric<([0-9.eE+-]+)$/);
      if (match) return `规模低于门槛（< ${formatUsdCompact(match[1])}）`;

      match = text.match(/^price<([0-9.eE+-]+)$/);
      if (match) return `价格低于门槛（< ${Number(match[1]).toFixed(2)} 美元）`;

      match = text.match(/^dividend_yield<([0-9.eE+-]+)$/);
      if (match) return `股息率低于门槛（< ${formatPercentMaybe(match[1])}）`;

      match = text.match(/^annualized_call_yield<([0-9.eE+-]+)$/);
      if (match) return `年化 Call 收益率低于门槛（< ${formatPercentMaybe(match[1])}）`;

      match = text.match(/^selected_call_spread_pct>([0-9.eE+-]+)$/);
      if (match) return `Call 点差过大（> ${formatPercentMaybe(match[1])}）`;

      match = text.match(/^beta_1y<([0-9.eE+-]+)$/);
      if (match) return `Beta 低于下限（< ${Number(match[1]).toFixed(2)}）`;

      match = text.match(/^beta_1y>([0-9.eE+-]+)$/);
      if (match) return `Beta 高于上限（> ${Number(match[1]).toFixed(2)}）`;

      match = text.match(/^realized_vol_1y<([0-9.eE+-]+)$/);
      if (match) return `历史波动率低于门槛（< ${formatPercentMaybe(match[1])}）`;

      match = text.match(/^realized_vol_1y>([0-9.eE+-]+)$/);
      if (match) return `历史波动率高于门槛（> ${formatPercentMaybe(match[1])}）`;

      match = text.match(/^total_return_3y<([0-9.eE+-]+)$/);
      if (match) return `3年回报低于门槛（< ${formatPercentMaybe(match[1])}）`;

      match = text.match(/^max_drawdown_3y>([0-9.eE+-]+)$/);
      if (match) return `3年最大回撤高于门槛（> ${formatPercentMaybe(match[1])}）`;

      match = text.match(/^([a-zA-Z0-9_]+)_missing$/);
      if (match) return `${COLUMN_LABELS[match[1]] || match[1]}缺失`;

      return text;
    }

    function columnLabel(key) {
      return COLUMN_LABELS[key] || key;
    }

    function marketDetailLabel(value) {
      const key = String(value || "").trim();
      if (!key) return "其他";
      if (MARKET_BUCKET_LABELS[key]) return MARKET_BUCKET_LABELS[key];
      if (key === "Fixed Income") return "债券/固定收益";
      if (key === "Commodities") return "商品";
      if (key === "Equities") return "股票";
      if (key === "Alternatives") return "另类";
      if (key === "Currencies") return "货币";
      return key.replaceAll("_", " ");
    }

    function formatNumber(value) {
      if (value == null || value === "") return "";
      if (typeof value !== "number") return value;
      if (Math.abs(value) >= 1e12) return (value / 1e12).toFixed(2) + "T";
      if (Math.abs(value) >= 1e9) return (value / 1e9).toFixed(2) + "B";
      if (Math.abs(value) >= 1e6) return (value / 1e6).toFixed(2) + "M";
      if (Math.abs(value) >= 1e3) return value.toLocaleString();
      return value.toFixed(3).replace(/\\.000$/, "");
    }

    function formatPercent(value) {
      if (value == null || value === "") return "";
      if (typeof value !== "number") return value;
      return Number((value * 100).toFixed(2)).toLocaleString("en-US", {
        maximumFractionDigits: 2,
      });
    }

    function formatPercentInputValue(value) {
      if (value == null || value === "" || Number.isNaN(Number(value))) return "";
      return String(Number((Number(value) * 100).toFixed(4)));
    }

    function formatCell(key, value) {
      const percentKeys = new Set([
        "dividend_yield",
        "annualized_call_yield",
        "combined_income_score_proxy",
        "selected_call_otm_pct",
        "selected_call_spread_pct",
        "realized_vol_1y",
        "total_return_3y",
        "max_drawdown_3y"
      ]);
      if (percentKeys.has(key)) return formatPercent(value);
      if (key === "score_total") return value == null ? "" : Number(value).toFixed(3);
      if (key === "selected_call_open_interest" || key === "selected_call_volume") {
        return value == null ? "" : Number(value).toLocaleString();
      }
      if (key === "avg_dollar_volume" || key === "size_metric") {
        return key === "avg_dollar_volume" ? formatUsdCompact(value, true) : formatUsdCompact(value, false);
      }
      if (key === "price" || key === "selected_strike" || key === "selected_call_bid" || key === "selected_call_ask" || key === "selected_call_mid") {
        return value == null ? "" : `${Number(value).toFixed(2)} 美元`;
      }
      if (key === "beta_1y") return value == null ? "" : Number(value).toFixed(2);
      if (key === "market_bucket") return MARKET_BUCKET_LABELS[String(value)] || value;
      if (key === "category_group") return marketDetailLabel(value);
      if (key === "fail_reasons" || key === "candidate_fail_reasons") {
        return String(value || "").split(";").map((item) => humanReason(item)).filter(Boolean).join("；");
      }
      return value == null ? "" : value;
    }

    function defaultSortDirection(key) {
      if (["status", "candidate_status", "symbol", "ticker", "name", "market_bucket", "category_group", "asset_type"].includes(key)) {
        return "asc";
      }
      return "desc";
    }

    function tableState(tableId) {
      if (!workflowTableState[tableId]) {
        workflowTableState[tableId] = {
          sortKey: null,
          sortDirection: "desc",
          limit: 20,
          page: 1,
        };
      }
      return workflowTableState[tableId];
    }

    function comparableValue(key, value) {
      if (value == null || value === "") return null;
      if (key === "status" || key === "candidate_status") {
        return { PASS: 0, FAIL: 1, ERROR: 2 }[String(value)] ?? 99;
      }
      if (key === "market_bucket") {
        return { equity: 0, commodity: 1, fixed_income: 2, currency: 3, alternative: 4, crypto: 5, other: 6 }[String(value)] ?? 99;
      }
      if (key === "fail_reasons" || key === "candidate_fail_reasons") {
        return String(value || "").split(";").map((item) => humanReason(item)).join(" ");
      }
      if (typeof value === "number") return value;
      const raw = String(value).trim();
      if (raw && /^[-+]?\\d+(?:\\.\\d+)?(?:[eE][-+]?\\d+)?$/.test(raw)) {
        return Number(raw);
      }
      return raw.toLowerCase();
    }

    function compareRows(a, b, key, direction) {
      const left = comparableValue(key, a[key]);
      const right = comparableValue(key, b[key]);
      if (left == null && right == null) return 0;
      if (left == null) return 1;
      if (right == null) return -1;
      let result = 0;
      if (typeof left === "number" && typeof right === "number") {
        result = left - right;
      } else {
        result = String(left).localeCompare(String(right), "zh-Hans-CN", {
          numeric: true,
          sensitivity: "base",
        });
      }
      return direction === "asc" ? result : -result;
    }

    function sortedTableRows(rows, state) {
      const output = [...rows];
      if (!state.sortKey) return output;
      output.sort((a, b) => compareRows(a, b, state.sortKey, state.sortDirection));
      return output;
    }

    function paginationMeta(rows, state) {
      const totalCount = rows.length;
      if (state.limit === "all") {
        return {
          totalCount,
          pageSize: "all",
          pageCount: 1,
          page: 1,
          startIndex: totalCount ? 1 : 0,
          endIndex: totalCount,
          rows,
        };
      }
      const pageSize = Number(state.limit) || 100;
      const pageCount = Math.max(1, Math.ceil(totalCount / pageSize));
      const page = Math.min(Math.max(1, Number(state.page) || 1), pageCount);
      const start = (page - 1) * pageSize;
      const end = Math.min(start + pageSize, totalCount);
      return {
        totalCount,
        pageSize,
        pageCount,
        page,
        startIndex: totalCount ? start + 1 : 0,
        endIndex: end,
        rows: rows.slice(start, end),
      };
    }

    function setWorkflowTableSort(tableId, key) {
      const state = tableState(tableId);
      if (state.sortKey === key) {
        state.sortDirection = state.sortDirection === "asc" ? "desc" : "asc";
      } else {
        state.sortKey = key;
        state.sortDirection = defaultSortDirection(key);
      }
      state.page = 1;
    }

    function setWorkflowTableLimit(tableId, limit) {
      const state = tableState(tableId);
      state.limit = limit === "all" ? "all" : Number(limit);
      state.page = 1;
    }

    function setWorkflowTablePage(tableId, action) {
      const state = tableState(tableId);
      if (state.limit === "all") {
        state.page = 1;
        return;
      }
      const delta = action === "prev" ? -1 : 1;
      state.page = Math.max(1, (Number(state.page) || 1) + delta);
    }

    function renderTableHtml(rows, title, emptyText, tableId) {
      if (!rows || !rows.length) {
        return `<div class="empty">${escapeHtml(emptyText)}</div>`;
      }
      const state = tableState(tableId);
      const columns = Object.keys(rows[0]);
      const sortedRows = sortedTableRows(rows, state);
      const meta = paginationMeta(sortedRows, state);
      state.page = meta.page;
      const displayRows = meta.rows;
      const displayCount = displayRows.length;
      const totalCount = meta.totalCount;
      const limitButtons = WORKFLOW_TABLE_LIMITS.map((limitValue) => {
        const label = limitValue === "all" ? "全部" : `${limitValue}/页`;
        const active = state.limit === limitValue ? "active" : "";
        return `
          <button type="button" class="table-limit-button ${active}" data-workflow-limit="${escapeHtml(tableId)}" data-limit="${escapeHtml(limitValue)}">
            ${escapeHtml(label)}
          </button>
        `;
      }).join("");
      const pagingSummary = state.limit === "all"
        ? `共 ${totalCount} 行，当前显示全部。可点列标题排序。`
        : `共 ${totalCount} 行，当前第 ${meta.page} / ${meta.pageCount} 页，显示第 ${meta.startIndex}-${meta.endIndex} 行。可点列标题排序。`;
      const pagingButtons = `
        <div class="table-page-group">
          <button type="button" class="table-page-button" data-workflow-page="${escapeHtml(tableId)}" data-page-action="prev" ${meta.page <= 1 || state.limit === "all" ? "disabled" : ""}>上一页</button>
          <span class="tiny">${state.limit === "all" ? "全部" : `第 ${meta.page} / ${meta.pageCount} 页`}</span>
          <button type="button" class="table-page-button" data-workflow-page="${escapeHtml(tableId)}" data-page-action="next" ${meta.page >= meta.pageCount || state.limit === "all" ? "disabled" : ""}>下一页</button>
        </div>
      `;
      return `
        <div class="workflow-section">
          <h3>${escapeHtml(title)}</h3>
          <div class="table-toolbar">
            <div class="tiny">${pagingSummary}</div>
            <div class="table-limit-group">
              <span class="tiny">每页</span>
              ${limitButtons}
              ${pagingButtons}
            </div>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>${columns.map((key) => {
                  const active = state.sortKey === key;
                  const indicator = active ? (state.sortDirection === "asc" ? "↑" : "↓") : "↕";
                  return `<th><button type="button" class="table-sort ${active ? "active" : ""}" data-workflow-sort="${escapeHtml(tableId)}" data-workflow-key="${escapeHtml(key)}">${escapeHtml(columnLabel(key))}<span class="sort-indicator">${indicator}</span></button></th>`;
                }).join("")}</tr>
              </thead>
              <tbody>
                ${displayRows.map((row) => `
                  <tr>
                    ${columns.map((key) => `<td>${escapeHtml(formatCell(key, row[key]))}</td>`).join("")}
                  </tr>
                `).join("")}
              </tbody>
            </table>
          </div>
        </div>
      `;
    }

    function renderReasonListHtml(items, title, emptyText) {
      if (!items || !items.length) {
        return `
          <div class="workflow-section">
            <h3>${escapeHtml(title)}</h3>
            <div class="empty">${escapeHtml(emptyText)}</div>
          </div>
        `;
      }
      return `
        <div class="workflow-section">
          <h3>${escapeHtml(title)}</h3>
          <div class="reason-list">
            ${items.map((item) => `<div class="reason-pill">${escapeHtml(humanReason(item.reason))} × ${escapeHtml(item.count)}</div>`).join("")}
          </div>
        </div>
      `;
    }

    function candidatePoolDetailHtml() {
      if (!currentWorkflow?.candidate_pool) {
        return `
          <div class="workflow-detail-panel">
            <h3>Step 1 结果</h3>
            <p>运行第 1 步后，候选池审查结果会直接显示在这里。</p>
            <div class="empty">现在还没有 Step 1 结果。</div>
          </div>
        `;
      }
      const counts = currentWorkflow.candidate_pool.status_counts || {};
      const warnings = currentWorkflow.candidate_pool.warnings || [];
      return `
        <div class="workflow-detail-panel">
          <h3>Step 1 结果</h3>
          <p>先看这一步：哪些标的被保留，哪些被剔除，主要剔除原因是什么。确认这一步口径没问题，再往下进入 Step 2。</p>
          <div class="badges">
            <div class="badge ok">PASS: ${counts.PASS || 0}</div>
            <div class="badge fail">FAIL: ${counts.FAIL || 0}</div>
          </div>
          ${warnings.length ? workflowSummaryHtml("Step 1 提示", warnings) : ""}
          ${renderReasonListHtml(
            currentWorkflow.candidate_pool.top_fail_reasons || [],
            "Step 1 主要剔除原因",
            "这一步暂时没有剔除原因。"
          )}
          ${renderTableHtml(
            currentWorkflow.candidate_pool.review_preview_rows || [],
            "Step 1 审查表",
            "这一步还没有可展示的审查结果。",
            "step1-review"
          )}
        </div>
      `;
    }

    function hardFilterDetailHtml() {
      if (!currentWorkflow?.hard_filter) {
        return `
          <div class="workflow-detail-panel">
            <h3>Step 2 结果</h3>
            <p>运行第 2 步后，候选池抓数和硬过滤结果会直接显示在这里。</p>
            <div class="empty">现在还没有 Step 2 结果。</div>
          </div>
        `;
      }
      const counts = currentWorkflow.hard_filter.status_counts || {};
      return `
        <div class="workflow-detail-panel">
          <h3>Step 2 结果</h3>
          <p>这一步先看候选池抓数是否正常，再看阈值有没有过严或过松。重点看 FAIL 原因分布，以及 PASS 样本是否仍符合你的直觉。</p>
          <div class="badges">
            <div class="badge ok">PASS: ${counts.PASS || 0}</div>
            <div class="badge fail">FAIL: ${counts.FAIL || 0}</div>
            <div class="badge error">ERROR: ${counts.ERROR || 0}</div>
          </div>
          ${renderReasonListHtml(
            currentWorkflow.hard_filter.top_fail_reasons || [],
            "Step 2 主要失败原因",
            "这一步暂时没有失败原因。"
          )}
          ${renderTableHtml(
            currentWorkflow.hard_filter.preview_rows || [],
            "Step 2 结果表",
            "这一步还没有可展示的过滤结果。",
            "step2-results"
          )}
        </div>
      `;
    }

    function rankingDetailHtml() {
      if (!currentWorkflow?.ranking) {
        return `
          <div class="workflow-detail-panel">
            <h3>Step 3 结果</h3>
            <p>运行第 3 步后，最终排序结果会直接显示在这里。</p>
            <div class="empty">现在还没有 Step 3 结果。</div>
          </div>
        `;
      }
      return `
        <div class="workflow-detail-panel">
          <h3>Step 3 结果</h3>
          <p>最后只在合格样本里看排序。这里应该直接回答“当前参数下，前排最值得研究的是谁”。</p>
          ${renderTableHtml(
            currentWorkflow.ranking.preview_rows || [],
            "Step 3 榜单表",
            "这一步还没有可展示的排序结果。",
            "step3-ranking"
          )}
        </div>
      `;
    }

    function inferWorkflowDefaults(state) {
      const defaults = deepClone(state.workflow_defaults || {});
      if (!Array.isArray(defaults.allowed_market_classes) || !defaults.allowed_market_classes.length) {
        defaults.allowed_market_classes = ["equity", "commodity"];
      }
      return {
        settings: defaults,
        workflow_dir: null,
        candidate_pool: null,
        hard_filter: null,
        ranking: null
      };
    }

    function currentWorkflowSettings() {
      const settings = deepClone(currentWorkflow?.settings || {});
      const sourceUniverse = document.getElementById("workflowSourceUniverse");
      const minAdv = document.getElementById("workflowMinAdv");
      const minSize = document.getElementById("workflowMinSize");
      const requireUsd = document.getElementById("workflowRequireUsd");
      const requireUsListing = document.getElementById("workflowRequireUsListing");
      const requireEtfAsset = document.getElementById("workflowRequireEtfAsset");
      const step2ReuseCache = document.getElementById("workflowStep2ReuseCache");

      if (sourceUniverse) settings.source_universe_csv = sourceUniverse.value.trim();
      if (minAdv) settings.min_avg_dollar_volume = parseNumericText(minAdv.value);
      if (minSize) settings.min_size_metric = parseNumericText(minSize.value);
      if (requireUsd) settings.require_usd = requireUsd.checked;
      if (requireUsListing) settings.require_us_listing = requireUsListing.checked;
      if (requireEtfAsset) settings.require_etf_asset_type = requireEtfAsset.checked;
      if (step2ReuseCache) settings.step2_reuse_same_day_cache = step2ReuseCache.checked;
      settings.allowed_market_classes = Array.from(
        document.querySelectorAll('input[data-market-class]:checked')
      ).map((input) => input.value);

      return settings;
    }

    function workflowSummaryHtml(title, lines) {
      return `
        <div class="workflow-summary">
          <h4>${title}</h4>
          ${lines.map((line) => `<div class="row">${line}</div>`).join("")}
        </div>
      `;
    }

    function renderWorkflow() {
      if (!currentWorkflow) {
        workflowRoot.innerHTML = '<div class="empty">请先加载配置。</div>';
        return;
      }

      const step1Done = Boolean(currentWorkflow.candidate_pool);
      const step2Done = Boolean(currentWorkflow.hard_filter);
      const step3Done = Boolean(currentWorkflow.ranking);
      const settings = currentWorkflow.settings || {};
      const selectedMarketClasses = Array.isArray(settings.allowed_market_classes) && settings.allowed_market_classes.length
        ? settings.allowed_market_classes
        : ["equity", "commodity"];
      const sourceUniverseOptions = universeCsvOptions(settings.source_universe_csv || "");
      const sourceUniverseMeta = fileCatalogEntry(settings.source_universe_csv || "");
      const minAdvLabel = formatMoneyHuman(settings.min_avg_dollar_volume);
      const minSizeLabel = settings.min_size_metric == null || settings.min_size_metric === ""
        ? ""
        : formatUsdCompact(settings.min_size_metric, false);
      const step2ReuseCache = Boolean(settings.step2_reuse_same_day_cache);
      const csvCatalogRows = csvFileCatalog()
        .map((item) => `
          <div class="row"><strong>${escapeHtml(item.name)}</strong>: ${escapeHtml(item.description || item.label || "")}</div>
        `)
        .join("");
      const csvHelpSummary = `
        <div class="workflow-summary">
          <h4>CSV 文件说明</h4>
          <div class="row"><strong>当前 Universe</strong>: ${escapeHtml(settings.source_universe_csv || "未选择")}${sourceUniverseMeta ? `，${escapeHtml(sourceUniverseMeta.description)}` : ""}</div>
          <div class="row"><strong>Step 1 说明</strong>: 这里直接用 Universe 做轻筛；不会在这一步联网抓价格或期权链。</div>
          <div class="row"><strong>Step 2 说明</strong>: 只会对 Step 1 候选池抓取价格、分红、波动率和期权链，再立刻做硬过滤。数据源和 IB 连接参数放在 Step 2 卡片里。</div>
          <details>
            <summary>查看当前可选 CSV 说明</summary>
            <div class="fields">${csvCatalogRows || '<div class="row">当前没有可选 CSV。</div>'}</div>
          </details>
        </div>
      `;

      const step1Summary = step1Done
        ? workflowSummaryHtml("Step 1 已保存", [
            `workflow_dir: ${currentWorkflow.workflow_dir}`,
            `源候选数 ${currentWorkflow.candidate_pool.source_universe_rows}`,
            `保留下来 ${currentWorkflow.candidate_pool.candidate_rows}`,
            `最低日均成交额 ${settings.min_avg_dollar_volume == null ? "未设置" : formatUsdCompact(settings.min_avg_dollar_volume, true)}`,
            `最小规模 ${settings.min_size_metric == null ? "未设置" : formatUsdCompact(settings.min_size_metric, false)}`,
            currentWorkflow.candidate_pool.artifacts.local_enrichment_csv
              ? `本地补齐缓存: ${currentWorkflow.candidate_pool.artifacts.local_enrichment_csv}`
              : "本地补齐缓存: 未使用",
            `candidate_universe_csv: ${currentWorkflow.candidate_pool.artifacts.candidate_universe_csv}`
          ])
        : '<div class="workflow-summary"><div class="row">还没有运行候选池步骤。</div></div>';

      const step2Counts = currentWorkflow.hard_filter?.status_counts || {};
      const step2Summary = step2Done
        ? workflowSummaryHtml("Step 2 已保存", [
            `PASS ${step2Counts.PASS || 0} / FAIL ${step2Counts.FAIL || 0} / ERROR ${step2Counts.ERROR || 0}`,
            currentWorkflow.hard_filter.market_data_source
              ? `数据源: ${currentWorkflow.hard_filter.market_data_source}${currentWorkflow.hard_filter.ibkr_connection ? ` (${currentWorkflow.hard_filter.ibkr_connection})` : ""}`
              : "",
            step2Counts.PASS || step2Counts.FAIL || step2Counts.ERROR || currentWorkflow.hard_filter.step2_reuse_same_day_cache != null
              ? `缓存策略: ${currentWorkflow.hard_filter.step2_reuse_same_day_cache ? "复用同日缓存" : "强制重抓"}`
              : "",
            `candidate_metrics_csv: ${currentWorkflow.hard_filter.artifacts.candidate_metrics_csv}`,
            `hard_filter_csv: ${currentWorkflow.hard_filter.artifacts.hard_filter_csv}`,
            `pass_csv: ${currentWorkflow.hard_filter.artifacts.hard_filter_pass_csv}`
          ].filter(Boolean))
        : '<div class="workflow-summary"><div class="row">先完成 Step 1，才能进入硬过滤。</div></div>';

      const rankedPreview = (currentWorkflow.ranking?.preview_rows || [])
        .slice(0, 5)
        .map((row) => row.ticker)
        .filter(Boolean)
        .join(", ");
      const step3Summary = step3Done
        ? workflowSummaryHtml("Step 3 已保存", [
            `最终合格样本 ${currentWorkflow.ranking.rows}`,
            rankedPreview ? `Top: ${rankedPreview}` : "Top: 暂无",
            `ranked_csv: ${currentWorkflow.ranking.artifacts.ranked_csv}`
          ])
        : '<div class="workflow-summary"><div class="row">先完成 Step 2，才能进入排序。</div></div>';

      workflowRoot.innerHTML = `
        <div class="workflow-stack">
          <div class="workflow-card">
            <div class="workflow-next">Step 1</div>
            <h3>候选池规则</h3>
            <p>这一步只定义研究范围，不讨论优劣。建议把 USD、市场范围、最低日均成交额和最小规模都在这里确定。</p>
            <div class="workflow-fields">
              <div class="field">
                <label for="workflowSourceUniverse">源 Universe CSV</label>
                <select id="workflowSourceUniverse">
                  ${selectOptionsHtml(sourceUniverseOptions, settings.source_universe_csv || "")}
                </select>
                <div class="tiny">Step 1 只用这份 Universe 做轻筛，不会在这一步联网抓价格或期权链。</div>
              </div>
              <div class="field">
                <label>保留哪些市场类别</label>
                <div class="checkbox-group">
                  ${WORKFLOW_MARKET_CLASS_OPTIONS.map((item) => `
                    <label class="checkbox">
                      <input type="checkbox" data-market-class="true" value="${escapeHtml(item.value)}" ${selectedMarketClasses.includes(item.value) ? "checked" : ""}>
                      ${escapeHtml(item.label)}
                    </label>
                  `).join("")}
                </div>
                <div class="tiny">默认只勾“股票、商品”。如果你想把债券或货币也纳入候选池，在这里直接勾选。</div>
              </div>
              <div class="field">
                <label for="workflowMinAdv">最低日均成交额</label>
                <input id="workflowMinAdv" type="text" inputmode="numeric" value="${escapeHtml(formatIntegerInput(settings.min_avg_dollar_volume))}">
                <div class="tiny">${minAdvLabel ? `当前约 ${escapeHtml(minAdvLabel)}` : "留空表示不加这条门槛。"}</div>
              </div>
              <div class="field">
                <label for="workflowMinSize">最小规模</label>
                <input id="workflowMinSize" type="text" inputmode="numeric" value="${escapeHtml(formatIntegerInput(settings.min_size_metric))}">
                <div class="tiny">${minSizeLabel ? `当前约 ${escapeHtml(minSizeLabel)}` : "留空表示不加这条门槛。"}</div>
              </div>
              <label class="checkbox"><input id="workflowRequireUsd" type="checkbox" ${settings.require_usd ? "checked" : ""}> 只保留 USD 交易</label>
              <label class="checkbox"><input id="workflowRequireUsListing" type="checkbox" ${settings.require_us_listing ? "checked" : ""}> 只保留美国主交易所上市</label>
              <label class="checkbox"><input id="workflowRequireEtfAsset" type="checkbox" ${settings.require_etf_asset_type ? "checked" : ""}> 只保留 asset_type=ETF</label>
            </div>
            <div class="workflow-actions">
              <button class="secondary" id="workflowStep1Button">运行第 1 步</button>
            </div>
            ${csvHelpSummary}
            ${step1Summary}
          </div>

          ${candidatePoolDetailHtml()}

          <div class="workflow-card ${step1Done ? "" : "locked"}">
            <div class="workflow-next">Step 2</div>
            <h3>抓取数据并硬过滤</h3>
            <p>先在这里选数据源、调硬过滤参数，再运行第 2 步。它只会对 Step 1 候选池抓取价格、分红、波动率和期权链，然后立刻做硬过滤。</p>
            <div class="workflow-fields">
              <label class="checkbox"><input id="workflowStep2ReuseCache" type="checkbox" ${step2ReuseCache ? "checked" : ""}> 复用同日缓存（不勾选则强制重抓）</label>
              <div class="tiny">默认不复用缓存，这样你能看到真实抓数耗时；需要更快时再勾上。</div>
            </div>
            <div class="workflow-fields two-col" id="workflowStep2Fields"></div>
            <div class="workflow-actions">
              <button id="workflowStep2Button" ${step1Done ? "" : "disabled"}>运行第 2 步</button>
            </div>
            ${step2Summary}
          </div>

          ${hardFilterDetailHtml()}

          <div class="workflow-card ${step2Done ? "" : "locked"}">
            <div class="workflow-next">Step 3</div>
            <h3>排序规则</h3>
            <p>先在这里调打分参数，再运行第 3 步。它只会在 Step 2 合格样本里排序，不会回头改候选池范围。</p>
            <div class="workflow-fields" id="workflowStep3Weights"></div>
            <div class="workflow-fields" id="workflowStep3Caps"></div>
            <div class="workflow-actions">
              <button id="workflowStep3Button" ${step2Done ? "" : "disabled"}>运行第 3 步</button>
            </div>
            ${step3Summary}
          </div>

          ${rankingDetailHtml()}
        </div>
      `;

      const step1Button = document.getElementById("workflowStep1Button");
      const step2Button = document.getElementById("workflowStep2Button");
      const step3Button = document.getElementById("workflowStep3Button");
      const workflowMinAdv = document.getElementById("workflowMinAdv");
      const workflowMinSize = document.getElementById("workflowMinSize");
      if (step1Button) step1Button.addEventListener("click", () => startWorkflowStep("workflow_step1"));
      if (step2Button) step2Button.addEventListener("click", () => startWorkflowStep("workflow_step2"));
      if (step3Button) step3Button.addEventListener("click", () => startWorkflowStep("workflow_step3"));
      mountSectionFields("workflowStep2Fields", ["Step 2 数据源", "Covered Call 条件"], currentConfig, []);
      mountSectionFields("workflowStep3Weights", ["打分权重"], currentConfig, []);
      mountSectionFields("workflowStep3Caps", ["打分上限与容忍度"], currentConfig, []);
      const marketDataSourceInput = document.querySelector('#workflowRoot [data-path="data.market_data_source"]');
      const updateWorkflowButtonAvailability = () => {
        const runtimeConfig = collectConfigFromForm();
        const step1Reason = workflowStepBlockedReason("workflow_step1", runtimeConfig);
        const step2Reason = workflowStepBlockedReason("workflow_step2", runtimeConfig);
        const step3Reason = workflowStepBlockedReason("workflow_step3", runtimeConfig);

        if (step1Button) {
          step1Button.disabled = Boolean(step1Reason);
          step1Button.title = step1Reason;
        }
        if (step2Button) {
          const locked = !step1Done;
          step2Button.disabled = locked || Boolean(step2Reason);
          step2Button.title = locked ? "先完成 Step 1，才能进入硬过滤。" : step2Reason;
        }
        if (step3Button) {
          const locked = !step2Done;
          step3Button.disabled = locked || Boolean(step3Reason);
          step3Button.title = locked ? "先完成 Step 2，才能进入排序。" : step3Reason;
        }
      };
      if (marketDataSourceInput) {
        marketDataSourceInput.addEventListener("change", updateWorkflowButtonAvailability);
      }
      updateWorkflowButtonAvailability();
      [workflowMinAdv, workflowMinSize].forEach((input) => {
        if (!input) return;
        input.addEventListener("blur", () => {
          const numeric = parseNumericText(input.value);
          input.value = numeric == null ? "" : formatIntegerInput(numeric);
        });
      });
    }

    function createInput(field, value, universeOptions) {
      const isFullRowCheckbox = field.type === "checkbox" && field.path === "screen.option_monthly_only";
      const wrap = document.createElement("div");
      wrap.className = field.type === "checkbox"
        ? `field checkbox-field${isFullRowCheckbox ? " checkbox-full-row" : ""}`
        : "field";

      const label = document.createElement("label");
      label.textContent = field.label;

      if (field.type === "checkbox") {
        const head = document.createElement("div");
        head.className = "checkbox-field-head";

        const title = document.createElement("div");
        title.className = "checkbox-field-label";
        title.textContent = field.label;
        head.appendChild(title);

        const holder = document.createElement("label");
        holder.className = "checkbox";
        const input = document.createElement("input");
        input.type = "checkbox";
        input.checked = Boolean(value);
        input.dataset.path = field.path;
        input.dataset.kind = field.type;
        holder.appendChild(input);
        holder.appendChild(document.createTextNode("启用"));
        head.appendChild(holder);
        wrap.appendChild(head);
        if (field.help) {
          const help = document.createElement("div");
          help.className = "tiny";
          help.textContent = field.help;
          wrap.appendChild(help);
        }
        return wrap;
      }

      wrap.appendChild(label);

      let input;
      if (field.type === "select") {
        input = document.createElement("select");
        field.options.forEach((optionValue) => {
          const option = document.createElement("option");
          option.value = optionValue;
          option.textContent = optionValue;
          if (String(value ?? "") === optionValue) option.selected = true;
          input.appendChild(option);
        });
      } else {
        input = document.createElement("input");
        input.type = field.type === "number" || field.type === "percent" ? "number" : field.type;
        if (field.step) input.step = field.step;
        if (field.placeholder) input.placeholder = field.placeholder;
        if (field.datalist) {
          input.setAttribute("list", field.datalist);
        }
        if (field.type === "date") {
          input.value = value || "";
        } else if (field.type === "number") {
          input.value = value == null ? "" : value;
        } else if (field.type === "percent") {
          input.value = formatPercentInputValue(value);
        } else if (field.type === "comma-list") {
          input.type = "text";
          input.value = Array.isArray(value) ? value.join(", ") : (value || "");
        } else {
          input.value = value == null ? "" : value;
        }
      }
      input.dataset.path = field.path;
      input.dataset.kind = field.type;
      if (field.nullable) input.dataset.nullable = "true";
      wrap.appendChild(input);

      if (field.datalist === "universeOptions") {
        const list = document.createElement("datalist");
        list.id = "universeOptions";
        universeOptions.forEach((name) => {
          const option = document.createElement("option");
          option.value = name;
          list.appendChild(option);
        });
        wrap.appendChild(list);
      }

      if (field.help) {
        const help = document.createElement("div");
        help.className = "tiny";
        help.textContent = field.help;
        wrap.appendChild(help);
      }

      return wrap;
    }

    function attachMonthlyOnlyBehavior(containerId) {
      const container = document.getElementById(containerId);
      if (!container) return;

      const monthlyOnlyInput = container.querySelector('[data-path="screen.option_monthly_only"]');
      const dteMaxInput = container.querySelector('[data-path="screen.option_dte_max"]');
      if (!monthlyOnlyInput || !dteMaxInput) return;

      const dteMaxField = dteMaxInput.closest(".field");
      if (!dteMaxField) return;

      let note = dteMaxField.querySelector(".monthly-auto-note");
      if (!note) {
        note = document.createElement("div");
        note.className = "tiny dynamic monthly-auto-note";
        dteMaxField.appendChild(note);
      }

      const update = (userTriggered = false) => {
        const rawValue = String(dteMaxInput.value || "").trim();
        const numericValue = rawValue === "" ? null : Number(rawValue);
        if (!monthlyOnlyInput.checked) {
          note.textContent = "";
          return;
        }
        if (userTriggered && (rawValue === "" || rawValue === "45")) {
          dteMaxInput.value = "56";
          note.textContent = "已因“仅看月度到期”自动把 DTE 最大值从默认的 45 放宽到 56。";
          return;
        }
        if (numericValue != null && Number.isFinite(numericValue) && numericValue < 56) {
          note.textContent = `当前 DTE 最大值为 ${numericValue}；仅看月度时通常建议放宽到 56。`;
          return;
        }
        note.textContent = "仅看月度时，当前 DTE 窗口已经足够覆盖更常见的月度到期。";
      };

      monthlyOnlyInput.addEventListener("change", () => update(true));
      update(false);
    }

    function mountSectionFields(containerId, sectionTitles, config, universeOptions) {
      const container = document.getElementById(containerId);
      if (!container) return;
      container.innerHTML = "";
      SECTION_SCHEMA
        .filter((section) => sectionTitles.includes(section.title))
        .forEach((section) => {
          section.fields.forEach((field) => {
            container.appendChild(createInput(field, getByPath(config, field.path), universeOptions));
          });
        });
      if (containerId === "workflowStep2Fields") {
        attachMonthlyOnlyBehavior(containerId);
      }
    }

    function renderForm(config, universeOptions) {
      formRoot.innerHTML = "";
      SECTION_SCHEMA.filter((section) => SIDEBAR_SECTION_TITLES.has(section.title)).forEach((section) => {
        const details = document.createElement("details");
        details.open = section.open;
        const summary = document.createElement("summary");
        summary.textContent = section.title;
        details.appendChild(summary);

        const fields = document.createElement("div");
        fields.className = "fields";
        section.fields.forEach((field) => {
          fields.appendChild(createInput(field, getByPath(config, field.path), universeOptions));
        });
        details.appendChild(fields);
        formRoot.appendChild(details);
      });
    }

    function collectConfigFromForm() {
      const config = deepClone(currentConfig);
      document.querySelectorAll("#formRoot [data-path], #workflowRoot [data-path]").forEach((input) => {
        const path = input.dataset.path;
        const kind = input.dataset.kind;
        const nullable = input.dataset.nullable === "true";
        let value;

        if (kind === "checkbox") {
          value = input.checked;
        } else if (kind === "number") {
          value = input.value === "" && nullable ? null : Number(input.value);
          if (input.value === "") value = nullable ? null : 0;
        } else if (kind === "percent") {
          value = input.value === "" && nullable ? null : Number(input.value) / 100;
          if (input.value === "") value = nullable ? null : 0;
        } else if (kind === "comma-list") {
          value = input.value
            .split(",")
            .map((item) => item.trim())
            .filter(Boolean);
        } else {
          value = input.value;
          if (nullable && value === "") value = null;
        }

        setByPath(config, path, value);
      });
      return config;
    }

    function renderArtifacts(artifacts) {
      if (!artifactCards) return;
      artifactCards.innerHTML = "";
      if (!artifacts) {
        artifactCards.innerHTML = '<div class="empty">还没有可展示的文件状态。</div>';
        return;
      }

      const items = [
        {
          title: "配置文件",
          lines: [artifacts.config_path]
        },
        {
          title: "Metrics 文件",
          lines: [
            artifacts.metrics.path,
            artifacts.metrics.exists ? `已存在，更新于 ${artifacts.metrics.updated_at}` : "尚未生成"
          ].concat(artifacts.metrics.summary ? [
            `行数 ${artifacts.metrics.summary.rows}`,
            `抓数报错 ${artifacts.metrics.summary.error_rows}`,
            artifacts.metrics.summary.as_of_dates.length
              ? `as_of_date: ${artifacts.metrics.summary.as_of_dates.join(", ")}`
              : "as_of_date: 无"
          ] : [])
        },
        {
          title: "Screen 文件",
          lines: [
            artifacts.screen.path,
            artifacts.screen.exists ? `已存在，更新于 ${artifacts.screen.updated_at}` : "尚未生成"
          ].concat(artifacts.screen.summary ? [
            `行数 ${artifacts.screen.summary.rows}`,
            `状态分布 ${JSON.stringify(artifacts.screen.summary.status_counts)}`
          ] : [])
        }
      ];

      items.forEach((item) => {
        const card = document.createElement("div");
        card.className = "card";
        const title = document.createElement("h3");
        title.textContent = item.title;
        card.appendChild(title);
        item.lines.forEach((line) => {
          const p = document.createElement("p");
          p.textContent = line;
          card.appendChild(p);
        });
        artifactCards.appendChild(card);
      });
    }

    function renderBadges(artifacts) {
      if (!statusBadges) return;
      statusBadges.innerHTML = "";
      const counts = artifacts?.screen?.summary?.status_counts || {};
      [["PASS", "ok"], ["FAIL", "fail"], ["ERROR", "error"]].forEach(([key, cls]) => {
        const badge = document.createElement("div");
        badge.className = `badge ${cls}`;
        badge.textContent = `${key}: ${counts[key] || 0}`;
        statusBadges.appendChild(badge);
      });
    }

    function renderPreview(rows) {
      if (!previewWrap) return;
      previewWrap.innerHTML = "";
      if (!rows || !rows.length) {
        previewWrap.innerHTML = '<div class="empty">当前还没有 screen 结果，或者这个结果文件里没有可显示的行。</div>';
        return;
      }

      const tableWrap = document.createElement("div");
      tableWrap.className = "table-wrap";
      const table = document.createElement("table");
      const thead = document.createElement("thead");
      const headerRow = document.createElement("tr");
      Object.keys(rows[0]).forEach((key) => {
        const th = document.createElement("th");
        th.textContent = columnLabel(key);
        headerRow.appendChild(th);
      });
      thead.appendChild(headerRow);
      table.appendChild(thead);

      const tbody = document.createElement("tbody");
      rows.forEach((row) => {
        const tr = document.createElement("tr");
        Object.entries(row).forEach(([key, value]) => {
          const td = document.createElement("td");
          td.textContent = formatCell(key, value);
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      });
      table.appendChild(tbody);
      tableWrap.appendChild(table);
      previewWrap.appendChild(tableWrap);
    }

    function setButtonsDisabled(disabled) {
      [screenButton, runButton, refreshButton, saveButton, reloadButton, configSelect].forEach((button) => {
        if (button) button.disabled = disabled;
      });
      workflowRoot.querySelectorAll("button").forEach((button) => {
        button.disabled = disabled;
      });
    }

    async function getJson(url) {
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(await response.text());
      }
      return response.json();
    }

    async function postJson(url, payload) {
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      return response.json();
    }

    async function loadConfig(configName) {
      if (pollTimer) {
        clearInterval(pollTimer);
        pollTimer = null;
      }
      currentJobId = null;
      currentJobMode = null;
      pollFailureCount = 0;
      const state = await getJson(`/api/config?name=${encodeURIComponent(configName)}`);
      bootState = state;
      deploymentInfo = state.deployment || deploymentInfo;
      currentConfigName = state.config_name;
      currentConfig = state.config;
      currentDataFileOptions = state.data_file_options || [];
      currentDataFileCatalog = state.data_file_catalog || [];
      workflowTableState = {};
      currentWorkflow = inferWorkflowDefaults(state);
      saveNameInput.value = currentConfigName;
      saveNameInput.disabled = deploymentInfo.read_only_mode;
      saveButton.disabled = deploymentInfo.read_only_mode;
      saveButton.title = deploymentInfo.read_only_mode ? readOnlyMessage() : "";
      renderForm(currentConfig, state.universe_options || []);
      renderWorkflow();
      renderArtifacts(state.artifacts);
      renderBadges(state.artifacts);
      renderPreview(state.preview_rows);
      jobBanner.textContent = deploymentInfo.read_only_mode
        ? `${readOnlyMessage()} 当前加载的是 ${currentConfigName}。`
        : `已加载 ${currentConfigName}。正式主路径从 Step 1 开始：先缩候选池，再抓深数据做 Step 2。`;
      logBox.textContent = "还没有运行任务。";
    }

    async function refreshConfigList(selectedName) {
      const bootstrap = await getJson("/api/bootstrap");
      currentDataFileOptions = bootstrap.data_file_options || currentDataFileOptions;
      currentDataFileCatalog = bootstrap.data_file_catalog || currentDataFileCatalog;
      configSelect.innerHTML = "";
      bootstrap.config_options.forEach((name) => {
        const option = document.createElement("option");
        option.value = name;
        option.textContent = name;
        if (name === selectedName) option.selected = true;
        configSelect.appendChild(option);
      });
    }

    async function startJob(mode) {
      if (deploymentInfo.read_only_mode) {
        alert(readOnlyMessage());
        return;
      }
      const runtimeConfig = collectConfigFromForm();
      currentConfig = deepClone(runtimeConfig);
      setButtonsDisabled(true);
      jobBanner.textContent = mode === "screen"
        ? "正在执行兼容模式 screen，通常很快。"
        : mode === "refresh"
          ? "正在执行兼容模式 refresh，日志会持续更新。"
          : "正在执行兼容模式 run，日志会持续更新。";
      logBox.textContent = "任务已提交，等待第一条日志...";

      const response = await postJson("/api/jobs", {
        mode,
        config_name: currentConfigName,
        config: runtimeConfig
      });
      currentJobId = response.job_id;
      currentJobMode = mode;
      pollFailureCount = 0;
      if (pollTimer) clearInterval(pollTimer);
      pollTimer = setInterval(pollJob, 1500);
      await pollJob();
    }

    async function startWorkflowStep(mode) {
      const runtimeConfig = collectConfigFromForm();
      const blockReason = workflowStepBlockedReason(mode, runtimeConfig);
      if (blockReason) {
        alert(blockReason);
        return;
      }
      currentConfig = deepClone(runtimeConfig);
      const workflow = {
        ...deepClone(currentWorkflow || {}),
        settings: currentWorkflowSettings()
      };

      if (mode !== "workflow_step1" && !workflow.workflow_dir) {
        alert("请先完成第 1 步。");
        return;
      }

      currentWorkflow = workflow;
      setButtonsDisabled(true);
      jobBanner.textContent = mode === "workflow_step1"
        ? "正在运行候选池规则，并保存 step1 结果。"
        : mode === "workflow_step2"
          ? "正在抓取 Step 1 候选池的数据，并保存 step2 结果。"
          : "正在运行排序规则，并保存 step3 结果。";
      logBox.textContent = "任务已提交，等待第一条日志...";

      const response = await postJson("/api/jobs", {
        mode,
        config_name: currentConfigName,
        config: {
          ...runtimeConfig,
          workflow: mode === "workflow_step1"
            ? workflow.settings
            : mode === "workflow_step2"
              ? { workflow_dir: workflow.workflow_dir, settings: workflow.settings }
              : { workflow_dir: workflow.workflow_dir }
        }
      });
      currentJobId = response.job_id;
      currentJobMode = mode;
      pollFailureCount = 0;
      if (pollTimer) clearInterval(pollTimer);
      pollTimer = setInterval(pollJob, 1500);
      await pollJob();
    }

    async function pollJob() {
      if (!currentJobId) return;
      let job;
      try {
        job = await getJson(`/api/jobs/${encodeURIComponent(currentJobId)}`);
      } catch (error) {
        pollFailureCount += 1;
        jobBanner.textContent = `后台连接暂时中断，正在重试（${pollFailureCount}）...`;
        logBox.textContent = `${logBox.textContent || "任务仍在后台运行。"}\n\n轮询失败：${error.message || error}`;
        return;
      }
      pollFailureCount = 0;
      if (job.config_name && currentConfigName && job.config_name !== currentConfigName) {
        return;
      }
      logBox.textContent = job.log_text || "任务正在运行，还没有日志。";

      if (job.status === "running") {
        jobBanner.textContent = `任务 ${job.mode} 运行中，开始于 ${job.started_at}。`;
        return;
      }

      clearInterval(pollTimer);
      pollTimer = null;
      setButtonsDisabled(false);
      currentJobMode = null;

      if (job.status === "completed") {
        if (job.mode === "workflow_step1") {
          jobBanner.textContent = `第 1 步已完成，先看上方“流程结果”里的候选池审查和主要剔除原因，确认口径后再进入第 2 步。`;
        } else if (job.mode === "workflow_step2") {
          jobBanner.textContent = `第 2 步已完成，候选池的数据和期权链已经抓好。先看上方“流程结果”里的 FAIL 原因和 PASS 样本，再决定是否进入第 3 步。`;
        } else if (job.mode === "workflow_step3") {
          jobBanner.textContent = `第 3 步已完成，当前参数下的前排榜单已经在上方“流程结果”里。`;
        } else {
          jobBanner.textContent = `任务完成，结束于 ${job.finished_at}。`;
        }
        if (job.result) {
          if (job.result.workflow_state) {
            currentWorkflow = {
              ...(currentWorkflow || {}),
              ...job.result.workflow_state,
              settings: job.result.workflow_state.settings || currentWorkflow?.settings || currentWorkflowSettings()
            };
          }
          renderArtifacts(job.result.artifacts);
          renderBadges(job.result.artifacts);
          renderPreview(job.result.preview_rows);
        }
      } else {
        jobBanner.textContent = `任务失败：${job.error || "未知错误"}`;
      }
      renderWorkflow();
    }

    async function saveConfig() {
      if (deploymentInfo.read_only_mode) {
        alert(readOnlyMessage());
        return;
      }
      const targetName = saveNameInput.value.trim();
      if (!targetName) {
        alert("请先填一个 YAML 文件名。");
        return;
      }
      const runtimeConfig = collectConfigFromForm();
      setButtonsDisabled(true);
      try {
        const response = await postJson("/api/save-config", {
          config_name: targetName,
          config: runtimeConfig
        });
        await refreshConfigList(response.saved_name);
        await loadConfig(response.saved_name);
        jobBanner.textContent = `已保存到 ${response.saved_name}。`;
      } finally {
        setButtonsDisabled(false);
      }
    }

    async function bootstrap() {
      const response = await getJson("/api/bootstrap");
      deploymentInfo = response.deployment || deploymentInfo;
      currentDataFileOptions = response.data_file_options || [];
      currentDataFileCatalog = response.data_file_catalog || [];
      configSelect.innerHTML = "";
      response.config_options.forEach((name) => {
        const option = document.createElement("option");
        option.value = name;
        option.textContent = name;
        configSelect.appendChild(option);
      });
      const selected = response.selected_config || response.config_options[0];
      if (selected) {
        configSelect.value = selected;
        await loadConfig(selected);
      }
    }

    configSelect.addEventListener("change", () => loadConfig(configSelect.value));
    if (screenButton) screenButton.addEventListener("click", () => startJob("screen"));
    if (runButton) runButton.addEventListener("click", () => startJob("run"));
    if (refreshButton) refreshButton.addEventListener("click", () => startJob("refresh"));
    saveButton.addEventListener("click", saveConfig);
    reloadButton.addEventListener("click", () => loadConfig(configSelect.value));
    workflowRoot.addEventListener("click", (event) => {
      const target = event.target.closest("[data-workflow-sort], [data-workflow-limit], [data-workflow-page]");
      if (!target) return;
      if (target.dataset.workflowSort) {
        setWorkflowTableSort(target.dataset.workflowSort, target.dataset.workflowKey);
        renderWorkflow();
        return;
      }
      if (target.dataset.workflowLimit) {
        setWorkflowTableLimit(target.dataset.workflowLimit, target.dataset.limit);
        renderWorkflow();
        return;
      }
      if (target.dataset.workflowPage) {
        setWorkflowTablePage(target.dataset.workflowPage, target.dataset.pageAction);
        renderWorkflow();
      }
    });

    bootstrap().catch((error) => {
      jobBanner.textContent = `初始化失败：${error.message}`;
      logBox.textContent = error.stack || error.message;
    });
  </script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "CoveredCallDashboard/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        return None

    def send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_text(self, text: str, status: int = HTTPStatus.OK) -> None:
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, html: str, status: int = HTTPStatus.OK) -> None:
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        return json.loads(raw.decode("utf-8") or "{}")

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.send_html(HTML_PAGE)
            return

        if parsed.path == "/api/bootstrap":
            configs = discover_configs()
            selected = configs[0] if configs else None
            selected_config = None
            if selected:
                selected_config = load_config(resolve_local_config_path(selected))
            self.send_json(
                {
                    "config_options": configs,
                    "selected_config": selected,
                    "deployment": {
                        "target": DEPLOY_TARGET,
                        "read_only_mode": READ_ONLY_MODE,
                        "job_policy": "cloud_safe" if READ_ONLY_MODE else "full",
                    },
                    "universe_options": discover_universe_files(config=selected_config),
                    "data_file_options": discover_data_files(config=selected_config, config_name=selected),
                    "data_file_catalog": discover_data_file_catalog(config=selected_config, config_name=selected),
                }
            )
            return

        if parsed.path == "/api/config":
            query = parse_qs(parsed.query)
            config_name = query.get("name", [None])[0]
            if not config_name:
                self.send_text("Missing config name", HTTPStatus.BAD_REQUEST)
                return
            try:
                self.send_json(collect_config_state(config_name))
            except Exception as exc:
                self.send_text(str(exc), HTTPStatus.BAD_REQUEST)
            return

        if parsed.path.startswith("/api/jobs/"):
            job_id = parsed.path.split("/")[-1]
            with JOB_LOCK:
                job = JOBS.get(job_id)
                snapshot = snapshot_job(job) if job else None
            if snapshot is None:
                self.send_text("Job not found", HTTPStatus.NOT_FOUND)
                return
            self.send_json(snapshot)
            return

        if parsed.path == "/api/workflow":
            query = parse_qs(parsed.query)
            workflow_dir_value = query.get("dir", [None])[0]
            if not workflow_dir_value:
                self.send_text("Missing workflow dir", HTTPStatus.BAD_REQUEST)
                return
            workflow_dir = Path(workflow_dir_value).resolve()
            if not workflow_dir.exists():
                self.send_text("Workflow dir not found", HTTPStatus.NOT_FOUND)
                return
            self.send_json(read_workflow_state(workflow_dir))
            return

        self.send_text("Not found", HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            payload = self.read_json_body()
        except json.JSONDecodeError as exc:
            self.send_text(f"Invalid JSON: {exc}", HTTPStatus.BAD_REQUEST)
            return

        if parsed.path == "/api/jobs":
            mode = str(payload.get("mode") or "").strip()
            config_name = str(payload.get("config_name") or "").strip()
            config_payload = payload.get("config") or {}
            if not config_name:
                self.send_text("Missing config_name", HTTPStatus.BAD_REQUEST)
                return
            block_reason = cloud_job_block_reason(mode, config_name, config_payload)
            if block_reason:
                self.send_text(block_reason, HTTPStatus.FORBIDDEN)
                return
            if mode not in {"screen", "refresh", "run", "workflow_step1", "workflow_step2", "workflow_step3"}:
                self.send_text("Unsupported mode", HTTPStatus.BAD_REQUEST)
                return
            job_id = next_job_id()
            job = JobState(job_id=job_id, mode=mode, config_name=config_name)
            with JOB_LOCK:
                JOBS[job_id] = job
            thread = threading.Thread(
                target=run_job,
                args=(job_id, mode, config_name, config_payload),
                daemon=True,
            )
            thread.start()
            self.send_json({"job_id": job_id})
            return

        if parsed.path == "/api/save-config":
            if READ_ONLY_MODE:
                self.send_text("Cloud snapshot is read-only. Save YAML locally and redeploy if needed.", HTTPStatus.FORBIDDEN)
                return
            config_name = str(payload.get("config_name") or "").strip()
            config_payload = payload.get("config") or {}
            if not config_name:
                self.send_text("Missing config_name", HTTPStatus.BAD_REQUEST)
                return
            normalized = sanitize_config_payload(config_payload)

            target_path = resolve_local_config_path(config_name)
            target_path.write_text(
                yaml.safe_dump(normalized, sort_keys=False, allow_unicode=True),
                encoding="utf-8",
            )
            self.send_json(
                {
                    "saved_name": target_path.name,
                    "config_options": discover_configs(),
                }
            )
            return

        self.send_text("Not found", HTTPStatus.NOT_FOUND)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local tuning dashboard for covered-call screening.")
    cloud_mode = RENDER_ENV or READ_ONLY_MODE
    default_host = os.getenv("COVERED_CALL_HOST") or ("0.0.0.0" if cloud_mode else "127.0.0.1")
    default_port = int(os.getenv("PORT") or 8765)
    parser.add_argument("--host", default=default_host, help=f"Host to bind. Default: {default_host}")
    parser.add_argument("--port", default=default_port, type=int, help=f"Port to bind. Default: {default_port}")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"Dashboard running at http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
