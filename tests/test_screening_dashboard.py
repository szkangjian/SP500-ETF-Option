from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

import screening_dashboard


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ScreeningDashboardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self.tempdir.name).resolve()
        self.config_name = "progress_test_config.yaml"
        self.config_path = self.base_dir / self.config_name
        self.config_path.write_text(
            (PROJECT_ROOT / self.config_name).read_text(encoding="utf-8"),
            encoding="utf-8",
        )

        out_dir = self.base_dir / "out"
        out_dir.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(
            PROJECT_ROOT / "out" / "progress_test_metrics.csv",
            out_dir / "progress_test_metrics.csv",
        )
        shutil.copyfile(
            PROJECT_ROOT / "out" / "progress_test_screen.csv",
            out_dir / "progress_test_screen.csv",
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_collect_config_state_reads_existing_files(self) -> None:
        with mock.patch.object(screening_dashboard, "BASE_DIR", self.base_dir):
            state = screening_dashboard.collect_config_state(self.config_name)

        self.assertEqual(state["config_name"], self.config_name)
        self.assertTrue(state["artifacts"]["metrics"]["exists"])
        self.assertEqual(state["artifacts"]["metrics"]["summary"]["rows"], 2)
        self.assertEqual(state["artifacts"]["screen"]["summary"]["status_counts"]["PASS"], 2)
        self.assertEqual(len(state["preview_rows"]), 2)
        catalog_names = {item["name"] for item in state["data_file_catalog"]}
        self.assertIn("out/progress_test_metrics.csv", catalog_names)
        metrics_entry = next(item for item in state["data_file_catalog"] if item["name"] == "out/progress_test_metrics.csv")
        self.assertEqual(metrics_entry["category"], "metrics")

    def test_run_job_screen_applies_runtime_override(self) -> None:
        job = screening_dashboard.JobState(
            job_id="job-1",
            mode="screen",
            config_name=self.config_name,
        )

        with (
            mock.patch.object(screening_dashboard, "BASE_DIR", self.base_dir),
            mock.patch.dict(screening_dashboard.JOBS, {"job-1": job}, clear=True),
        ):
            screening_dashboard.run_job(
                job_id="job-1",
                mode="screen",
                config_name=self.config_name,
                payload_config={"screen": {"min_call_annualized_yield": 0.20}},
            )

        self.assertEqual(job.status, "completed")
        self.assertIsNotNone(job.result)
        self.assertIn("Status counts:", job.log_text)

        screen_df = pd.read_csv(self.base_dir / "out" / "progress_test_screen.csv")
        status_by_ticker = dict(zip(screen_df["ticker"], screen_df["status"]))
        self.assertEqual(status_by_ticker["VZ"], "FAIL")
        self.assertEqual(status_by_ticker["SCHD"], "FAIL")

    def test_discover_data_files_prefers_official_and_current_config_files(self) -> None:
        (self.base_dir / "finance_etfs.csv").write_text("symbol\nSPY\n", encoding="utf-8")
        (self.base_dir / "etf_broad_universe.csv").write_text("symbol\nSPY\n", encoding="utf-8")
        (self.base_dir / "etf_broad_universe.txt").write_text("SPY\n", encoding="utf-8")
        (self.base_dir / "sp500_constituents.csv").write_text("symbol\nMMM\n", encoding="utf-8")
        (self.base_dir / "etf_equity_metrics.csv").write_text("ticker\nOLD\n", encoding="utf-8")
        nested = self.base_dir / "out" / "workflow_runs" / "out" / "workflow_runs" / "bad"
        nested.mkdir(parents=True, exist_ok=True)
        (nested / "step1_candidate_review.csv").write_text("symbol\nBAD\n", encoding="utf-8")
        latest = self.base_dir / "out" / "workflow_runs" / "20260417-progress_test"
        latest.mkdir(parents=True, exist_ok=True)
        (latest / "step1_candidate_review.csv").write_text("symbol\nGOOD\n", encoding="utf-8")
        (latest / "workflow_state.json").write_text("{}", encoding="utf-8")
        (latest / "step1_candidate_tickers.txt").write_text("GOOD\n", encoding="utf-8")
        older = self.base_dir / "out" / "workflow_runs" / "20260416-progress_test"
        older.mkdir(parents=True, exist_ok=True)
        (older / "step1_candidate_review.csv").write_text("symbol\nOLD\n", encoding="utf-8")
        unrelated = self.base_dir / "out" / "workflow_runs" / "20260417-etf_broad"
        unrelated.mkdir(parents=True, exist_ok=True)
        (unrelated / "step1_candidate_review.csv").write_text("symbol\nOTHER\n", encoding="utf-8")

        with mock.patch.object(screening_dashboard, "BASE_DIR", self.base_dir):
            config = screening_dashboard.load_config(self.config_path)
            discovered = screening_dashboard.discover_data_files(
                config=config,
                config_name=self.config_name,
            )

        self.assertIn("finance_etfs.csv", discovered)
        self.assertIn("out/progress_test_metrics.csv", discovered)
        self.assertIn("out/workflow_runs/20260417-progress_test/step1_candidate_review.csv", discovered)
        self.assertNotIn("etf_equity_metrics.csv", discovered)
        self.assertNotIn("etf_broad_universe.txt", discovered)
        self.assertNotIn("out/workflow_runs/20260417-progress_test/workflow_state.json", discovered)
        self.assertNotIn("out/workflow_runs/20260417-progress_test/step1_candidate_tickers.txt", discovered)
        self.assertNotIn("out/workflow_runs/20260416-progress_test/step1_candidate_review.csv", discovered)
        self.assertNotIn("out/workflow_runs/20260417-etf_broad/step1_candidate_review.csv", discovered)
        self.assertNotIn("out/workflow_runs/out/workflow_runs/bad/step1_candidate_review.csv", discovered)

    def test_sanitize_config_payload_normalizes_optional_fields(self) -> None:
        raw_config = {
            "data": {"as_of_date": ""},
            "universe": {"limit": ""},
            "screen": {"allowed_asset_types": "ETF, EQUITY", "target_otm_pct": 0.06},
        }

        normalized = screening_dashboard.sanitize_config_payload(raw_config)

        self.assertIsNone(normalized["data"]["as_of_date"])
        self.assertIsNone(normalized["universe"]["limit"])
        self.assertEqual(normalized["screen"]["allowed_asset_types"], ["ETF", "EQUITY"])
        self.assertEqual(normalized["screen"]["min_otm_pct"], 0.06)
        self.assertEqual(normalized["screen"]["max_otm_pct"], 0.06)
        self.assertEqual(normalized["screen"]["preferred_otm_pct"], 0.06)

    def test_cloud_job_block_reason_allows_only_safe_render_workflow(self) -> None:
        with (
            mock.patch.object(screening_dashboard, "BASE_DIR", self.base_dir),
            mock.patch.object(screening_dashboard, "READ_ONLY_MODE", True),
        ):
            self.assertIsNone(
                screening_dashboard.cloud_job_block_reason(
                    "workflow_step1",
                    self.config_name,
                    {},
                )
            )
            self.assertIsNone(
                screening_dashboard.cloud_job_block_reason(
                    "workflow_step2",
                    self.config_name,
                    {"data": {"market_data_source": "yahoo"}},
                )
            )
            self.assertIn(
                "Yahoo 数据源",
                screening_dashboard.cloud_job_block_reason(
                    "workflow_step2",
                    self.config_name,
                    {"data": {"market_data_source": "ibkr"}},
                ),
            )
            self.assertIn(
                "正式三步流程",
                screening_dashboard.cloud_job_block_reason(
                    "screen",
                    self.config_name,
                    {},
                ),
            )

    def test_workflow_csv_sources_render_as_select_dropdowns(self) -> None:
        html = screening_dashboard.HTML_PAGE

        self.assertIn("CSV 文件说明", html)
        self.assertIn("流程结果", html)
        self.assertIn("年化 Call 收益率", html)
        self.assertIn("当前股价", html)
        self.assertIn("命中 Strike", html)
        self.assertIn("Call 买价", html)
        self.assertIn("Call 卖价", html)
        self.assertIn("命中 OTM", html)
        self.assertIn("最小 OTM %", html)
        self.assertIn("最大 OTM %", html)
        self.assertIn("偏好 OTM %", html)
        self.assertIn("股息率 %", html)
        self.assertIn("最低股息率 %", html)
        self.assertIn("最大点差比例 %", html)
        self.assertIn("仅看月度到期", html)
        self.assertIn("Step 2 数据源", html)
        self.assertIn("IB Port", html)
        self.assertIn("limit: 20", html)
        self.assertIn("仅看月度时，这个参数只在多个月度到期同时落入窗口时才更有意义。", html)
        self.assertIn("自动把 DTE 最大值从默认的 45 放宽到 56", html)
        self.assertIn("日均成交额低于门槛", html)
        self.assertIn("债券/固定收益", html)
        self.assertIn("data-workflow-sort", html)
        self.assertIn("可点列标题排序", html)
        self.assertIn("data-market-class", html)
        self.assertIn("默认只勾“股票、商品”", html)
        self.assertIn("上一页", html)
        self.assertIn("下一页", html)
        self.assertIn("每页", html)
        self.assertIn("最小规模", html)
        self.assertNotIn("最低价格", html)
        self.assertNotIn("只保留 size_metric 高于中位数", html)
        self.assertNotIn("只保留日成交额高于中位数", html)
        self.assertIn("workflow-stack", html)
        self.assertIn("Step 1 结果", html)
        self.assertIn("Step 2 结果", html)
        self.assertIn("Step 3 结果", html)
        self.assertIn("Step 1 提示", html)
        self.assertIn("正式主路径只走三步", html)
        self.assertIn("执行状态", html)
        self.assertIn("准备就绪。先加载配置，再从 Step 1 开始。", html)
        self.assertIn("正式主路径不再在这里展示旧的一步式 screen 结果。", html)
        self.assertIn("Step 1 只用这份 Universe 做轻筛", html)
        self.assertIn("抓取数据并硬过滤", html)
        self.assertIn("先在这里选数据源、调硬过滤参数", html)
        self.assertIn("数据源和 IB 连接参数放在 Step 2 卡片里", html)
        self.assertIn("复用同日缓存（不勾选则强制重抓）", html)
        self.assertIn('class="workflow-fields two-col" id="workflowStep2Fields"', html)
        self.assertIn(".checkbox-field-head", html)
        self.assertIn(".checkbox-full-row", html)
        self.assertIn('<select id="workflowSourceUniverse">', html)
        self.assertNotIn('input id="workflowSourceUniverse" type="text"', html)
        self.assertNotIn("workflowSourceMetrics", html)
        self.assertNotIn("目标 OTM %", html)
        self.assertNotIn("只重筛", html)
        self.assertNotIn("刷新并筛选", html)
        self.assertNotIn("只刷新数据", html)
        self.assertNotIn("结果区", html)
        self.assertNotIn("前排结果", html)


if __name__ == "__main__":
    unittest.main()
