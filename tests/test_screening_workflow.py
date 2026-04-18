from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

import screening_workflow


class ScreeningWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self.tempdir.name).resolve()

        (self.base_dir / "out").mkdir(exist_ok=True)
        self.universe_path = self.base_dir / "etf_broad_universe.csv"
        self.metrics_path = self.base_dir / "out" / "etf_broad_metrics.csv"
        self.config_path = self.base_dir / "etf_broad_config.yaml"
        self.config_path.write_text("data:\n  cache_dir: out\n  metrics_filename: etf_broad_metrics.csv\n", encoding="utf-8")

        universe_df = pd.DataFrame(
            [
                {
                    "symbol": "EQ1",
                    "name": "Example Equity ETF",
                    "currency": "USD",
                    "summary": "Holds common stocks of listed companies.",
                    "category_group": "Equities",
                    "exchange": "PCX",
                    "avg_dollar_volume": 200_000_000,
                },
                {
                    "symbol": "CM1",
                    "name": "Example Gold Commodity ETF",
                    "currency": "USD",
                    "summary": "Tracks gold bullion commodity exposure.",
                    "category_group": "Commodities",
                    "exchange": "PCX",
                    "avg_dollar_volume": 150_000_000,
                },
                {
                    "symbol": "EQ2",
                    "name": "Small Equity ETF",
                    "currency": "USD",
                    "summary": "Holds common stocks of listed companies.",
                    "category_group": "Equities",
                    "exchange": "PCX",
                    "avg_dollar_volume": 20_000_000,
                },
                {
                    "symbol": "FI1",
                    "name": "Long Treasury Bond ETF",
                    "currency": "USD",
                    "summary": "Tracks long-dated treasury bond exposure in the fixed income market.",
                    "category_group": "Fixed Income",
                    "exchange": "PCX",
                    "avg_dollar_volume": 140_000_000,
                },
            ]
        )
        universe_df.to_csv(self.universe_path, index=False)

        self.metrics_df = pd.DataFrame(
            [
                {
                    "ticker": "EQ1",
                    "asset_type": "ETF",
                    "size_metric": 10_000_000_000,
                    "price": 50,
                    "dividend_yield": 0.03,
                    "annualized_call_yield": 0.08,
                    "selected_call_open_interest": 100,
                    "selected_call_volume": 10,
                    "selected_call_spread_pct": 0.02,
                    "beta_1y": 0.9,
                    "realized_vol_1y": 0.2,
                    "total_return_3y": 0.15,
                    "max_drawdown_3y": 0.2,
                },
                {
                    "ticker": "CM1",
                    "asset_type": "ETF",
                    "size_metric": 8_000_000_000,
                    "price": 40,
                    "dividend_yield": 0.025,
                    "annualized_call_yield": 0.07,
                    "selected_call_open_interest": 80,
                    "selected_call_volume": 8,
                    "selected_call_spread_pct": 0.03,
                    "beta_1y": 0.3,
                    "realized_vol_1y": 0.18,
                    "total_return_3y": 0.12,
                    "max_drawdown_3y": 0.18,
                },
                {
                    "ticker": "EQ2",
                    "asset_type": "ETF",
                    "size_metric": 500_000_000,
                    "price": 12,
                    "dividend_yield": 0.01,
                    "annualized_call_yield": 0.01,
                    "selected_call_open_interest": 0,
                    "selected_call_volume": 0,
                    "selected_call_spread_pct": 0.2,
                    "beta_1y": 1.8,
                    "realized_vol_1y": 0.6,
                    "total_return_3y": -0.2,
                    "max_drawdown_3y": 0.7,
                },
                {
                    "ticker": "FI1",
                    "asset_type": "ETF",
                    "size_metric": 7_000_000_000,
                    "price": 30,
                    "dividend_yield": 0.04,
                    "annualized_call_yield": 0.05,
                    "selected_call_open_interest": 90,
                    "selected_call_volume": 12,
                    "selected_call_spread_pct": 0.03,
                    "beta_1y": 0.2,
                    "realized_vol_1y": 0.14,
                    "total_return_3y": 0.02,
                    "max_drawdown_3y": 0.16,
                },
            ]
        )
        self.metrics_df.to_csv(self.metrics_path, index=False)

        self.workflow_dir = screening_workflow.create_workflow_dir(
            self.base_dir, "etf_equity_commodity_config.yaml"
        )
        self.config = {
            "data": {
                "cache_dir": "out",
                "metrics_filename": "etf_broad_metrics.csv",
                "history_period": "4y",
                "benchmark": "SPY",
                "market_data_source": "hybrid",
                "resume_existing_metrics": True,
                "progress_every": 10,
                "checkpoint_every": 10,
                "as_of_date": "2026-04-16",
                "ibkr": {
                    "host": "127.0.0.1",
                    "port": 4001,
                    "readonly": True,
                    "timeout": 10,
                },
            },
            "screen": {
                "min_price": 15,
                "min_size_metric": 3_000_000_000,
                "min_dividend_yield": 0.02,
                "min_call_annualized_yield": 0.03,
                "min_call_open_interest": 50,
                "min_call_volume": 3,
                "max_option_spread_pct": 0.12,
                "min_beta_1y": -0.25,
                "max_beta_1y": 1.25,
                "min_realized_vol_1y": 0.08,
                "max_realized_vol_1y": 0.45,
                "min_total_return_3y": -0.10,
                "max_drawdown_3y": 0.50,
                "allowed_asset_types": ["ETF"],
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
        }

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_candidate_pool_step_saves_intermediate_outputs(self) -> None:
        settings = {
            "source_universe_csv": self.universe_path.name,
            "allowed_market_classes": ["equity", "commodity"],
            "require_usd": True,
            "require_us_listing": True,
            "require_etf_asset_type": True,
            "min_avg_dollar_volume": 100_000_000,
            "min_size_metric": 3_000_000_000,
        }

        summary = screening_workflow.run_candidate_pool_step(
            base_dir=self.base_dir,
            workflow_dir=self.workflow_dir,
            workflow_settings=settings,
            config_name="etf_equity_commodity_config.yaml",
        )

        self.assertEqual(summary["candidate_rows"], 2)
        self.assertEqual(summary["status_counts"]["PASS"], 2)
        self.assertEqual(summary["status_counts"]["FAIL"], 2)
        self.assertEqual(len(summary["review_preview_rows"]), 4)
        self.assertTrue(summary["top_fail_reasons"])
        self.assertIn(
            {"reason": "market_bucket_not_allowed:fixed_income", "count": 1},
            summary["top_fail_reasons"],
        )
        self.assertTrue((self.workflow_dir / "step1_candidate_universe.csv").exists())
        self.assertFalse((self.workflow_dir / "step1_candidate_metrics.csv").exists())
        self.assertEqual(summary["artifacts"]["local_enrichment_csv"], "out/etf_broad_metrics.csv")
        candidate_universe = pd.read_csv(self.workflow_dir / "step1_candidate_universe.csv")
        self.assertEqual(set(candidate_universe["symbol"]), {"EQ1", "CM1"})
        state = screening_workflow.read_workflow_state(self.workflow_dir)
        self.assertEqual(state["settings"]["allowed_market_classes"], ["equity", "commodity"])

    def test_default_workflow_settings_disable_adv_for_sp500_source(self) -> None:
        (self.base_dir / "sp500_constituents.csv").write_text("symbol,security\nMMM,3M\n", encoding="utf-8")
        config = {
            "universe": {"path": "sp500_constituents.csv"},
            "screen": {"min_size_metric": 10_000_000_000},
        }
        settings = screening_workflow.default_workflow_settings(config, base_dir=self.base_dir)
        self.assertEqual(settings["source_universe_csv"], "sp500_constituents.csv")
        self.assertIsNone(settings["min_avg_dollar_volume"])
        self.assertEqual(settings["allowed_market_classes"], ["equity"])
        self.assertFalse(settings["require_etf_asset_type"])

    def test_candidate_pool_step_handles_missing_avg_dollar_volume_column(self) -> None:
        sp500_like_path = self.base_dir / "sp500_constituents.csv"
        pd.DataFrame(
            [
                {"symbol": "EQ1", "security": "Example 1"},
                {"symbol": "CM1", "security": "Example 2"},
            ]
        ).to_csv(sp500_like_path, index=False)
        settings = {
            "source_universe_csv": sp500_like_path.name,
            "allowed_market_classes": ["equity", "commodity", "other"],
            "require_usd": False,
            "require_us_listing": False,
            "require_etf_asset_type": False,
            "min_avg_dollar_volume": None,
            "min_size_metric": None,
        }

        summary = screening_workflow.run_candidate_pool_step(
            base_dir=self.base_dir,
            workflow_dir=self.workflow_dir,
            workflow_settings=settings,
            config_name="sp500_config.yaml",
        )

        self.assertEqual(summary["candidate_rows"], 2)
        self.assertEqual(summary["status_counts"]["PASS"], 2)

    def test_candidate_pool_step_ignores_adv_threshold_when_column_unavailable(self) -> None:
        sp500_like_path = self.base_dir / "sp500_constituents.csv"
        pd.DataFrame(
            [
                {"symbol": "EQ1", "security": "Example 1"},
                {"symbol": "CM1", "security": "Example 2"},
            ]
        ).to_csv(sp500_like_path, index=False)
        settings = {
            "source_universe_csv": sp500_like_path.name,
            "allowed_market_classes": ["equity", "commodity", "other"],
            "require_usd": False,
            "require_us_listing": False,
            "require_etf_asset_type": False,
            "min_avg_dollar_volume": 10_000_000,
            "min_size_metric": None,
        }

        summary = screening_workflow.run_candidate_pool_step(
            base_dir=self.base_dir,
            workflow_dir=self.workflow_dir,
            workflow_settings=settings,
            config_name="sp500_config.yaml",
        )

        self.assertEqual(summary["candidate_rows"], 2)
        self.assertEqual(summary["status_counts"]["PASS"], 2)
        self.assertIn("当前 Universe 不含可用的日均成交额，已自动忽略“最低日均成交额”条件。", summary["warnings"])
        self.assertFalse(any(item["reason"].startswith("avg_dollar_volume<") for item in summary["top_fail_reasons"]))

    def test_hard_filter_and_ranking_steps_chain(self) -> None:
        settings = {
            "source_universe_csv": self.universe_path.name,
            "allowed_market_classes": ["equity", "commodity"],
            "require_usd": True,
            "require_us_listing": True,
            "require_etf_asset_type": True,
            "min_avg_dollar_volume": 100_000_000,
            "min_size_metric": 3_000_000_000,
        }
        screening_workflow.run_candidate_pool_step(
            base_dir=self.base_dir,
            workflow_dir=self.workflow_dir,
            workflow_settings=settings,
            config_name="etf_equity_commodity_config.yaml",
        )

        captured_kwargs: dict[str, object] = {}

        def mock_refresh(*, tickers: list[str], output_path: Path, **kwargs: object) -> pd.DataFrame:
            captured_kwargs.update(kwargs)
            subset = self.metrics_df[self.metrics_df["ticker"].isin(tickers)].copy()
            subset.to_csv(output_path, index=False)
            return subset

        with mock.patch.object(
            screening_workflow,
            "refresh_metrics_for_tickers",
            side_effect=mock_refresh,
        ):
            hard_filter_summary = screening_workflow.run_hard_filter_step(
                base_dir=self.base_dir,
                workflow_dir=self.workflow_dir,
                config=self.config,
                config_path=self.config_path,
            )
        ranking_summary = screening_workflow.run_ranking_step(
            workflow_dir=self.workflow_dir,
            config=self.config,
        )

        self.assertEqual(hard_filter_summary["status_counts"]["PASS"], 2)
        self.assertEqual(hard_filter_summary["top_fail_reasons"], [])
        self.assertEqual(hard_filter_summary["market_data_source"], "hybrid")
        self.assertEqual(hard_filter_summary["ibkr_connection"], "127.0.0.1:4001")
        self.assertTrue((self.workflow_dir / "step2_candidate_metrics.csv").exists())
        self.assertTrue((self.workflow_dir / "step2_hard_filter.csv").exists())
        self.assertIn("metadata_by_ticker", captured_kwargs)
        metadata_by_ticker = captured_kwargs["metadata_by_ticker"]
        self.assertIsInstance(metadata_by_ticker, dict)
        self.assertEqual(metadata_by_ticker["EQ1"]["asset_type"], "ETF")
        self.assertEqual(metadata_by_ticker["EQ1"]["size_metric"], 10_000_000_000)
        self.assertEqual(ranking_summary["rows"], 2)
        ranked = pd.read_csv(self.workflow_dir / "step3_ranked.csv")
        self.assertEqual(list(ranked["ticker"]), ["EQ1", "CM1"])

    def test_default_workflow_settings_uses_existing_universe_csv(self) -> None:
        config = {
            "universe": {"path": "starter_universe.txt"},
            "data": {"cache_dir": "out", "metrics_filename": "etf_broad_metrics.csv"},
        }
        (self.base_dir / "etf_broad_universe.csv").write_text("symbol\nXLE\n", encoding="utf-8")
        (self.base_dir / "out").mkdir(exist_ok=True)
        (self.base_dir / "out" / "etf_broad_metrics.csv").write_text("ticker\nXLE\n", encoding="utf-8")

        settings = screening_workflow.default_workflow_settings(config, base_dir=self.base_dir)

        self.assertEqual(settings["source_universe_csv"], "etf_broad_universe.csv")
        self.assertNotIn("source_metrics_csv", settings)
        self.assertFalse(settings["step2_reuse_same_day_cache"])

    def test_default_workflow_settings_prefers_metrics_paired_with_universe(self) -> None:
        config = {
            "universe": {"path": "starter_universe.txt"},
            "data": {"cache_dir": "out", "metrics_filename": "latest_metrics.csv"},
        }
        (self.base_dir / "etf_broad_universe.csv").write_text("symbol\nXLE\n", encoding="utf-8")
        (self.base_dir / "out").mkdir(exist_ok=True)
        (self.base_dir / "out" / "etf_broad_metrics.csv").write_text(
            "ticker\nXLE\n",
            encoding="utf-8",
        )

        settings = screening_workflow.default_workflow_settings(config, base_dir=self.base_dir)

        self.assertEqual(settings["source_universe_csv"], "etf_broad_universe.csv")
        self.assertNotIn("source_metrics_csv", settings)
        self.assertFalse(settings["step2_reuse_same_day_cache"])


if __name__ == "__main__":
    unittest.main()
