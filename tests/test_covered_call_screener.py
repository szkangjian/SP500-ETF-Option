from __future__ import annotations

import asyncio
import threading
import unittest
from datetime import date

import pandas as pd

import covered_call_screener


class CoveredCallScreenerTests(unittest.TestCase):
    def test_build_metrics_signature_includes_market_data_source(self) -> None:
        config = covered_call_screener.deep_merge(
            covered_call_screener.DEFAULT_CONFIG,
            {"data": {"market_data_source": "hybrid"}},
        )

        signature = covered_call_screener.build_metrics_signature(config)

        self.assertIn('"market_data_source":"hybrid"', signature)
        self.assertIn('"min_otm_pct":0.05', signature)
        self.assertIn('"max_otm_pct":0.15', signature)

    def test_resolve_otm_preferences_supports_legacy_target(self) -> None:
        min_otm, max_otm, preferred_otm = covered_call_screener.resolve_otm_preferences(
            {"target_otm_pct": 0.07}
        )

        self.assertEqual((min_otm, max_otm, preferred_otm), (0.07, 0.07, 0.07))

    def test_choose_expiration_allows_weekly_when_monthly_only_disabled(self) -> None:
        expiration, dte = covered_call_screener.choose_expiration(
            expirations=["2026-05-08", "2026-05-15", "2026-05-22"],
            as_of=date(2026, 4, 16),
            min_dte=20,
            max_dte=30,
            target_dte=25,
            monthly_only=False,
        )

        self.assertEqual(expiration, "2026-05-08")
        self.assertEqual(dte, 22)

    def test_choose_expiration_filters_to_standard_monthly_when_enabled(self) -> None:
        expiration, dte = covered_call_screener.choose_expiration(
            expirations=["2026-05-08", "2026-05-15", "2026-05-22"],
            as_of=date(2026, 4, 16),
            min_dte=20,
            max_dte=30,
            target_dte=25,
            monthly_only=True,
        )

        self.assertEqual(expiration, "2026-05-15")
        self.assertEqual(dte, 29)

    def test_normalize_ib_expiration_formats_yyyymmdd(self) -> None:
        self.assertEqual(covered_call_screener.normalize_ib_expiration("20260515"), "2026-05-15")

    def test_ensure_asyncio_event_loop_creates_loop_in_thread(self) -> None:
        results: list[bool] = []

        def worker() -> None:
            loop = covered_call_screener.ensure_asyncio_event_loop()
            results.append(isinstance(loop, asyncio.AbstractEventLoop))
            loop.close()

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join()

        self.assertEqual(results, [True])

    def test_select_call_option_prefers_in_range_strike(self) -> None:
        calls = pd.DataFrame(
            [
                {
                    "strike": 103.0,
                    "bid": 1.10,
                    "ask": 1.30,
                    "lastPrice": 1.20,
                    "openInterest": 500,
                    "volume": 200,
                },
                {
                    "strike": 108.0,
                    "bid": 0.70,
                    "ask": 0.82,
                    "lastPrice": 0.76,
                    "openInterest": 400,
                    "volume": 150,
                },
            ]
        )

        selected = covered_call_screener.select_call_option(
            calls=calls,
            spot=100.0,
            min_otm_pct=0.05,
            max_otm_pct=0.15,
            preferred_otm_pct=0.08,
            dte=30,
        )

        self.assertEqual(selected.strike, 108.0)
        self.assertAlmostEqual(selected.otm_pct, 0.08)


if __name__ == "__main__":
    unittest.main()
