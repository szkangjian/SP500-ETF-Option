from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class YamlCompatTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_force_simple = os.environ.get("FORCE_SIMPLE_YAML")
        sys.modules.pop("yaml_compat", None)

    def tearDown(self) -> None:
        if self.original_force_simple is None:
            os.environ.pop("FORCE_SIMPLE_YAML", None)
        else:
            os.environ["FORCE_SIMPLE_YAML"] = self.original_force_simple
        sys.modules.pop("yaml_compat", None)

    def load_module(self, force_simple: bool):
        if force_simple:
            os.environ["FORCE_SIMPLE_YAML"] = "1"
        else:
            os.environ.pop("FORCE_SIMPLE_YAML", None)
        sys.modules.pop("yaml_compat", None)
        return importlib.import_module("yaml_compat")

    def test_simple_loader_parses_project_config(self) -> None:
        yaml_compat = self.load_module(force_simple=True)
        config_text = (PROJECT_ROOT / "progress_test_config.yaml").read_text(encoding="utf-8")

        config = yaml_compat.safe_load(config_text)

        self.assertEqual(config["data"]["metrics_filename"], "progress_test_metrics.csv")
        self.assertEqual(config["screen"]["min_call_annualized_yield"], 0.04)
        self.assertEqual(config["universe"]["tickers"], ["VZ", "SCHD"])

    def test_simple_dump_round_trip(self) -> None:
        yaml_compat = self.load_module(force_simple=True)
        payload = {
            "data": {"benchmark": "SPY", "as_of_date": None},
            "screen": {"allowed_asset_types": ["ETF"], "min_call_annualized_yield": 0.03},
            "output": {"show_failed": False},
        }

        dumped = yaml_compat.safe_dump(payload, sort_keys=False, allow_unicode=True)
        loaded = yaml_compat.safe_load(dumped)

        self.assertEqual(loaded, payload)


if __name__ == "__main__":
    unittest.main()
