from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

import covered_call_screener


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class OptionalDependencyTests(unittest.TestCase):
    def test_dashboard_imports_without_yfinance_or_tabulate(self) -> None:
        code = """
import sys
sys.modules['yfinance'] = None
sys.modules['tabulate'] = None
import screening_dashboard
print('imported', screening_dashboard.__name__)
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("imported screening_dashboard", result.stdout)

    def test_refresh_requires_yfinance_with_clear_message(self) -> None:
        with mock.patch.object(covered_call_screener, "yf", None):
            with self.assertRaises(ModuleNotFoundError) as context:
                covered_call_screener.require_yfinance()

        self.assertIn("yfinance is required for refresh/market-data actions", str(context.exception))


if __name__ == "__main__":
    unittest.main()
