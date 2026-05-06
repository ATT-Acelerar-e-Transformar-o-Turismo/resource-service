"""Tests for the column-hint plumbing added in Phase 2.

Verifies that source_config.{sheet_name, time_column, value_columns} flow into
the rendered XLSX / CSV instructions. Without these hints, Gemini guesses a
single column — the user reported this drops 9 of 10 columns from a multi-
column sheet.

Run inside the resource-service container (which has jinja2):
    docker exec resource-service python3 -m unittest tests.test_prompt_manager -v
"""

import unittest
from types import SimpleNamespace

from services.prompt_manager import PromptManager


class XlsxColumnHintTests(unittest.TestCase):
    def setUp(self):
        self.pm = PromptManager()

    def test_no_column_hint_omits_selection_block(self):
        # source_config is None or has no value_columns → no "USE EXACTLY THESE"
        out = self.pm._get_source_specific_instructions("XLSX", "Annual", None)
        self.assertNotIn("USE EXACTLY THESE", out)

    def test_value_columns_renders_selection_block(self):
        cfg = SimpleNamespace(
            sheet_name="Dados",
            time_column="Anos",
            value_columns=[
                "Total",
                "Gestão de águas residuais",
                "Protecção contra ruídos e vibrações",
            ],
        )
        out = self.pm._get_source_specific_instructions("XLSX", "Annual", cfg)
        self.assertIn("USE EXACTLY THESE", out)
        self.assertIn("Sheet: Dados", out)
        self.assertIn("Time/X column: Anos", out)
        # Each picked column should be rendered into the prompt
        self.assertIn("Total", out)
        self.assertIn("Gestão de águas residuais", out)
        self.assertIn("Protecção contra ruídos e vibrações", out)
        # Multi-column emission instruction
        self.assertIn("series", out.lower())

    def test_value_columns_without_sheet_falls_back_to_default_phrase(self):
        cfg = SimpleNamespace(
            sheet_name=None,
            time_column="Year",
            value_columns=["Total"],
        )
        out = self.pm._get_source_specific_instructions("XLSX", "Annual", cfg)
        self.assertIn("USE EXACTLY THESE", out)
        # When no sheet was picked the template tells the AI to use the first
        # data sheet — the literal phrase keeps the prompt unambiguous.
        self.assertIn("first sheet with data", out)

    def test_empty_value_columns_means_no_selection_block(self):
        cfg = SimpleNamespace(sheet_name="Dados", time_column="Year", value_columns=None)
        out = self.pm._get_source_specific_instructions("XLSX", "Annual", cfg)
        self.assertNotIn("USE EXACTLY THESE", out)


class CsvColumnHintTests(unittest.TestCase):
    def setUp(self):
        self.pm = PromptManager()

    def test_no_column_hint_omits_selection_block(self):
        out = self.pm._get_source_specific_instructions("CSV", "Annual", None)
        self.assertNotIn("USE EXACTLY THESE", out)

    def test_value_columns_renders_selection_block(self):
        cfg = SimpleNamespace(time_column="date", value_columns=["visitors", "revenue"])
        out = self.pm._get_source_specific_instructions("CSV", "Annual", cfg)
        self.assertIn("USE EXACTLY THESE", out)
        self.assertIn("Time/X column: date", out)
        self.assertIn("visitors", out)
        self.assertIn("revenue", out)
        # CSV instructions should not mention sheets — that concept is XLSX-only.
        self.assertNotIn("Sheet:", out)


class ApiInstructionsUntouchedTests(unittest.TestCase):
    """Sanity check: API instructions don't accidentally render column-picker
    blocks even though the new signature accepts source_config."""

    def setUp(self):
        self.pm = PromptManager()

    def test_api_template_ignores_source_config(self):
        cfg = SimpleNamespace(
            sheet_name="Dados", time_column="Anos", value_columns=["Total"]
        )
        out = self.pm._get_source_specific_instructions("API", "Daily", cfg)
        self.assertNotIn("USE EXACTLY THESE", out)


class PromptManagerWiringTests(unittest.TestCase):
    """End-to-end-ish: generate_wrapper_prompt must include column hints when
    they're present on source_config (regression for the bug where Gemini
    silently picked one column out of many)."""

    def setUp(self):
        self.pm = PromptManager()

    def test_full_prompt_carries_column_hints_for_xlsx(self):
        meta = SimpleNamespace(
            name="Despesas em ambiente",
            domain="Environment",
            subdomain="Climate",
            description="…",
            unit="EUR",
            source="INE",
            scale="Regional",
            governance_indicator=False,
            carrying_capacity=None,
            periodicity="Annual",
        )
        cfg = SimpleNamespace(
            location="/tmp/file.xlsx",
            file_id="abc",
            sheet_name="Dados",
            time_column="Anos",
            value_columns=["Gestão de resíduos", "Protecção contra ruídos e vibrações"],
            auth_config={},
        )
        prompt = self.pm.generate_wrapper_prompt(
            indicator_metadata=meta,
            source_config=cfg,
            source_type="XLSX",
            wrapper_id="w-1",
            data_sample="(sample omitted)",
        )
        self.assertIn("Gestão de resíduos", prompt)
        self.assertIn("Protecção contra ruídos e vibrações", prompt)
        self.assertIn("Sheet: Dados", prompt)
        # Multi-column instruction asks Gemini to tag each emitted point
        self.assertIn("series", prompt.lower())


if __name__ == "__main__":
    unittest.main()
