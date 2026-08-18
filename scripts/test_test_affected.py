import importlib.util
from pathlib import Path
import subprocess
import sys
import unittest


SCRIPT = Path(__file__).with_name("test_affected.py")
SPEC = importlib.util.spec_from_file_location("test_affected", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
test_affected = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = test_affected
SPEC.loader.exec_module(test_affected)


class AffectedTestsTest(unittest.TestCase):
    def test_frontend_change_uses_vitest_dependency_graph_and_build(self):
        selection = test_affected.select(["web/src/delivery/EditorViews.tsx"])
        rust, web = test_affected.commands(selection)

        self.assertEqual(rust, [])
        self.assertEqual(web[0][:4], ["npx", "--no-install", "vitest", "related"])
        self.assertEqual(web[1], ["npm", "run", "build"])

    def test_provider_change_selects_unit_and_owned_e2e_tests(self):
        selection = test_affected.select(["src/providers/clickhouse/sink/client.rs"])
        rust, web = test_affected.commands(selection)

        self.assertEqual(web, [])
        self.assertEqual(len(rust), 2)
        self.assertIn("--lib", rust[0])
        self.assertIn("providers::clickhouse::", rust[0])
        self.assertNotIn("e2e_clickhouse_source", rust[1])
        self.assertIn("e2e_sinks", rust[1])

    def test_changed_integration_test_runs_only_that_target(self):
        selection = test_affected.select(["tests/e2e_postgres.rs"])
        rust, _ = test_affected.commands(selection)

        self.assertEqual(
            rust,
            [["cargo", "test", "--all-features", "--test", "e2e_postgres"]],
        )

    def test_top_level_parser_file_uses_a_real_module_filter(self):
        selection = test_affected.select(["src/parsers/detection.rs"])
        rust, _ = test_affected.commands(selection)

        self.assertIn("parsers::tests::detection::", rust[0])
        self.assertEqual(len(rust), 1)

    def test_unknown_build_input_falls_back_to_full_suite(self):
        selection = test_affected.select(["Cargo.toml"])
        rust, _ = test_affected.commands(selection)

        self.assertTrue(selection.full)
        self.assertEqual(
            rust,
            [["cargo", "test", "--workspace", "--all-targets", "--all-features"]],
        )

    def test_shared_core_contract_falls_back_to_full_suite(self):
        selection = test_affected.select(["crates/transferia-core/src/source.rs"])

        self.assertTrue(selection.full)

    def test_api_generator_preserves_unchanged_output_timestamp(self):
        generator = test_affected.ROOT / "web/scripts/generate-api.mjs"
        output = test_affected.ROOT / "web/src/generated/apiContract.ts"
        subprocess.run(["node", generator], cwd=generator.parents[1], check=True)
        before = output.stat().st_mtime_ns

        subprocess.run(["node", generator], cwd=generator.parents[1], check=True)

        self.assertEqual(output.stat().st_mtime_ns, before)


if __name__ == "__main__":
    unittest.main()
