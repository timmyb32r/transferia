import importlib.util
from pathlib import Path
import sys
import unittest


SCRIPT = Path(__file__).with_name("test_affected.py")
SPEC = importlib.util.spec_from_file_location("test_affected", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
test_affected = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = test_affected
SPEC.loader.exec_module(test_affected)


class AffectedChecksTest(unittest.TestCase):
    def test_frontend_change_runs_only_typescript_typecheck(self):
        rust, web = test_affected.commands(
            test_affected.select(["web/src/delivery/EditorViews.tsx"])
        )

        self.assertEqual(rust, [])
        self.assertEqual(web, [["npm", "run", "typecheck"]])

    def test_connector_change_runs_only_its_cargo_check(self):
        selection = test_affected.select([
            "crates/transferia-connector-clickhouse/src/connectors/clickhouse/sink/client.rs"
        ])
        rust, web = test_affected.commands(selection)

        self.assertEqual(web, [])
        self.assertEqual(selection.integration_tests, set())
        self.assertEqual(
            rust,
            [[
                "cargo",
                "check",
                "--all-targets",
                "--all-features",
                "-p",
                "transferia-connector-clickhouse",
            ]],
        )

    def test_public_crate_surface_includes_transitive_dependents_in_one_check(self):
        rust, _ = test_affected.commands(
            test_affected.select(["crates/transferia-connectors/src/lib.rs"])
        )

        self.assertEqual(len(rust), 2)
        self.assertEqual(rust[0][:4], ["cargo", "check", "--all-targets", "--all-features"])
        self.assertIn("transferia-connectors", rust[0])
        self.assertEqual(rust[1][:5], ["cargo", "check", "--lib", "--bins", "--all-features"])
        self.assertIn("transferia-composition", rust[1])
        self.assertNotIn("transferia-delivery", rust[1])

    def test_middleware_change_never_selects_tests_or_delivery(self):
        selection = test_affected.select([
            "crates/transferia-middleware-datafusion/src/lib.rs"
        ])
        rust, _ = test_affected.commands(selection)

        flattened = " ".join(part for command in rust for part in command)
        self.assertNotIn("test", flattened)
        self.assertNotIn("clippy", flattened)
        self.assertNotIn("fmt", flattened)
        self.assertNotIn("transferia-delivery", flattened)
        self.assertIn("transferia-middleware-datafusion", flattened)
        self.assertIn("transferia-connectors", flattened)

    def test_changed_integration_test_is_checked_but_not_executed(self):
        rust, _ = test_affected.commands(
            test_affected.select(["tests/e2e_postgres.rs"])
        )

        self.assertEqual(
            rust,
            [["cargo", "check", "--all-features", "--test", "e2e_postgres"]],
        )

    def test_root_manifest_checks_root_composition_without_workspace_fallback(self):
        selection = test_affected.select(["Cargo.toml"])
        rust, web = test_affected.commands(selection)

        self.assertFalse(selection.full)
        self.assertEqual(web, [])
        self.assertEqual(
            rust,
            [[
                "cargo",
                "check",
                "--all-targets",
                "--all-features",
                "-p",
                "transferia",
            ]],
        )

    def test_lockfile_alone_runs_nothing(self):
        rust, web = test_affected.commands(test_affected.select(["Cargo.lock"]))
        self.assertEqual((rust, web), ([], []))

    def test_documentation_change_runs_nothing(self):
        rust, web = test_affected.commands(test_affected.select(["docs/server.md"]))
        self.assertEqual((rust, web), ([], []))

    def test_route_manifest_runs_only_api_contract_gate(self):
        selection = test_affected.select([
            "crates/transferia-server-contracts/src/routes.rs"
        ])
        rust, web = test_affected.commands(selection)

        self.assertIn(["just", "api-contract-check"], rust)
        self.assertIn(
            ["npm", "test", "--", "--run", "tests/apiContract.test.ts"], web
        )
        self.assertNotIn(["just", "catalog-contract-check"], rust)

    def test_connector_config_runs_catalog_contract_gate(self):
        selection = test_affected.select([
            "crates/transferia-connector-kafka/src/connectors/kafka/config.rs"
        ])
        rust, web = test_affected.commands(selection)

        self.assertIn(["just", "catalog-contract-check"], rust)
        self.assertNotIn(["just", "api-contract-check"], rust)
        self.assertEqual(web, [])


if __name__ == "__main__":
    unittest.main()
