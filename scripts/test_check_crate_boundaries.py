import importlib.util
from pathlib import Path
import sys
import unittest


SCRIPT = Path(__file__).with_name("check_crate_boundaries.py")
SPEC = importlib.util.spec_from_file_location("check_crate_boundaries", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
boundaries = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = boundaries
SPEC.loader.exec_module(boundaries)


class CrateBoundariesTest(unittest.TestCase):
    def test_delivery_test_support_is_allowed_only_for_tests(self):
        dependency = "transferia-test-support"
        self.assertNotIn(dependency, boundaries.PRODUCTION_ALLOWED["transferia-delivery"])
        self.assertIn(dependency, boundaries.DEV_EXTRA["transferia-delivery"])

    def test_connector_pipeline_dependency_is_allowed_only_for_tests(self):
        manifest = {
            "dependencies": {"transferia-pipeline": {"path": "../transferia-pipeline"}},
            "dev-dependencies": {"transferia-pipeline": {"path": "../transferia-pipeline"}},
        }

        production = boundaries.internal_dependencies(manifest, "dependencies")
        development = boundaries.internal_dependencies(manifest, "dev-dependencies")

        self.assertNotEqual(
            production - boundaries.PRODUCTION_ALLOWED["transferia-connectors"],
            set(),
        )
        self.assertEqual(
            development
            - (
                boundaries.PRODUCTION_ALLOWED["transferia-connectors"]
                | boundaries.DEV_EXTRA["transferia-connectors"]
            ),
            set(),
        )

    def test_heavy_dependencies_are_owned_by_isolated_connector_crates(self):
        manifests = {}
        for dependency, owners in boundaries.HEAVY_CONNECTOR_OWNERS.items():
            for owner in owners:
                manifests.setdefault(owner, {"dependencies": {}})["dependencies"][
                    dependency
                ] = {"workspace": True}
        manifests["transferia-connector-support"] = {"dependencies": {}}
        manifests["transferia-connectors"] = {"dependencies": {}}
        manifests["transferia-middleware-datafusion"] = {
            "dependencies": {"datafusion": {"workspace": True}}
        }

        self.assertEqual(boundaries.connector_isolation_errors(manifests), [])

        manifests["transferia-connector-clickhouse"]["dependencies"]["rdkafka"] = {
            "workspace": True
        }
        self.assertIn(
            "transferia-connector-clickhouse: heavy dependency 'rdkafka' belongs only to transferia-connector-kafka",
            boundaries.connector_isolation_errors(manifests),
        )

    def test_datafusion_is_owned_only_by_its_registered_component_crate(self):
        manifests = {
            "transferia-middleware-datafusion": {
                "dependencies": {"datafusion": {"workspace": True}}
            },
            "transferia-delivery": {"dependencies": {}},
        }
        self.assertEqual(boundaries.connector_isolation_errors(manifests), [])

        manifests["transferia-delivery"]["dependencies"]["datafusion"] = {
            "workspace": True
        }
        self.assertIn(
            "transferia-delivery: heavy dependency 'datafusion' belongs only to transferia-middleware-datafusion",
            boundaries.connector_isolation_errors(manifests),
        )

    def test_middleware_crates_cannot_depend_on_each_other(self):
        manifests = {
            "transferia-middleware-datafusion": {
                "dependencies": {
                    "transferia-middleware-filter": {"path": "../filter"}
                }
            },
            "transferia-middleware-filter": {"dependencies": {}},
        }
        self.assertIn(
            "transferia-middleware-datafusion: middleware crates must not depend on siblings: transferia-middleware-filter",
            boundaries.connector_isolation_errors(manifests),
        )

    def test_connector_crates_cannot_depend_on_each_other(self):
        manifests = {
            "transferia-connector-clickhouse": {
                "dependencies": {"transferia-connector-kafka": {"path": "../kafka"}}
            },
            "transferia-connector-kafka": {"dependencies": {}},
            "transferia-connector-support": {"dependencies": {}},
        }

        self.assertIn(
            "transferia-connector-clickhouse: connector crates must not depend on siblings: transferia-connector-kafka",
            boundaries.connector_isolation_errors(manifests),
        )


if __name__ == "__main__":
    unittest.main()
