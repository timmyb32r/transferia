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
    def test_provider_pipeline_dependency_is_allowed_only_for_tests(self):
        manifest = {
            "dependencies": {"transferia-pipeline": {"path": "../transferia-pipeline"}},
            "dev-dependencies": {"transferia-pipeline": {"path": "../transferia-pipeline"}},
        }

        production = boundaries.internal_dependencies(manifest, "dependencies")
        development = boundaries.internal_dependencies(manifest, "dev-dependencies")

        self.assertNotEqual(
            production - boundaries.PRODUCTION_ALLOWED["transferia-providers"],
            set(),
        )
        self.assertEqual(
            development
            - (
                boundaries.PRODUCTION_ALLOWED["transferia-providers"]
                | boundaries.DEV_EXTRA["transferia-providers"]
            ),
            set(),
        )

    def test_heavy_dependencies_are_owned_by_isolated_provider_crates(self):
        manifests = {
            owner: {"dependencies": {dependency: {"workspace": True}}}
            for dependency, owner in boundaries.HEAVY_PROVIDER_OWNERS.items()
        }
        manifests["transferia-provider-support"] = {"dependencies": {}}
        manifests["transferia-providers"] = {"dependencies": {}}
        manifests["transferia-middleware-datafusion"] = {
            "dependencies": {"datafusion": {"workspace": True}}
        }

        self.assertEqual(boundaries.provider_isolation_errors(manifests), [])

        manifests["transferia-provider-clickhouse"]["dependencies"]["rdkafka"] = {
            "workspace": True
        }
        self.assertIn(
            "transferia-provider-clickhouse: heavy dependency 'rdkafka' belongs only to transferia-provider-kafka",
            boundaries.provider_isolation_errors(manifests),
        )

    def test_datafusion_is_owned_only_by_its_registered_component_crate(self):
        manifests = {
            "transferia-middleware-datafusion": {
                "dependencies": {"datafusion": {"workspace": True}}
            },
            "transferia-delivery": {"dependencies": {}},
        }
        self.assertEqual(boundaries.provider_isolation_errors(manifests), [])

        manifests["transferia-delivery"]["dependencies"]["datafusion"] = {
            "workspace": True
        }
        self.assertIn(
            "transferia-delivery: heavy dependency 'datafusion' belongs only to transferia-middleware-datafusion",
            boundaries.provider_isolation_errors(manifests),
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
            boundaries.provider_isolation_errors(manifests),
        )

    def test_provider_crates_cannot_depend_on_each_other(self):
        manifests = {
            "transferia-provider-clickhouse": {
                "dependencies": {"transferia-provider-kafka": {"path": "../kafka"}}
            },
            "transferia-provider-kafka": {"dependencies": {}},
            "transferia-provider-support": {"dependencies": {}},
        }

        self.assertIn(
            "transferia-provider-clickhouse: provider crates must not depend on siblings: transferia-provider-kafka",
            boundaries.provider_isolation_errors(manifests),
        )


if __name__ == "__main__":
    unittest.main()
