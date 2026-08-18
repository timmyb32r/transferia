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

    def test_heavy_provider_dependencies_and_consumers_are_feature_gated(self):
        providers = {
            "dependencies": {
                dependency: {"optional": True}
                for dependency in boundaries.HEAVY_PROVIDER_DEPENDENCIES
            }
        }
        manifests = {
            "transferia-providers": providers,
            "transferia-delivery": {
                "dependencies": {
                    "transferia-providers": {"default-features": False}
                }
            },
            "transferia-control-plane": {
                "dependencies": {
                    "transferia-providers": {
                        "default-features": False,
                        "features": ["provider-logbroker"],
                    }
                }
            },
        }

        self.assertEqual(boundaries.provider_feature_errors(manifests), [])

        providers["dependencies"]["rdkafka"]["optional"] = False
        self.assertIn(
            "transferia-providers: heavy dependency 'rdkafka' must be optional",
            boundaries.provider_feature_errors(manifests),
        )


if __name__ == "__main__":
    unittest.main()
