from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from scripts.check_oss_boundary import violations


class OssBoundaryTest(unittest.TestCase):
    def test_rejects_internal_identity(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            internal_host = "service.yandex" + "-team.example"
            path.write_text(f"host: {internal_host}\n", encoding="utf-8")
            self.assertEqual(len(violations([path])), 1)

    def test_accepts_public_repository_urls(self) -> None:
        owner = "timmy" + "b32r"
        with TemporaryDirectory() as directory:
            path = Path(directory) / "README.md"
            for suffix in ("", ".git", "/issues/1", "#readme"):
                with self.subTest(suffix=suffix):
                    path.write_text(f"[repo](https://github.com/{owner}/transferia{suffix})")
                    self.assertEqual(violations([path]), [])

    def test_public_url_does_not_hide_private_paths_or_other_repositories(self) -> None:
        owner = "timmy" + "b32r"
        public_url = f"https://github.com/{owner}/transferia"
        with TemporaryDirectory() as directory:
            path = Path(directory) / "README.md"
            for line in (
                f"{public_url} /Users/{owner}/project",
                f"https://github.com/{owner}/private",
                f"{public_url}-private",
                f"{public_url}.git-private",
            ):
                with self.subTest(line=line):
                    path.write_text(line)
                    self.assertEqual(len(violations([path])), 1)

    def test_accepts_vendor_neutral_example(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            path.write_text("host: service.example.com\n", encoding="utf-8")
            self.assertEqual(violations([path]), [])


if __name__ == "__main__":
    unittest.main()
