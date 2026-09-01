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

    def test_accepts_vendor_neutral_example(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            path.write_text("host: service.example.com\n", encoding="utf-8")
            self.assertEqual(violations([path]), [])


if __name__ == "__main__":
    unittest.main()
