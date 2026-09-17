import tempfile
import unittest
from pathlib import Path

import yaml

from package_chart import prepare_chart, validate_release


IMAGE = "ghcr.io/nakiner/cnpg-connect-plugin"
REVISION = "1234567890abcdef" * 2 + "12345678"
SOURCE_URL = "https://github.com/nakiner/cnpg-connect-plugin"


class ReleaseValidationTests(unittest.TestCase):
    def test_semver_releases_and_prereleases(self):
        for version in ("0.0.1", "1.20.300", "1.0.0-rc.1", "1.0.0-0", "1.0.0-01a"):
            with self.subTest(version=version):
                validate_release(version, IMAGE, REVISION, SOURCE_URL)

    def test_rejects_ambiguous_or_unsupported_versions(self):
        for version in (
            "v0.0.1", "1.0", "01.0.0", "1.0.0-01", "1.0.0-rc.01", "1.0.0-",
            "1.0.0+build.1", "1.0.0\n", "1.0.0-" + "a" * 123, "../1.0.0",
        ):
            with self.subTest(version=version), self.assertRaises(ValueError):
                validate_release(version, IMAGE, REVISION, SOURCE_URL)

    def test_rejects_bad_image_revision_and_source(self):
        cases = (
            ("ghcr.io/Nakiner/plugin", REVISION, SOURCE_URL),
            (IMAGE + ":latest", REVISION, SOURCE_URL),
            ("docker.io/nakiner/plugin", REVISION, SOURCE_URL),
            (IMAGE, "abcdef0", SOURCE_URL),
            (IMAGE, REVISION, "https://github.com/nakiner/cnpg-connect-plugin?x=1"),
            (IMAGE, REVISION, "https://github.com.evil.example/nakiner/plugin"),
        )
        for image, revision, source_url in cases:
            with self.subTest(image=image, revision=revision, source_url=source_url):
                with self.assertRaises(ValueError):
                    validate_release("0.0.1", image, revision, source_url)

    def test_packaging_older_tag_replaces_both_versions_and_image_without_editing_source(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            source.mkdir()
            (source / "Chart.yaml").write_text(
                "apiVersion: v2\nname: cnpg-connect-plugin\nversion: 0.1.0\nappVersion: 0.1.0-dev\n",
                encoding="utf-8",
            )
            (source / "values.yaml").write_text(
                'image:\n  repository: cnpg-connect-plugin\n  tag: "0.1.0-dev"\nreplicaCount: 2\n',
                encoding="utf-8",
            )
            original = {path.name: path.read_bytes() for path in source.iterdir()}
            staged = Path(directory) / "staged"
            prepare_chart(source, staged, "0.0.1", IMAGE, REVISION, SOURCE_URL)
            metadata = yaml.safe_load((staged / "Chart.yaml").read_text(encoding="utf-8"))
            values = yaml.safe_load((staged / "values.yaml").read_text(encoding="utf-8"))
            self.assertEqual(metadata["version"], "0.0.1")
            self.assertEqual(metadata["appVersion"], "0.0.1")
            self.assertEqual(metadata["annotations"]["org.opencontainers.image.revision"], REVISION)
            self.assertEqual(metadata["annotations"]["org.opencontainers.image.source"], SOURCE_URL)
            self.assertEqual(values["image"], {"repository": IMAGE, "tag": "0.0.1"})
            self.assertEqual(values["replicaCount"], 2)
            self.assertEqual({path.name: path.read_bytes() for path in source.iterdir()}, original)


if __name__ == "__main__":
    unittest.main()
