import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from release_metadata import release_version, resolve


class ReleaseMetadataTests(unittest.TestCase):
    def test_rejects_nonrelease_refs_and_output_injection(self):
        for tag in ("main", "v1", "v01.0.0", "v1.0.0+build", "v1.0.0-rc.01",
                    "v1.0.0\nimage=evil", "v1.0.0^{commit}", "--help", "v1.0.0-" + "x" * 129):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                release_version(tag)

    def test_accepts_release_and_prerelease_versions(self):
        self.assertEqual(release_version("v0.0.1"), "0.0.1")
        self.assertEqual(release_version("v1.2.3-rc.1"), "1.2.3-rc.1")

    def test_resolves_lightweight_and_annotated_tags_without_using_branch_tip(self):
        with tempfile.TemporaryDirectory() as directory:
            def git(*args):
                return subprocess.check_output(["git", "-C", directory, *args], text=True).strip()

            git("init", "--quiet")
            git("config", "user.name", "Release test")
            git("config", "user.email", "test@example.invalid")
            git("config", "commit.gpgsign", "false")
            git("config", "tag.gpgsign", "false")
            git("commit", "--quiet", "--allow-empty", "-m", "tagged source")
            tagged_commit = git("rev-parse", "HEAD")
            git("tag", "v0.0.1")
            git("tag", "-a", "v0.0.2-rc.1", "-m", "annotated release")
            git("commit", "--quiet", "--allow-empty", "-m", "later tooling")
            self.assertNotEqual(tagged_commit, git("rev-parse", "HEAD"))
            previous_directory = Path.cwd()
            try:
                os.chdir(directory)
                for tag in ("v0.0.1", "v0.0.2-rc.1"):
                    with self.subTest(tag=tag):
                        result = resolve(tag, "NaKiNeR/cnpg-connect-plugin")
                        self.assertEqual(result["commit"], tagged_commit)
                        self.assertEqual(result["image"], "ghcr.io/nakiner/cnpg-connect-plugin")
                        self.assertEqual(result["chart_registry"], "ghcr.io/nakiner/charts")
                        self.assertEqual(result["version"], tag[1:])
            finally:
                os.chdir(previous_directory)


if __name__ == "__main__":
    unittest.main()
