#!/usr/bin/env python3
"""Resolve a real Git release tag into safe GitHub Actions outputs."""

import os
import re
import subprocess
from pathlib import Path


# Build metadata is intentionally unsupported: '+' is not a Docker tag character.
TAG_PATTERN = re.compile(
    r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
)


def release_version(tag):
    match = TAG_PATTERN.fullmatch(tag)
    if not match:
        raise ValueError("release tag must be vMAJOR.MINOR.PATCH with optional prerelease, without build metadata")
    for identifier in (match.group(4) or "").split("."):
        if identifier.isdigit() and len(identifier) > 1 and identifier.startswith("0"):
            raise ValueError("numeric prerelease identifiers cannot have leading zeroes")
    version = tag[1:]
    if len(version) > 128:
        raise ValueError("release version exceeds Docker's 128-character tag limit")
    return version


def resolve(tag, repository):
    version = release_version(tag)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]*/[A-Za-z0-9][A-Za-z0-9_.-]*", repository):
        raise ValueError("repository must have the form owner/repository")
    commit = subprocess.check_output(
        ["git", "rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}"],
        text=True,
    ).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("release tag did not resolve to a full Git commit")
    repository = repository.lower()
    owner = repository.split("/", 1)[0]
    return {
        "tag": tag,
        "version": version,
        "commit": commit,
        "image": f"ghcr.io/{repository}",
        "chart_registry": f"ghcr.io/{owner}/charts",
    }


def main():
    metadata = resolve(os.environ["RELEASE_TAG"], os.environ["RELEASE_REPOSITORY"])
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as output:
        for key, value in metadata.items():
            output.write(f"{key}={value}\n")
    print(f"Resolved {metadata['tag']} to {metadata['commit']}")


if __name__ == "__main__":
    main()
