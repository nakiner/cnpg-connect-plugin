#!/usr/bin/env python3
"""Build a release chart from a source checkout without modifying that checkout.

Requires Helm and PyYAML==6.0.3. In particular, this updates the image defaults in
older tags, whose checked-in chart predates the GHCR release pipeline.
"""

import argparse
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

import yaml


VERSION_PATTERN = re.compile(
    r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
    r"(?:-(?:(?:0|[1-9][0-9]*)|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:(?:0|[1-9][0-9]*)|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*))*)?"
)
IMAGE_COMPONENT = r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*"
IMAGE_PATTERN = re.compile(rf"ghcr\.io/{IMAGE_COMPONENT}(?:/{IMAGE_COMPONENT})+")
REVISION_PATTERN = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")


def validate_release(version, image, revision, source_url):
    if not VERSION_PATTERN.fullmatch(version) or len(version) > 128:
        raise ValueError(
            "version must be SemVer without a v prefix or build metadata, "
            "and fit a Docker tag (at most 128 characters)"
        )
    if not IMAGE_PATTERN.fullmatch(image) or len(image) > 255:
        raise ValueError("image must be a lowercase ghcr.io/owner/repository path without a tag or digest")
    if not REVISION_PATTERN.fullmatch(revision):
        raise ValueError("revision must be a full lowercase Git commit ID (40 or 64 hexadecimal characters)")
    parsed = urlsplit(source_url)
    if (
        parsed.scheme != "https"
        or parsed.netloc != "github.com"
        or not re.fullmatch(r"/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", parsed.path)
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("source-url must be an HTTPS GitHub repository URL without a query or fragment")


def read_mapping(path):
    with path.open(encoding="utf-8") as handle:
        result = yaml.safe_load(handle)
    if not isinstance(result, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return result


def write_mapping(path, value):
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(value, handle, sort_keys=False)


def prepare_chart(source, staged, version, image, revision, source_url):
    validate_release(version, image, revision, source_url)
    # Resolve links while copying so writes always target the temporary directory.
    shutil.copytree(source, staged, symlinks=False)
    metadata_path = staged / "Chart.yaml"
    metadata = read_mapping(metadata_path)
    if metadata.get("name") != "cnpg-connect-plugin" or metadata.get("apiVersion") != "v2":
        raise ValueError("source must be the cnpg-connect-plugin v2 Helm chart")
    metadata.update(version=version, appVersion=version, home=source_url, sources=[source_url])
    annotations = metadata.setdefault("annotations", {})
    if not isinstance(annotations, dict):
        raise ValueError("Chart.yaml annotations must be a mapping")
    annotations.update({
        "org.opencontainers.image.source": source_url,
        "org.opencontainers.image.revision": revision,
        "org.opencontainers.image.version": version,
    })
    write_mapping(metadata_path, metadata)

    values_path = staged / "values.yaml"
    values = read_mapping(values_path)
    image_values = values.setdefault("image", {})
    if not isinstance(image_values, dict):
        raise ValueError("values.yaml image must be a mapping")
    image_values.update(repository=image, tag=version)
    write_mapping(values_path, values)


def package_chart(args):
    validate_release(args.version, args.image, args.revision, args.source_url)
    source = args.source.resolve(strict=True)
    destination = args.destination.resolve()
    if not source.is_dir():
        raise ValueError("source must be a chart directory")
    if destination == source or source in destination.parents:
        raise ValueError("destination must be outside the source chart directory")
    if shutil.which("helm") is None:
        raise ValueError("helm must be installed and available on PATH")

    with tempfile.TemporaryDirectory(prefix="cnpg-connect-chart-") as directory:
        staged = Path(directory) / "cnpg-connect-plugin"
        prepare_chart(source, staged, args.version, args.image, args.revision, args.source_url)
        subprocess.run(["helm", "lint", "--strict", str(staged)], check=True)
        rendered = subprocess.run(
            ["helm", "template", "release-check", str(staged), "--namespace", "cnpg-system",
             "--kube-version", "1.30.0"],
            check=True, capture_output=True, text=True,
        )
        deployments = [
            resource for resource in yaml.safe_load_all(rendered.stdout)
            if isinstance(resource, dict) and resource.get("kind") == "Deployment"
        ]
        images = [
            container.get("image")
            for deployment in deployments
            for container in deployment["spec"]["template"]["spec"]["containers"]
            if container.get("name") == "plugin"
        ]
        expected_image = f"{args.image}:{args.version}"
        if images != [expected_image]:
            raise ValueError(f"rendered plugin image must be {expected_image!r}, got {images!r}")
        destination.mkdir(parents=True, exist_ok=True)
        subprocess.run(["helm", "package", str(staged), "--destination", str(destination)], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path, help="source chart directory")
    parser.add_argument("--version", required=True, help="SemVer version without the v prefix")
    parser.add_argument("--image", required=True, help="GHCR image repository without a tag")
    parser.add_argument("--revision", required=True, help="full Git commit ID of the release")
    parser.add_argument("--source-url", required=True, help="HTTPS GitHub repository URL")
    parser.add_argument("--destination", required=True, type=Path, help="directory for the packaged chart")
    args = parser.parse_args()
    try:
        package_chart(args)
    except (OSError, ValueError, yaml.YAMLError, subprocess.CalledProcessError) as error:
        print(f"error: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            print(error.stderr, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
