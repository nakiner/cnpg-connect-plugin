#!/usr/bin/env bash
# Helm owns chart packaging; yq only updates a disposable copy of its values.
set -euo pipefail
cd "$(dirname "$0")/.."
export RELEASE_VERSION=${1:?Usage: package-chart.sh VERSION IMAGE DIGEST [DESTINATION]}
export RELEASE_IMAGE=${2:?Image repository is required}
export RELEASE_DIGEST=${3:?Image digest is required}
destination=${4:-work/packages}
[[ $RELEASE_DIGEST =~ ^sha256:[a-f0-9]{64}$ ]] || { echo 'Invalid image digest' >&2; exit 1; }
[[ $RELEASE_VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ && ${#RELEASE_VERSION} -le 122 ]] || exit 1
archive=cnpg-connect-plugin-$RELEASE_VERSION.tgz
[[ ! -e $destination/$archive ]] || { echo "Refusing to overwrite $destination/$archive" >&2; exit 1; }
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
cp -R chart "$scratch/chart"
yq -i '.image.repository = strenv(RELEASE_IMAGE) | .image.tag = strenv(RELEASE_VERSION) | .image.digest = strenv(RELEASE_DIGEST)' "$scratch/chart/values.yaml"
helm package "$scratch/chart" --version "$RELEASE_VERSION" --app-version "$RELEASE_VERSION" --destination "$scratch"
helm lint --strict "$scratch/$archive"
rendered=$(helm template release-check "$scratch/$archive" --namespace cnpg-system)
image=$(yq 'select(.kind == "Deployment") | .spec.template.spec.containers[] | select(.name == "plugin") | .image' <<<"$rendered")
[[ $image == "$RELEASE_IMAGE@$RELEASE_DIGEST" ]]
mkdir -p "$destination"
# noclobber also refuses a competing writer after the initial existence check.
(set -o noclobber; cat "$scratch/$archive" >"$destination/$archive")
