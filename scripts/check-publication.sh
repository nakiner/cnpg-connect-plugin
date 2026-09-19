#!/usr/bin/env bash
# Fail closed: only an authenticated, explicit MANIFEST_UNKNOWN proves absence.
set -euo pipefail
: "${GH_TOKEN:?}" "${GITHUB_ACTOR:?}" "${GITHUB_REPOSITORY:?}" "${RELEASE_TAG:?}" "${RELEASE_IMAGE:?}" "${RELEASE_CHART:?}"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
gh api --paginate "repos/$GITHUB_REPOSITORY/releases?per_page=100" >"$scratch/releases.json"
jq -se --arg tag "$RELEASE_TAG" '
  length > 0 and all(.[]; type == "array") and
  all(.[][]; (.tag_name | type == "string") and .tag_name != $tag)' "$scratch/releases.json" >/dev/null
version=${RELEASE_TAG#v}
for reference in "$RELEASE_IMAGE:$version" "$RELEASE_IMAGE:$version-amd64" "$RELEASE_IMAGE:$version-arm64" "$RELEASE_CHART:$version"; do
  repository=${reference%:*}
  repository=${repository#ghcr.io/}
  tag=${reference##*:}
  token=$(curl --fail --silent --show-error --max-time 30 --user "$GITHUB_ACTOR:$GH_TOKEN" --get \
    --data-urlencode service=ghcr.io --data-urlencode "scope=repository:$repository:pull" https://ghcr.io/token \
    | jq -er '.token | select(type == "string" and length > 0)')
  status=$(curl --silent --show-error --max-time 30 --output "$scratch/manifest.json" --write-out '%{http_code}' \
    --header "Authorization: Bearer $token" \
    --header 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
    "https://ghcr.io/v2/$repository/manifests/$tag")
  if [[ $status != 404 ]] || ! jq -e '.errors | type == "array" and length > 0 and all(.[]; .code == "MANIFEST_UNKNOWN")' "$scratch/manifest.json" >/dev/null; then
    echo "Refusing publication: $reference exists or its absence is unproven (HTTP $status)" >&2
    exit 1
  fi
done
