# Release checks and failure handling

Every release uses one complete Git tag: application source, Dockerfile, chart,
workflow and scripts. A tag push starts publication; a manual run must select the
same tag in the workflow's ref selector. Branch runs do not publish.

The workflow checks Go code, races and chart rendering, then builds Linux amd64
and arm64 images. `govulncheck` scans the source and both final image binaries.
Those exact images are pushed and combined into a multiarchitecture index.
CI reads the preferred `toolchain` from `go.mod`, rather than building with the
module's older compatibility minimum. Keep that toolchain and the Docker builder
on the same patched Go version when updating them.
The chart is packaged by Helm with its version and appVersion taken from the tag;
its default image is pinned to the index digest. A Helm OCI pull-back verifies
that the published archive matches the local archive.

[Publishing instructions](releasing.md) cover package permissions and commands.
Workflow actions use major-version tags. The Go builder image is digest-pinned;
release tools are versioned in the workflow. Registry checks and vulnerability
results depend on the current remote services. Release notes record the source
commit, workflow run and image digest; the chart archive is attached to the
GitHub release. These are ordinary build records, not signed attestations.

## Existing versions and partial failures

Publication is serialized across versions. Before reserving a version, the
workflow requires a successful authenticated GitHub release listing, including
drafts, and authenticated registry checks for the image, its architecture tags,
and the chart. Only an explicit `404 MANIFEST_UNKNOWN` means absent. Failed
authentication, network errors or ambiguous responses stop the release.

After validation, a new draft GitHub release reserves the version. Existing
releases and drafts are never reused, and asset uploads never use `--clobber`.
A failed publication leaves that draft and any published artifacts in place.
Use a new version after fixing the failure; do not delete the draft or move the
tag to force a retry. A run that failed before reservation can be retried if it
published nothing.

GHCR tag writes are not atomic create-if-absent operations. The guard prevents
accidental replacement by this serialized workflow, but an independent registry
writer can race it. Keep publication permissions limited to the publisher and
protect release tags from changes.

## Local checks

Install Helm, jq, ShellCheck and the Go-based [yq](https://github.com/mikefarah/yq):

```sh
go install github.com/mikefarah/yq/v4@v4.47.2
make check race vuln
shellcheck scripts/*.sh
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.11
```

A protected test release is still needed to verify hosted-runner behavior, GHCR
permissions, first publication, both architecture pushes and GitHub release
transitions. Local checks do not publish anything or establish those permissions.
