# Publishing the image and OCI chart

The [release GitHub Actions workflow](https://github.com/nakiner/cnpg-connect-plugin/actions/workflows/release.yaml) publishes two versioned packages from one Git tag:

| Artifact | For source tag `v0.0.1` |
| --- | --- |
| Multi-architecture image | `ghcr.io/nakiner/cnpg-connect-plugin:0.0.1` |
| Helm OCI chart | `oci://ghcr.io/nakiner/charts/cnpg-connect-plugin`, version `0.0.1` |
| Chart application version | `0.0.1` |
| Image platforms | `linux/amd64`, `linux/arm64` |

The leading `v` belongs to the source tag only. The workflow checks out that exact tagged source, derives chart `version` and `appVersion` from the tag, and packages the chart with the corresponding GHCR image defaults. This also allows the existing `v0.0.1` tag to be released even though its checked-in chart used earlier development defaults. It does not move the Git tag or change its source tree.

The image and chart use different package paths so their OCI tags do not collide. Installation uses the full chart path; `helm repo add` and a GitHub Pages chart index are unnecessary. See [Helm's OCI registry documentation](https://helm.sh/docs/v3/topics/registries/).

## Repository setup

1. Commit and push the release workflow, packaging helper, and related changes to the repository's default branch. Manual dispatch must be available from that branch.
2. Ensure Actions and GitHub Packages publishing are allowed by repository/organization policy.
3. The workflow requests `contents: read` and `packages: write` and authenticates using the repository's `GITHUB_TOKEN`. No custom PAT or registry-password repository secret is needed for publishing.
4. If either package already exists and is not associated with this repository, connect it or grant this repository Actions write access in the package settings before publishing.

GitHub documents [workflow token authentication and package access](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-in-a-github-actions-workflow). A workflow cannot override an organization policy that forbids publication.

## Publish v0.0.1

For this initial release, you can recreate `v0.0.1` at the commit containing the release workflow and packaging changes, then push that tag. The tag event starts publication automatically. Commit the changes before retagging so the tagged source contains the workflow.

Alternatively, keep the existing tag unchanged and publish it manually. Merely pushing an unchanged existing tag does not create another tag-push event, and a workflow added afterward is not retroactively triggered. After the workflow is on the default branch:

1. Open the [release workflow](https://github.com/nakiner/cnpg-connect-plugin/actions/workflows/release.yaml).
2. Select **Run workflow**, use the default branch containing this workflow, and set the **tag** input to `v0.0.1`.
3. Run it and wait for the image and chart publication steps to succeed.
4. Check both package versions under the repository/account's **Packages** page.

With manual dispatch, the workflow definition, Dockerfile, and chart-packaging helper come from the selected default branch; the application and chart source come from the requested tag. The build injects the release version into the binary, including when publishing a tag that predates version injection. See [GitHub's manual workflow instructions](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow).

A published Git source tag by itself does not mean these packages exist. The OCI installation commands work once publication has completed and the caller has package access.

## Make installation public, or configure private access

GHCR packages are private when first published. Change the visibility of **both** packages to public for anonymous chart downloads and container pulls:

- `cnpg-connect-plugin` — the application image.
- `charts/cnpg-connect-plugin` — the Helm chart.

A public GitHub repository does not automatically make these packages public. Find both on [nakiner's Packages page](https://github.com/nakiner?tab=packages), open each package's settings, and review its access/visibility configuration. See [GitHub's package visibility instructions](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility).

If packages should remain private, installers need Helm registry credentials for the chart and Kubernetes `imagePullSecrets` for the image. The [README private-registry instructions](../README.md#1-check-the-release-and-registry-access) describe both. Making the chart public alone does not allow a cluster to pull a private image.

## Verify publication

With package access configured, inspect the chart and its default image selection:

```sh
helm show chart oci://ghcr.io/nakiner/charts/cnpg-connect-plugin --version 0.0.1
helm show values oci://ghcr.io/nakiner/charts/cnpg-connect-plugin --version 0.0.1
helm template connect oci://ghcr.io/nakiner/charts/cnpg-connect-plugin \
  --version 0.0.1 --namespace cnpg-system
```

Expect chart `version` and `appVersion` to be `0.0.1`, and the rendered Deployment image to be `ghcr.io/nakiner/cnpg-connect-plugin:0.0.1` without a consumer image override.

For a maintainer with Docker/buildx installed, inspect the image manifest:

```sh
docker buildx imagetools inspect ghcr.io/nakiner/cnpg-connect-plugin:0.0.1
```

Confirm that it includes `linux/amd64` and `linux/arm64`. The workflow summary records the source tag/commit, image reference/digest, and chart reference/version. Package publication does not deploy the plugin or establish Kubernetes runtime behavior; follow the [installation and verification guide](../README.md#install-in-kubernetes).

## Subsequent releases

Choose a new semantic version and push a new `v`-prefixed tag from the commit being released. Prerelease suffixes are supported; `+build` metadata is not supported by this workflow. For example, once `0.0.2` is ready:

```sh
git tag -a v0.0.2 -m 'Release v0.0.2'
git push origin v0.0.2
```

The tagged commit must contain the release workflow for automatic tag-push publication. The workflow derives chart/image versions from the tag; manually editing all version fields is not required for packaging. New release tags should remain fixed. Use a new version for source changes and keep installations pinned to the desired chart `--version`.

The workflow can also be dispatched with an existing release tag for initial publication or recovery of a failed run. A rerun rebuilds and republishes that version; the workflow does not enforce registry tag immutability. Use a new version for changes to an already distributed release. Inspect the failed step and any already-published packages before retrying. Publishing the image and chart is not a single atomic registry operation, so a failed run can leave only one artifact available.

## Validate workflow changes locally

Install [ShellCheck](https://github.com/koalaman/shellcheck#installing) before running the workflow checks. Actionlint [skips ShellCheck when it cannot find it](https://github.com/rhysd/actionlint/blob/main/docs/checks.md#shellcheck-integration-for-run), so an actionlint-only pass can miss diagnostics reported by GitHub's Ubuntu runner. Use the same prerequisite check as CI:

```sh
shellcheck --version && go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.11
```
