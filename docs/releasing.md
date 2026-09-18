# Publishing the image and OCI chart

The [release workflow](https://github.com/nakiner/cnpg-connect-plugin/actions/workflows/release.yaml) publishes two packages from a versioned Git tag:

| Artifact | Reference |
| --- | --- |
| Container image | `ghcr.io/nakiner/cnpg-connect-plugin:VERSION` |
| Helm OCI chart | `oci://ghcr.io/nakiner/charts/cnpg-connect-plugin`, version `VERSION` |
| Image platforms | `linux/amd64`, `linux/arm64` |

The source tag has a leading `v`; image tags, chart versions, and chart `appVersion` omit it. The chart selects the corresponding image automatically. Chart and image use separate package paths, so their tags do not collide. No Helm repository index is needed; see [Helm OCI registries](https://helm.sh/docs/v3/topics/registries/).

## Release the simplified connection setup

Automatic Cluster discovery, tokenless defaults, connection metadata, and Gateway h2c support are newer than plugin `v0.0.3`. Installing that old release does not enable them. Publish the plugin changes before publishing a client that requires the new generated protobuf fields:

1. Tag and publish a new plugin version containing the protocol, observer, runtime, and chart changes.
2. Update `cnpgconnect-go` to require that published plugin module version, then release the client.
3. Update application dependencies and deploy the matching plugin/chart configuration.

The API bindings are part of the plugin Go module, at `github.com/nakiner/cnpg-connect-plugin/api/connect/v1`; there is no separate protobuf module to publish. A Go source tag publishes module source, while the Actions workflow publishes the deployable image and OCI chart.

Until publication, [build this checkout's image and local chart](../README.md#run-this-checkout) and build clients against the matching source checkout.

## Repository setup

The workflow uses `GITHUB_TOKEN` with `contents: read` and `packages: write`; no custom registry-password Secret is needed. Repository/organization policy must allow Actions and package publication. If packages already exist outside this repository's ownership, connect them or grant the repository Actions write access in their settings. See [GitHub package workflow authentication](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-in-a-github-actions-workflow).

## Publish a version

Set `CNPG_CONNECT_VERSION` to the semantic version being released, without the leading `v`, then tag the committed source and push:

```sh
git tag -a "v${CNPG_CONNECT_VERSION:?Set the new release version}" \
  -m "Release v${CNPG_CONNECT_VERSION}"
git push origin "v${CNPG_CONNECT_VERSION}"
```

The tagged source must contain the workflow. Prerelease suffixes are supported; `+build` metadata is not. The workflow derives packaged chart/image versions from the tag, so release packaging does not require changing every checked-in version field.

For an existing tag or a failed publication, open [the workflow](https://github.com/nakiner/cnpg-connect-plugin/actions/workflows/release.yaml), choose **Run workflow**, and set its **tag** input. The workflow definition, Dockerfile, and packaging helper come from the selected branch; application and chart source come from the requested tag. The workflow does not move the tag. See [manual workflow runs](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow).

Use a new version for changed source already distributed to users. A rerun republishes that version and does not enforce registry immutability. Image and chart publication is not atomic; a failed run may leave only one artifact available.

## Registry access

Make both packages public for anonymous installation:

- `cnpg-connect-plugin` — image.
- `charts/cnpg-connect-plugin` — chart.

A public repository does not automatically make its GHCR packages public. Configure their visibility on [the Packages page](https://github.com/nakiner?tab=packages); see [GitHub package access settings](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility).

For private packages, authenticate the workstation's Helm client:

```sh
helm registry login ghcr.io --username YOUR_GITHUB_USERNAME
```

Use a personal access token (classic) with `read:packages` and package access at the password prompt. See [GHCR authentication](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-with-a-personal-access-token-classic).

Kubernetes nodes need separate image pull credentials. Create a Secret from your secret manager's Docker-format authentication file, then reference it in Helm values:

```sh
kubectl -n cnpg-system create secret generic ghcr-pull \
  --type=kubernetes.io/dockerconfigjson \
  --from-file=.dockerconfigjson=/secure/path/ghcr-config.json
```

```yaml
imagePullSecrets:
  - name: ghcr-pull
```

Helm login does not create this Kubernetes Secret. Public packages require neither set of registry credentials. Registry credentials are also unrelated to the optional discovery bearer token.

## Verify publication

```sh
helm show chart oci://ghcr.io/nakiner/charts/cnpg-connect-plugin \
  --version "${CNPG_CONNECT_VERSION:?Set the published release version}"
helm show values oci://ghcr.io/nakiner/charts/cnpg-connect-plugin \
  --version "$CNPG_CONNECT_VERSION"
docker buildx imagetools inspect \
  "ghcr.io/nakiner/cnpg-connect-plugin:${CNPG_CONNECT_VERSION}"
```

Chart `version` and `appVersion` should match the chosen version; its Deployment uses the matching image. The image manifest should include both supported architectures. The workflow summary records the tag, commit, image digest, and chart version. Publication does not deploy a cluster; use the [installation guide](../README.md#install-in-kubernetes).

## Validate workflow changes locally

Actionlint uses ShellCheck when available; CI's Ubuntu runner has it. Install [ShellCheck](https://github.com/koalaman/shellcheck#installing), then run:

```sh
shellcheck --version && go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.11
```

Workflow action references use major-version tags (`@v7`, `@v5`, and so on), following this repository's convention.
