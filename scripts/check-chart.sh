#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
helm lint chart
helm template connect chart --namespace cnpg-system >/dev/null
for example in examples/values-*.yaml; do
  helm template connect chart --namespace cnpg-system -f "$example" >/dev/null
done
# Test the deployed production shape, including placement and availability.
rendered=$(helm template connect chart --namespace cnpg-system -f examples/values-production.yaml)
yq -o=json -I=0 . <<<"$rendered" | jq -se '
  (map(select(.kind == "Deployment"))[0].spec) as $deployment |
  $deployment.strategy.rollingUpdate.maxUnavailable == 0 and
  $deployment.template.spec.topologySpreadConstraints[0].minDomains == 2 and
  $deployment.template.spec.topologySpreadConstraints[0].matchLabelKeys == ["pod-template-hash"]' >/dev/null
yq -o=json -I=0 . <<<"$rendered" | jq -se 'any(.[]; .kind == "PodDisruptionBudget" and .spec.minAvailable == 1)' >/dev/null
# CA rotation depends on a metadata watch, including namespace-scoped installs.
for scope in '' databases; do
  helm template connect chart --set "watchNamespace=$scope" | yq -o=json -I=0 . | jq -se '
    any(.[]; (.kind == "Role" or .kind == "ClusterRole") and
      any(.rules[]; .resources == ["secrets"] and (.verbs | sort) == ["get", "list", "watch"]))' >/dev/null
done

for invalid in tls.certManager.enabled=false observer.kubeAPIQPS=0 observer.kubeAPIBurst=0 \
  image.digest=sha256:bad application.limits.maxConcurrentStreams=0 \
  podDisruptionBudget.enabled=true networkPolicy.enabled=true; do
  if helm template connect chart --set "$invalid" >/dev/null 2>&1; then
    echo "Chart unexpectedly accepted $invalid" >&2
    exit 1
  fi
done
# Exercise the same native package path used by release CI.
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
scripts/package-chart.sh 0.0.0-check ghcr.io/nakiner/cnpg-connect-plugin \
  sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa "$scratch"
