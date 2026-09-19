#!/usr/bin/env bash
# One disposable fixture; the assertions and recovery measurements live in Go.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd -P)
library=$(cd "${1:?Usage: run-isolated.sh LIBRARY [OUTPUT] [REPETITIONS]}" && pwd -P)
output=${2:-"$root/work/isolated-results"}
repetitions=${3:-1}
[[ $repetitions =~ ^([1-9]|1[0-9]|20)$ ]] || { echo 'Repetitions must be 1-20' >&2; exit 1; }
[[ -f $library/test/integration/lifecycle_test.go ]]
for tool in kind kubectl helm docker go jq openssl; do command -v "$tool" >/dev/null; done
[[ ! -e $output ]] || { echo "Choose a new output directory: $output" >&2; exit 1; }
mkdir -p "$output"
output=$(cd "$output" && pwd -P)

cluster=cnpg-connect-test
context=kind-$cluster
clusters=$(kind get clusters)
if grep -qx "$cluster" <<<"$clusters"; then
  echo "Refusing to reuse or delete existing $context" >&2
  exit 1
fi
scratch=$(mktemp -d)
scratch=$(cd "$scratch" && pwd -P)
export KUBECONFIG=$scratch/kubeconfig
owner=/cnpg-connect-owner-$(openssl rand -hex 16)
touch "$scratch/owner"

cleanup() {
  status=$?
  trap - EXIT
  trap '' INT TERM
  # A bind mount unique to this invocation proves ownership even if kind failed
  # partway through creation. Delete the immutable container ID, never its name.
  node=$(docker ps -aq --no-trunc --filter "name=^/${cluster}-control-plane$") || status=1
  if [[ -n $node ]]; then
    if docker inspect "$node" | jq -e --arg source "$scratch/owner" --arg target "$owner" \
      'length == 1 and any(.[0].Mounts[]; .Source == $source and .Destination == $target and .RW == false)' >/dev/null; then
      kubectl --context "$context" --request-timeout=10s get pods -A -o wide >"$output/pods.log" 2>&1 || true
      kubectl --context "$context" --request-timeout=10s get events -A >"$output/events.log" 2>&1 || true
      docker rm -fv "$node" || status=1
    else
      echo 'Node ownership changed; refusing cleanup' >&2
      status=1
    fi
  fi
  rm -rf "$scratch"
  if [[ $status == 0 ]]; then echo passed >"$output/result"; else echo failed >"$output/result"; fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf 'running\n' >"$output/result"
cat >"$scratch/kind.yaml" <<YAML
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    image: kindest/node:v1.34.0@sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a
    extraMounts:
      - hostPath: "$scratch/owner"
        containerPath: "$owner"
        readOnly: true
    extraPortMappings:
      - {containerPort: 30443, hostPort: 7443, listenAddress: "127.0.0.1"}
      - {containerPort: 30541, hostPort: 7541, listenAddress: "127.0.0.1"}
      - {containerPort: 30542, hostPort: 7542, listenAddress: "127.0.0.1"}
      - {containerPort: 30543, hostPort: 7543, listenAddress: "127.0.0.1"}
YAML
kind create cluster --name "$cluster" --kubeconfig "$KUBECONFIG" --config "$scratch/kind.yaml" --wait 180s
[[ $(kubectl config current-context) == "$context" ]]
kubectl() { command kubectl --kubeconfig "$KUBECONFIG" --context "$context" "$@"; }
# Optional standard kind archive preload; pulling the pinned fixture image is
# otherwise left to Kubernetes. No custom registry or archive implementation.
if [[ -n ${POSTGRES_ARCHIVE:-} ]]; then kind load image-archive "$POSTGRES_ARCHIVE" --name "$cluster"; fi
kubectl apply --server-side -f https://github.com/cert-manager/cert-manager/releases/download/v1.20.3/cert-manager.yaml
kubectl apply --server-side -f https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/v1.30.0/releases/cnpg-1.30.0.yaml
for deployment in cert-manager cert-manager-webhook cert-manager-cainjector; do
  kubectl rollout status "deployment/$deployment" -n cert-manager --timeout=240s
done
kubectl rollout status deployment/cnpg-controller-manager -n cnpg-system --timeout=240s

docker build --build-arg VERSION=isolated-test -t cnpg-connect-plugin:isolated-test "$root"
kind load docker-image cnpg-connect-plugin:isolated-test --name "$cluster"
printf '%s' "$(openssl rand -hex 32)" >"$scratch/token"
kubectl create secret generic cnpg-connect-auth -n cnpg-system --from-file=token="$scratch/token"
helm install connect "$root/chart" --namespace cnpg-system --kubeconfig "$KUBECONFIG" --kube-context "$context" \
  --set fullnameOverride=connect --set replicaCount=2 \
  --set image.repository=cnpg-connect-plugin,image.tag=isolated-test,image.digest=,image.pullPolicy=Never \
  --set application.auth.existingSecret=cnpg-connect-auth --wait --timeout=300s
kubectl apply -f "$root/scripts/fixtures.yaml"
kubectl wait cluster/app-db -n databases --for=condition=Ready --timeout=600s

export GOWORK=$scratch/go.work
(cd "$scratch" && GOWORK=off go work init "$root" "$library")
export CNPG_CONNECT_E2E_KUBECONFIG=$KUBECONFIG
export CNPG_CONNECT_E2E_ENDPOINT=127.0.0.1:7443 CNPG_CONNECT_E2E_SERVER_NAME=connect-api.cnpg-system.svc
export CNPG_CONNECT_E2E_TOKEN_SECRET=cnpg-connect-auth CNPG_CONNECT_E2E_REPLACE_STANDBY=1
export CNPG_CONNECT_E2E_ANNOTATION_OUTAGE=1 CNPG_CONNECT_E2E_TLS_ROTATION=1
export CNPGCONNECT_GO_E2E_KUBECONFIG=$KUBECONFIG CNPGCONNECT_GO_E2E_FAILOVER=1
export CNPGCONNECT_GO_E2E_DISCOVERY_OUTAGE=1 CNPGCONNECT_GO_E2E_STALL=1
export CNPGCONNECT_GO_E2E_RECOVERY_SLO=${CNPGCONNECT_GO_E2E_RECOVERY_SLO:-60s}

suite() {
  name=$1 checkout=$2 tag=$3 package=$4
  shift 4
  tests=$(IFS='|'; echo "$*")
  echo "Running $name"
  (cd "$checkout" && go test -json -count=1 -timeout=15m -tags="$tag" -run "^($tests)$" "$package") \
    | tee "$output/$name.jsonl"
  # A green Go exit alone allows skipped/missing tests. Require every named test
  # to pass, and reject skips (including subtests).
  jq -se 'all(.[]; .Action != "skip")' "$output/$name.jsonl" >/dev/null
  for test in "$@"; do
    jq -se --arg test "$test" 'any(.[]; .Action == "pass" and .Test == $test)' "$output/$name.jsonl" >/dev/null
  done
}
suite plugin-lifecycle "$root" e2e ./test/e2e TestLiveTopologyLifecycle TestLiveTopologySmoke
suite certificate-rotation "$root" e2e ./test/e2e TestLiveDiscoveryCertificateRotation TestLiveCABundleUpdate
suite plugin-outage "$root" e2e ./test/e2e TestLiveAnnotationOutage TestLiveTopologySmoke
for ((iteration=1; iteration<=repetitions; iteration++)); do
  suite "client-lifecycle-$iteration" "$library" integration ./test/integration TestLiveLifecycle
done
suite client-outage "$library" integration ./test/integration TestLiveDiscoveryOutage
suite client-stall "$library" integration ./test/integration TestLiveDiscoveryStall
pods=$(kubectl get pods -n cnpg-system -l app.kubernetes.io/instance=connect -o json)
jq -e '.items | length == 2' <<<"$pods" >/dev/null
while read -r pod; do
  kubectl get --raw "/api/v1/namespaces/cnpg-system/pods/$pod:8081/proxy/metrics" >"$output/$pod.prom"
  grep -q '^cnpg_connect_' "$output/$pod.prom"
done < <(jq -r '.items[].metadata.name' <<<"$pods")
echo "All live suites passed. Logs and measurements: $output"
