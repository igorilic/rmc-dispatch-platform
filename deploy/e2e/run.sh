#!/usr/bin/env bash
# make e2e (SPEC §12): the cloud chart and the three example customers on a fresh kind cluster,
# then the demo scenario, then checks that a plan exists, that rheinbau's CronJob ingests, and
# that the DLQ is empty. CI runs it on every push; locally it needs kind (brew install kind).
# It writes its kubeconfig under target/, so ~/.kube/config is never touched.
#   E2E_KEEP=1    keeps the cluster afterwards (delete it with: kind delete cluster --name pourline-e2e)
#   SKIP_BUILD=1  reuses the pourline/<service>:e2e images built last time
set -euo pipefail
cd "$(dirname "$0")/../.."

CLUSTER=pourline-e2e
# The default node image of kind v0.33.0.
NODE_IMAGE=kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
SERVICES=(ingest dispatch simulators demo operator)
NS=pourline
mkdir -p target/e2e
export KUBECONFIG=$PWD/target/e2e/kubeconfig

for tool in docker kind kubectl helm jq; do
  command -v "$tool" >/dev/null || {
    echo "make e2e needs $tool. On macOS: brew install kind kubectl helm jq (and Docker)."
    exit 1
  }
done

# What went wrong, if something did.
diagnose() {
  echo "--- the e2e test failed; the cluster's state:"
  kubectl -n "$NS" get customerintegrations,deployments,cronjobs,jobs,pods -o wide || true
  kubectl -n "$NS" get events --sort-by=.lastTimestamp | tail -30 || true
  for deployment in operator ingest dispatch ingest-bauwerk-ag; do
    echo "--- logs of $deployment"
    kubectl -n "$NS" logs "deploy/$deployment" --tail=40 || true
  done
  # Every running pod that restarted or isn't ready: why its container last stopped, and its log
  # from before that, which is where a crash says what went wrong.
  local pods
  pods=$(kubectl -n "$NS" get pods -o json | jq -r '.items[] | select(.status.phase != "Succeeded")
    | select(any(.status.containerStatuses[]?; (.ready | not) or .restartCount > 0)) | .metadata.name') || true
  for pod in $pods; do
    echo "--- $pod"
    kubectl -n "$NS" get pod "$pod" -o json |
      jq -c '.status.containerStatuses[] | {name, restartCount, state, lastState}' || true
    kubectl -n "$NS" logs "$pod" --previous --tail=40 2>/dev/null ||
      kubectl -n "$NS" logs "$pod" --tail=40 || true
    see_inside "$pod"
  done
}
# What a crashing pod's container sees: a copy of the pod runs a shell instead of its command and
# lists its mounts and reads their files, so an unreadable file shows its error. It needs a shell
# in the image, so it shows nothing for the distroless Pourline images.
see_inside() {
  local container mounts
  container=$(kubectl -n "$NS" get pod "$1" -o jsonpath='{.spec.containers[0].name}')
  # Not the service account's token.
  mounts=$(kubectl -n "$NS" get pod "$1" -o jsonpath='{.spec.containers[0].volumeMounts[*].mountPath}' |
    tr ' ' '\n' | grep -v '^/var/run/secrets' | tr '\n' ' ')
  kubectl -n "$NS" debug "$1" --copy-to="$1-inside" --container="$container" --attach=false \
    --quiet -- sh -c "id; for m in $mounts; do echo \"== \$m\"; ls -la \"\$m\"; \
      for f in \"\$m\"/*; do [ -f \"\$f\" ] && { echo \"-- \$f\"; head -c 300 \"\$f\"; echo; }; done; done" \
    >/dev/null 2>&1 || return 0
  kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$1-inside" --timeout=60s \
    >/dev/null 2>&1 || true
  echo "--- inside $1"
  kubectl -n "$NS" logs "$1-inside" -c "$container" 2>&1 || true
}
finish() {
  local status=$?
  [ "$status" = 0 ] || diagnose
  if [ "${E2E_KEEP:-}" != 1 ]; then kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; fi
  exit "$status"
}
trap finish EXIT

kind get clusters 2>/dev/null | grep -qx "$CLUSTER" ||
  kind create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --kubeconfig "$KUBECONFIG" --wait 120s
helm lint deploy/helm/pourline deploy/helm/plant-agent

if [ "${SKIP_BUILD:-}" != 1 ]; then
  for service in "${SERVICES[@]}"; do
    docker build --build-arg BIN="$service" -t "pourline/$service:e2e" .
  done
fi
for service in "${SERVICES[@]}"; do
  kind load docker-image --name "$CLUSTER" "pourline/$service:e2e"
done

echo "--- install"
# The CRD first and by this script, as make k8s does, because Helm never upgrades crds/.
kubectl apply --server-side --force-conflicts -f deploy/helm/pourline/crds/
helm upgrade --install pourline deploy/helm/pourline --skip-crds --namespace "$NS" --create-namespace \
  -f deploy/e2e/values.yaml --wait --timeout 5m
kubectl kustomize --load-restrictor LoadRestrictionsNone deploy/integrations | kubectl apply -f -
# Poll every minute, so the test sees a CronJob run; it also checks that a spec change rolls out.
kubectl -n "$NS" patch customerintegration rheinbau-gmbh --type merge \
  -p '{"spec":{"schedule":"* * * * *"}}'
kubectl -n "$NS" wait customerintegration --all \
  --for=jsonpath='{.status.phase}'=Running --timeout=180s
kubectl -n "$NS" get customerintegrations

echo "--- the demo scenario"
kubectl -n "$NS" run demo --rm -i --restart=Never --quiet \
  --image pourline/demo:e2e --image-pull-policy=Never \
  --env DISPATCH_URL=http://dispatch:8081 --env MQTT_URL=mqtt://mosquitto:1883 \
  --env BAUWERK_WEBHOOK_URL=http://ingest-bauwerk-ag:8080/webhooks/bauwerk-ag \
  --env NECKAR_WEBHOOK_URL=http://ingest-neckar-infra:8080/webhooks/neckar-infra \
  --env DEMO_TIMEOUT_SECS=180

echo "--- checks"
# Within two CronJob runs and one status refresh.
deadline=$((SECONDS + 240))
until [ -n "$(kubectl -n "$NS" get customerintegration rheinbau-gmbh \
  -o jsonpath='{.status.lastSuccessfulIngest}')" ]; do
  [ $SECONDS -lt $deadline ] || { echo "rheinbau-gmbh's CronJob ingested nothing within 4 minutes"; exit 1; }
  sleep 5
done
echo "✓ rheinbau-gmbh's CronJob ingested a drop"

# Through the API server's service proxy, as make chaos reads the plant-agent.
proxy() { kubectl get --raw "/api/v1/namespaces/$NS/services/$1/proxy/$2"; }
loads=$(proxy dispatch:8081 plans/latest | jq '.loads | length')
[ "$loads" -gt 0 ] || { echo "the latest plan has no loads"; exit 1; }
echo "✓ the latest plan has $loads loads"
dead_letters=$(proxy nats:monitoring 'jsz?streams=true' |
  jq '[.account_details[].stream_detail[] | select(.name == "DLQ") | .state.messages] | add // 0')
[ "$dead_letters" = 0 ] || { echo "the DLQ holds $dead_letters messages"; exit 1; }
reported=$(kubectl -n "$NS" get customerintegrations \
  -o jsonpath='{range .items[*]}{.metadata.name}={.status.deadLetterCount} {end}')
grep -qv '=[1-9]' <<<"$reported" || { echo "dead letters reported: $reported"; exit 1; }
echo "✓ the DLQ is empty, and every integration reports 0 dead letters"
