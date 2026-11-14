#!/usr/bin/env bash
# make k8s (SPEC §9): the Docker network pourline-net, a local registry, the k3d clusters `cloud`
# and `plant-walldorf`, Pourline's images, the Helm charts in deploy/helm, and the example
# CustomerIntegrations in deploy/integrations. It creates only what is missing, so it is safe to
# run again: it then rebuilds the images, and the services whose image changed roll out.
# SKIP_BUILD=1 reuses the images built last, for a change to the charts only.
set -euo pipefail
cd "$(dirname "$0")/../.."

NETWORK=pourline-net
SUBNET=172.28.0.0/24 # fixed, so make chaos can reconnect the plant node with its old IP
REGISTRY=pourline-registry
SERVICES=(ingest dispatch simulators demo plant-agent operator console)

for tool in docker k3d kubectl helm; do
  command -v "$tool" >/dev/null || {
    echo "make k8s needs $tool. On macOS: brew install k3d kubectl helm (and Docker). Check: k3d version && kubectl version --client && helm version"
    exit 1
  }
done
if ! k3d cluster get cloud >/dev/null 2>&1 && (exec 3<>/dev/tcp/127.0.0.1/8080) 2>/dev/null; then
  echo "localhost:8080 is taken, and the cloud cluster's ingress needs it. Is the compose stack up? Run make down first."
  exit 1
fi

docker network inspect "$NETWORK" >/dev/null 2>&1 || docker network create --subnet "$SUBNET" "$NETWORK"
k3d registry get "$REGISTRY" >/dev/null 2>&1 ||
  k3d registry create "$REGISTRY" --port 127.0.0.1:5050 --default-network "$NETWORK" --image registry:3.1.2
k3d cluster get cloud >/dev/null 2>&1 || k3d cluster create --config deploy/k3d/cloud.yaml --wait
cloud_ip=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NETWORK\").IPAddress}}" k3d-cloud-server-0)
k3d cluster get plant-walldorf >/dev/null 2>&1 ||
  k3d cluster create --config deploy/k3d/plant-walldorf.yaml --host-alias "$cloud_ip:cloud" --wait
# The plant node's own address: the console and Prometheus read the agent there, past the uplink.
plant_ip=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NETWORK\").IPAddress}}" k3d-plant-walldorf-server-0)

# Pushed even with SKIP_BUILD=1: make down deletes the registry, and a fresh one is empty.
for service in "${SERVICES[@]}"; do
  [ "${SKIP_BUILD:-}" = 1 ] || docker build --build-arg BIN="$service" -t "localhost:5050/$service:dev" .
  docker push "localhost:5050/$service:dev"
done

# Each image is deployed by the digest just pushed. A pod then runs exactly that build, a new build
# rolls out by itself, and a plant pod that restarts while its cluster is cut off starts from the
# image cached on its node.
image() {
  local digest
  digest=$(docker inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "localhost:5050/$1:dev" |
    sed -n "s#^localhost:5050/$1@##p" | head -1)
  [ -n "$digest" ] || { echo "localhost:5050/$1:dev was never pushed: run make k8s without SKIP_BUILD" >&2; exit 1; }
  echo "k3d-pourline-registry:5050/$1@$digest"
}
images=()
for service in ingest dispatch simulators operator console; do
  ref=$(image "$service") # a plain assignment, so a missing image stops the script
  images+=(--set "images.$service=$ref")
done
plant_image=$(image plant-agent)

# M7 applied plain manifests, and Helm won't take over objects it didn't create.
for cluster in cloud plant-walldorf; do
  if kubectl --context "k3d-$cluster" -n pourline get deployments -o name 2>/dev/null | grep -q . &&
    ! kubectl --context "k3d-$cluster" -n pourline get deployments \
      -o jsonpath='{.items[*].metadata.annotations.meta\.helm\.sh/release-name}' | grep -q .; then
    echo "The $cluster cluster still runs the plain manifests from before the Helm charts. Run make down once, then make k8s."
    exit 1
  fi
done

# Helm installs crds/ once and never upgrades it, so this script owns the CRD: it applies it first,
# server-side, and Helm skips it. A plain `helm install` of the chart still installs it.
kubectl --context k3d-cloud apply --server-side --force-conflicts -f deploy/helm/pourline/crds/
helm upgrade --install pourline deploy/helm/pourline --skip-crds --kube-context k3d-cloud \
  --namespace pourline --create-namespace --wait --timeout 5m \
  "${images[@]}" --set simulators.agentPlants=plant-walldorf --set "edge.plantAddress=$plant_ip" \
  --set "clockOffsetHours=${CLOCK_OFFSET_HOURS:-0}"
kubectl kustomize --load-restrictor LoadRestrictionsNone deploy/integrations |
  kubectl --context k3d-cloud apply -f -
kubectl --context k3d-cloud -n pourline wait customerintegration --all \
  --for=jsonpath='{.status.phase}'=Running --timeout=180s
helm upgrade --install plant-agent deploy/helm/plant-agent --kube-context k3d-plant-walldorf \
  --namespace pourline --create-namespace --wait --timeout 5m --set "image=$plant_image" \
  --set "clockOffsetHours=${CLOCK_OFFSET_HOURS:-0}"

kubectl --context k3d-cloud -n pourline get customerintegrations
cat <<EOF

Pourline runs in two k3d clusters on $NETWORK:
  cloud            the operator runs the three customer integrations; webhooks at
                   http://localhost:8080/webhooks/<name>, the dispatch API everywhere else
                   (Swagger UI at /swagger-ui/)
  plant-walldorf   the plant-agent ($plant_ip), which reaches the cloud as \`cloud\`
                   ($cloud_ip) through the uplink

  http://localhost:8080/console   the map console
  http://localhost:8080/grafana   Grafana
  http://localhost:8080/jaeger    Jaeger

  kubectl --context k3d-cloud -n pourline get customerintegrations
  kubectl --context k3d-cloud -n pourline get pods
  kubectl --context k3d-plant-walldorf -n pourline logs deploy/plant-agent -f
  make chaos       cuts plant-walldorf off the cloud for 2 minutes
EOF
if [ "${CLOCK_OFFSET_HOURS:-0}" != 0 ]; then
  echo "The world's clock is shifted by ${CLOCK_OFFSET_HOURS} h (CLOCK_OFFSET_HOURS), so the plants are open."
fi
