#!/usr/bin/env bash
# make k8s (SPEC §9): the Docker network pourline-net, a local registry, the k3d clusters `cloud`
# and `plant-walldorf`, Pourline's images, and the manifests in deploy/k3d. It creates only what
# is missing, so it is safe to run again: it then rebuilds the images and restarts the services.
# SKIP_BUILD=1 reuses the images already in the registry, for a change to the manifests only.
set -euo pipefail
cd "$(dirname "$0")/../.."

NETWORK=pourline-net
SUBNET=172.28.0.0/24 # fixed, so make chaos can reconnect the plant node with its old IP
REGISTRY=pourline-registry
SERVICES=(ingest dispatch simulators demo plant-agent)

for tool in docker k3d kubectl; do
  command -v "$tool" >/dev/null || {
    echo "make k8s needs $tool. On macOS: brew install k3d kubectl (and Docker). Check: k3d version && kubectl version --client"
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

if [ "${SKIP_BUILD:-}" != 1 ]; then
  for service in "${SERVICES[@]}"; do
    docker build --build-arg BIN="$service" -t "localhost:5050/$service:dev" .
    docker push "localhost:5050/$service:dev"
  done
fi

# deploy CLUSTER DEPLOYMENT...: applies the cluster's manifests, restarts Pourline's own
# deployments so they pull the new images, and waits until everything is ready.
deploy() {
  local cluster=$1
  shift
  kubectl kustomize --load-restrictor LoadRestrictionsNone "deploy/k3d/$cluster" |
    kubectl --context "k3d-$cluster" apply -f -
  for deployment in "$@"; do
    kubectl --context "k3d-$cluster" -n pourline rollout restart "deployment/$deployment"
  done
  kubectl --context "k3d-$cluster" -n pourline rollout status deployment --timeout=300s
}
deploy cloud ingest dispatch simulators
deploy plant-walldorf plant-agent

cat <<EOF

Pourline runs in two k3d clusters on $NETWORK:
  cloud            ingest and dispatch at http://localhost:8080 (webhooks under /webhooks,
                   the dispatch API everywhere else, Swagger UI at /swagger-ui/)
  plant-walldorf   the plant-agent, which reaches the cloud as \`cloud\` ($cloud_ip)

  kubectl --context k3d-cloud -n pourline get pods
  kubectl --context k3d-plant-walldorf -n pourline logs deploy/plant-agent -f
  make chaos       cuts plant-walldorf off the cloud for 2 minutes
EOF
