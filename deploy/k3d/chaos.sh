#!/usr/bin/env bash
# make chaos (SPEC §9): cuts the plant-walldorf cluster off pourline-net for 2 minutes, then
# reconnects it, and shows the plant-agent's outbox filling and draining. It reads the agent's
# /status through the plant's own API server, inside the node, which answers while it is cut off.
# CHAOS_SECONDS sets the length of the cut.
set -euo pipefail
cd "$(dirname "$0")/../.."

NETWORK=pourline-net
NODE=k3d-plant-walldorf-server-0
CUT=${CHAOS_SECONDS:-120}

if ! k3d cluster get plant-walldorf >/dev/null 2>&1 || ! k3d cluster get cloud >/dev/null 2>&1; then
  echo "make chaos needs the k3d clusters: run make k8s first."
  exit 1
fi

status() {
  docker exec "$NODE" kubectl get --raw \
    /api/v1/namespaces/pourline/services/plant-agent:http/proxy/status 2>/dev/null || true
}
# A field of the status, which is one flat JSON object.
field() { sed -n "s/.*\"$1\":\([^,}]*\).*/\1/p" <<<"$2"; }
started=$SECONDS
report() {
  local s links
  s=$(status)
  if [ -z "$s" ]; then
    printf '  t+%3ss  (the agent is not answering)\n' $((SECONDS - started))
    return
  fi
  links=down
  [ "$(field mqtt "$s")" = true ] && [ "$(field dispatch "$s")" = true ] && links=up
  printf '  t+%3ss  links %-4s  outbox %s  sync lag %4ss  plan v%s, %4ss old\n' \
    $((SECONDS - started)) "$links" "$(field outbox "$s")" "$(field sync_lag_seconds "$s")" \
    "$(field plan_version "$s")" "$(field plan_age_seconds "$s")"
}

# The order pod runs on the same world clock as the cloud, which make k8s may have shifted.
offset=$(kubectl --context k3d-cloud -n pourline get deploy dispatch \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CLOCK_OFFSET_HOURS")].value}')
echo "Placing three orders near Walldorf, so its bays are busy during the cut…"
orders=$(kubectl --context k3d-cloud -n pourline run "chaos-orders-$(date +%s)" --rm -i \
  --restart=Never --quiet --image k3d-pourline-registry:5050/demo:dev --image-pull-policy=Always \
  --env DISPATCH_URL=http://dispatch:8081 \
  --env BAUWERK_WEBHOOK_URL=http://ingest-bauwerk-ag:8080/webhooks/bauwerk-ag \
  --env NECKAR_WEBHOOK_URL=http://ingest-neckar-infra:8080/webhooks/neckar-infra \
  --env MQTT_URL=mqtt://mosquitto:1883 --env "CLOCK_OFFSET_HOURS=${offset:-0}" -- edge) || { echo "$orders"; exit 1; }
grep -v '^PLAN_VERSION=' <<<"$orders"
version=$(sed -n 's/^PLAN_VERSION=//p' <<<"$orders")
[ -n "$version" ] || { echo "the orders weren't planned"; exit 1; }

deadline=$((SECONDS + 60))
until [ "$(field plan_version "$(status)")" -ge "$version" ] 2>/dev/null; do
  [ $SECONDS -lt $deadline ] || { echo "the agent didn't get plan v$version within 60 s"; exit 1; }
  sleep 2
done
echo "  the agent has plan v$version"

ip=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NETWORK\").IPAddress}}" "$NODE")
reconnect() {
  # k3s doesn't survive a new node IP, so the node comes back with its old one.
  docker network connect --ip "$ip" "$NETWORK" "$NODE" 2>/dev/null || true
}
trap reconnect EXIT

echo "Cutting plant-walldorf off $NETWORK for $CUT s"
started=$SECONDS
docker network disconnect "$NETWORK" "$NODE"
until [ $((SECONDS - started)) -ge "$CUT" ]; do
  report
  sleep 10
done
cut=$(status)
reconnect
trap - EXIT
echo "Reconnected"
reconnected=$SECONDS
deadline=$((SECONDS + 120))
while :; do
  sleep 5
  report
  s=$(status)
  [ "$(field outbox "$s")" = 0 ] && [ "$(field mqtt "$s")" = true ] &&
    [ "$(field dispatch "$s")" = true ] && break
  [ $SECONDS -lt $deadline ] || { echo "the outbox didn't drain within 120 s"; exit 1; }
done

# Nothing leaves the outbox during the cut, so what waits at its end is what the cut held back.
# (The agent's own counters restart with its pod, so they can't say this.)
held=$(field outbox "$cut")
if [ "${held:-0}" -gt 0 ]; then
  echo "✓ $held tickets waited out the cut, and all were sent in order within $((SECONDS - reconnected)) s of reconnecting"
else
  echo "No ticket fell due during the cut, so there was nothing to hold back. Run make chaos again."
fi
