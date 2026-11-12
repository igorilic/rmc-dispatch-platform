# docs/demo.gif: a short overview of Pourline in three scenes, recorded with VHS
# (https://github.com/charmbracelet/vhs):
#   1. a truck breaks down, then a pump is late, and each re-dispatch says why each load moved
#      (make demo)
#   2. a customer's mapping goes wrong: the order lands in the dead-letter queue with the reason,
#      and a replay after the fix publishes it (ingest-cli)
#   3. on Kubernetes, the operator runs each customer, and the plant keeps batching while it is
#      cut off from the cloud (make k8s, make chaos)
# The waits happen off camera: recording takes about 15 minutes, the GIF about half a minute.
#
# Run it from the repository root, at any time: outside the plants' opening hours, make demo and
# make k8s shift the simulated world's clock into the daytime (CLOCK_OFFSET_HOURS).
#
#   vhs docs/demo.tape
#
# It needs what make demo and make k8s need. It runs make down first, briefly breaks
# customers/neckar-infra/mapping.yaml (if the recording stops halfway, restore it with
# `git checkout customers/neckar-infra/mapping.yaml`), and leaves the k3d clusters running.

Output docs/demo.gif

Set Shell bash
Set FontSize 14
Set Width 1500
Set Height 920
Set Padding 20
Set Framerate 24
Set TypingSpeed 15ms
Set PlaybackSpeed 1.25

Hide
# make down first: a k3d cluster left running would hold port 8080, and make demo would fail.
Type "make down >/dev/null 2>&1; export COMPOSE_FILE=deploy/compose/compose.yaml && cargo build -q -p ingest-cli && clear" Enter
Wait@10m
Show

# Scene 1: make demo, cut to its two re-dispatches (steps 3 and 4). The stack restart and the rest
# of the scenario run off camera.
Type "# 1. A truck breaks down, then a pump is late: dispatch repairs the plan and says why each load moved" Enter
Type "make demo 2>&1 | sed -n '/^[34]\. /,/^$/p'" Enter
Hide
Wait@20m
Show
Sleep 9s

# Scene 2: break neckar-infra's mapping, post its v2 sample order, read the DLQ, fix, replay.
Hide
Type "clear" Enter
Show
Type "# 2. A mapping goes wrong: the order lands in the dead-letter queue with the reason, and a replay fixes it" Enter
Type "sed -i.bak s/@shipTo/@shipto/ customers/neckar-infra/mapping.yaml && docker compose --progress quiet restart ingest" Enter
Hide
Wait
Sleep 6s
Show
Type "curl -s -H 'Authorization: Bearer dev-token-neckar-infra' --data-binary @customers/neckar-infra/samples/order-v2.xml localhost:8080/webhooks/neckar-infra; echo" Enter
Wait
Sleep 2s
Type "cargo run -q -p ingest-cli -- dlq list" Enter
Wait
Sleep 3s
Type "mv customers/neckar-infra/mapping.yaml.bak customers/neckar-infra/mapping.yaml && docker compose --progress quiet restart ingest" Enter
Hide
Wait
Sleep 6s
Show
Type "cargo run -q -p ingest-cli -- dlq replay --customer neckar-infra" Enter
Wait
Sleep 4s

# Scene 3: two k3d clusters. A first make chaos off camera gives the plant loads that fall due
# during the cut on camera.
Hide
Type "make down >/dev/null 2>&1; make k8s >/dev/null 2>&1 && make chaos >/dev/null 2>&1; clear" Enter
Wait@45m
Show
Type "# 3. On Kubernetes an operator runs each customer, and a plant cut off from the cloud keeps batching" Enter
Type "kubectl --context k3d-cloud -n pourline get customerintegrations" Enter
Wait
Sleep 3s
Type "make chaos" Enter
Hide
Wait@10m
Show
Sleep 9s
