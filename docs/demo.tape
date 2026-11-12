# docs/demo.gif: the re-dispatch scenario of `make demo`, recorded with VHS
# (https://github.com/charmbracelet/vhs). Re-record from the repository root after the demo's
# output changes:
#
#   make up            # builds the images, if you haven't already
#   vhs docs/demo.tape
#
# Off camera it restarts the stack with empty volumes and slow order generation, as `make demo`
# does, so the scenario plays on a quiet world. It leaves the stack running at the normal order rate.

Output docs/demo.gif

Set Shell bash
Set FontSize 14
Set Width 1400
Set Height 920
Set Padding 20
Set Framerate 24
Set TypingSpeed 40ms
# The demo takes about 80 s in real time, mostly waiting for re-dispatches.
Set PlaybackSpeed 2.0

Hide
Type "export COMPOSE_FILE=deploy/compose/compose.yaml" Enter
Type "docker compose down -v --remove-orphans && ORDER_INTERVAL_SECS=3600 docker compose up -d --wait && clear" Enter
Wait@10m
Show

Type "docker compose --progress quiet --profile demo run --rm demo"
Sleep 500ms
Enter
Wait@5m
# Time to read the last screen.
Sleep 16s

Hide
Type "docker compose up -d --wait simulators" Enter
Wait@5m
