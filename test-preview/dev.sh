#!/usr/bin/env bash
# Start and stop the whole preview: Redis, two workers, one web server.
#
#   ./dev.sh start          start everything in the background, wait until it is up
#   ./dev.sh logs [name]    follow all logs (or one: redis, worker1, worker2, web)
#   ./dev.sh status         what is running
#   ./dev.sh kill worker1   kill a worker the hard way (SIGKILL) to watch the takeover
#   ./dev.sh stop           stop everything (workers get a graceful shutdown)
#   ./dev.sh restart
#
# Logs and pids are in test-preview/.dev/. Set WEB_PORT or REDIS_PORT to change ports
# (REDIS_PORT must match syntasks.toml, 55460 by default). Redis is started only if
# nothing listens on its port yet, and only a Redis started here is stopped here.
set -uo pipefail
cd "$(dirname "$0")"

VENV=../.venv/bin
WEB_PORT=${WEB_PORT:-8001}
REDIS_PORT=${REDIS_PORT:-55460}
DIR=.dev
export SHOP_REDIS_URL=${SHOP_REDIS_URL:-redis://127.0.0.1:$REDIS_PORT/0}

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
die() { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }
listening() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
alive() { [ -f "$DIR/$1.pid" ] && kill -0 "$(cat "$DIR/$1.pid")" 2>/dev/null; }

# launch NAME CMD...: its own session (so `kill -- -pid` takes the whole worker
# supervisor and its processes), output to .dev/NAME.log
launch() {
  local name=$1; shift
  : > "$DIR/$name.log"
  setsid "$@" >> "$DIR/$name.log" 2>&1 &
  echo $! > "$DIR/$name.pid"
}

start() {
  [ -x "$VENV/python" ] || die "no $VENV/python: run 'uv sync --all-extras --group preview' in the repository root"
  "$VENV/python" -c "import fastapi, uvicorn, websockets" 2>/dev/null \
    || die "web dependencies are missing: run 'uv sync --all-extras --group preview' in the repository root"
  mkdir -p "$DIR"
  for n in redis worker1 worker2 web; do
    alive $n && die "$n is running already (./dev.sh status); use ./dev.sh restart"
  done
  listening "$WEB_PORT" && die "port $WEB_PORT is in use"

  bold "Redis"
  if listening "$REDIS_PORT"; then
    echo "  something already listens on $REDIS_PORT: using it (a disposable Redis, please)"
  else
    command -v redis-server >/dev/null || die "redis-server is not installed (or: docker compose up -d --wait)"
    launch redis redis-server --port "$REDIS_PORT" --save "" --appendonly no
    for _ in $(seq 50); do listening "$REDIS_PORT" && break; sleep 0.1; done
    listening "$REDIS_PORT" || die "redis did not start; see $DIR/redis.log"
    echo "  started on $REDIS_PORT"
  fi

  bold "Workers and web server"
  launch worker1 "$VENV/syntasks" worker
  launch worker2 "$VENV/syntasks" worker
  WEB_NAME=web launch web "$VENV/uvicorn" shop.web:api --port "$WEB_PORT" --log-level info
  echo "  starting worker1, worker2 and the web server..."

  for _ in $(seq 100); do
    up=$(curl -s "localhost:$WEB_PORT/api/cluster" 2>/dev/null | grep -o '"alive":true' | wc -l)
    [ "$up" -ge 4 ] && break
    for n in worker1 worker2 web; do
      alive $n || { tail -n 15 "$DIR/$n.log"; die "$n stopped; see $DIR/$n.log"; }
    done
    sleep 0.3
  done
  [ "${up:-0}" -ge 4 ] || die "not ready after 30 s; see ./dev.sh logs"

  echo
  bold "Up: 2 workers (4 processes) and 1 web server"
  echo "  page      http://127.0.0.1:$WEB_PORT/"
  echo "  api docs  http://127.0.0.1:$WEB_PORT/docs"
  echo "  logs      ./dev.sh logs        (or: logs worker1 | worker2 | web | redis)"
  echo "  cli       SHOP_REDIS_URL=$SHOP_REDIS_URL $VENV/syntasks nodes"
  echo "  stop      ./dev.sh stop"
}

# Stop a launched process group; TERM first, KILL if it hasn't gone in 15 s.
halt() {
  local name=$1 pid
  alive "$name" || { rm -f "$DIR/$name.pid"; return; }
  pid=$(cat "$DIR/$name.pid")
  kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null
  for _ in $(seq 150); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
  if kill -0 "$pid" 2>/dev/null; then
    kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null
    echo "  $name did not stop in time: killed"
  else
    echo "  $name stopped"
  fi
  rm -f "$DIR/$name.pid"
}

stop() {
  [ -d "$DIR" ] || { echo "nothing to stop"; return; }
  bold "Stopping"
  halt web
  halt worker1 &
  halt worker2 &
  wait
  halt redis
}

status() {
  for n in redis worker1 worker2 web; do
    if alive $n; then printf '  %-8s running  (pid %s)\n' "$n" "$(cat "$DIR/$n.pid")"
    else printf '  %-8s stopped\n' "$n"; fi
  done
  listening "$REDIS_PORT" && echo "  redis port $REDIS_PORT: listening" || echo "  redis port $REDIS_PORT: closed"
  curl -s "localhost:$WEB_PORT/api/cluster" 2>/dev/null | "$VENV/python" -c "
import json, sys
try:
    d = json.load(sys.stdin)
except ValueError:
    sys.exit()
by = {}
for n in d['nodes']:
    by.setdefault(n['machine'], []).append(n['alive'])
print('  cluster:', ', '.join(f'{m}: {sum(v)} process(es) alive' for m, v in sorted(by.items())))
"
}

logs() {
  local names=("$@")
  [ ${#names[@]} -gt 0 ] || names=(redis worker1 worker2 web)
  local colors=(36 33 35 32) i=0 pids=()
  trap 'kill "${pids[@]}" 2>/dev/null; exit 0' INT TERM
  for n in "${names[@]}"; do
    [ -f "$DIR/$n.log" ] || die "no log for '$n' (redis, worker1, worker2, web)"
    tail -n 5 -F "$DIR/$n.log" 2>/dev/null | sed -u "s/^/\x1b[${colors[i % 4]}m$(printf '%-7s' "$n")\x1b[0m /" &
    pids+=($!)
    i=$((i + 1))
  done
  wait
}

kill_worker() {
  local n=${1:-worker1}
  [[ "$n" =~ ^worker[12]$ ]] || n=worker$n
  alive "$n" || die "$n is not running"
  kill -KILL -- "-$(cat "$DIR/$n.pid")" 2>/dev/null
  echo "$n killed (SIGKILL): watch the cluster card on the page, or: ./dev.sh logs"
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  restart) stop; start ;;
  status) status ;;
  logs) shift; logs "$@" ;;
  kill) shift; kill_worker "$@" ;;
  *) sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
