#!/usr/bin/env bash
# Run the SynTasks preview locally: every service in its own terminal tab.
# Redis (a throwaway one on :55460), two workers ("machines") and one web server,
# so you watch each one's real log live, and a tab that stops comes back on Enter.
#
#   ./dev.sh [start] [name…]   start the services (all, when none named), each in its own tab
#   ./dev.sh stop [name…]      stop them (all, when none named)
#   ./dev.sh status            what is running, and the cluster seen from the web server
#   ./dev.sh logs [name…]      follow the logs in this terminal: .dev/logs/<name>.log
#   ./dev.sh kill [worker1]    SIGKILL a worker (its tab stays: Enter starts it again)
#   ./dev.sh open              open the page in your browser
#   ./dev.sh run <name>        one service in this terminal (what each tab runs)
#
# Services: redis worker1 worker2 web
# No desktop (or DEV_TERM=bg): they run in the background; watch them with ./dev.sh logs.
# WEB_PORT (8001) and REDIS_PORT (55460, must match syntasks.toml) change the ports.
# If something already listens on the Redis port, that Redis is used and never stopped here.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGS=$ROOT/.dev/logs PIDS=$ROOT/.dev/pids
SERVICES=(redis worker1 worker2 web)
VENV=$ROOT/../.venv/bin
WEB_PORT=${WEB_PORT:-8001}
REDIS_PORT=${REDIS_PORT:-55460}
export SHOP_REDIS_URL=${SHOP_REDIS_URL:-redis://127.0.0.1:$REDIS_PORT/0}

die()   { echo "dev.sh: $*" >&2; exit 1; }
known() { [[ " ${SERVICES[*]} " == *" $1 "* ]] || die "no service '$1': one of ${SERVICES[*]}"; }
port()  { case $1 in redis) echo "$REDIS_PORT";; web) echo "$WEB_PORT";; *) echo "";; esac; }
listening() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
# The pid of a service's tab, if it's still ours and alive.
pid_of() { local p; { read -r p _ < "$PIDS/$1"; } 2>/dev/null || return 1
           kill -0 "$p" 2>/dev/null && tr '\0' ' ' < "/proc/$p/cmdline" | grep -q dev.sh && echo "$p"; }
# A process and everything below it (a worker supervisor has its worker processes as children).
tree() { local c; echo "$1"; for c in $(pgrep -P "$1" 2>/dev/null); do tree "$c"; done; }

# The service itself, in the foreground. It records its own pid (the exec keeps it).
exec_service() {
  export FORCE_COLOR=1 PYTHONUNBUFFERED=1
  echo $BASHPID > "$PIDS/$1.svc"
  cd "$ROOT"
  case $1 in
    redis)   exec redis-server --port "$REDIS_PORT" --save "" --appendonly no ;;
    worker1|worker2) exec "$VENV/syntasks" worker ;;
    web)     WEB_NAME=web exec "$VENV/uvicorn" shop.web:api --port "$WEB_PORT" --reload --reload-dir shop --use-colors ;;
  esac
}

# What each tab runs: the service, its output on screen and in its log; Enter restarts it when it stops.
cmd_run() {
  known "${1:-}"; mkdir -p "$LOGS" "$PIDS"
  # Never start a second copy (two would overwrite each other's pid file).
  if pid_of "$1" >/dev/null || { p=$(port "$1"); [ -n "$p" ] && listening "$p"; }; then
    echo "$1 is already running (or its port is taken): ./dev.sh stop $1 first"; sleep 4; exit 1
  fi
  echo $$ > "$PIDS/$1"
  printf '\033]0;%s\007' "$1"
  while true; do
    echo "── $1 · $(date '+%F %T')" | tee "$LOGS/$1.log"
    if [ "$1" != redis ]; then   # workers and the web server need Redis first
      for _ in $(seq 1 100); do listening "$REDIS_PORT" && break; sleep 0.3; done
    fi
    ( exec_service "$1" ) 2>&1 | tee -a "$LOGS/$1.log" || true
    echo "── $1 stopped"
    [ -t 0 ] || exit 0
    read -rp "Enter = start it again · close the tab to quit " _ || exit 0
  done
}

cmd_start() {
  local list=("$@") todo=() s
  [ $# -gt 0 ] || list=("${SERVICES[@]}")
  [ -x "$VENV/python" ] || die "no $VENV/python: run 'uv sync --all-extras --group preview' in the repository root"
  "$VENV/python" -c "import fastapi, uvicorn, websockets" 2>/dev/null ||
    die "web dependencies missing: run 'uv sync --all-extras --group preview' in the repository root"
  for s in "${list[@]}"; do
    known "$s"
    if pid_of "$s" >/dev/null; then echo "$s is already running"
    elif [ "$s" = redis ] && listening "$REDIS_PORT"; then echo "redis: using the one already on :$REDIS_PORT"
    else todo+=("$s"); fi
  done
  [ ${#todo[@]} -gt 0 ] || return 0
  [[ " ${todo[*]} " != *" redis "* ]] || command -v redis-server >/dev/null || die "redis-server is not installed"
  mkdir -p "$LOGS" "$PIDS"
  if [ "${DEV_TERM:-}" != bg ] && [ -n "${DISPLAY:-}${WAYLAND_DISPLAY:-}" ] && command -v gnome-terminal >/dev/null; then
    local args=() flag=--window
    for s in "${todo[@]}"; do args+=("$flag" --title "$s" -e "$ROOT/dev.sh run $s"); flag=--tab; done
    gnome-terminal --working-directory "$ROOT" "${args[@]}" 2>/dev/null
    echo "Started in terminal tabs: ${todo[*]}"
  else
    for s in "${todo[@]}"; do setsid "$ROOT/dev.sh" run "$s" </dev/null >/dev/null 2>&1 & done
    echo "Started in the background: ${todo[*]}"
  fi
  local up=0 i
  for i in $(seq 1 100); do
    up=$(curl -s "localhost:$WEB_PORT/api/cluster" 2>/dev/null | grep -o '"alive":true' | wc -l || true)
    if [ "$up" -ge 4 ]; then break; fi
    sleep 0.3
  done
  if [ "$up" -ge 4 ]; then
    echo "Up: http://127.0.0.1:$WEB_PORT/  ($up worker processes)"
  else
    echo "Not all up yet: check the tabs, or ./dev.sh logs"
  fi
  echo "logs: ./dev.sh logs [name] · kill a worker: ./dev.sh kill worker1 · stop: ./dev.sh stop"
}

cmd_stop() {
  local list=("$@") pids=() s p i
  [ $# -gt 0 ] || list=("${SERVICES[@]}")
  # Last started, first stopped: the web server and workers before Redis.
  for s in "${list[@]}"; do known "$s"; done
  if [ $# -eq 0 ]; then list=(web worker1 worker2 redis); fi
  for s in "${list[@]}"; do
    if p=$(pid_of "$s"); then kill -TERM -- "-$p" 2>/dev/null || true; pids+=("$p"); echo "stopped $s"; fi
    rm -f "$PIDS/$s" "$PIDS/$s.svc"
  done
  for i in $(seq 1 30); do
    local left=0; for p in ${pids[@]+"${pids[@]}"}; do kill -0 -- "-$p" 2>/dev/null && left=1; done
    [ $left = 0 ] && break; sleep 0.5
  done
  for p in ${pids[@]+"${pids[@]}"}; do kill -KILL -- "-$p" 2>/dev/null || true; done
}

cmd_status() {
  local s p st
  for s in "${SERVICES[@]}"; do
    p=$(port "$s"); pid_of "$s" >/dev/null && st=running || st=stopped
    if [ -n "$p" ]; then listening "$p" && p=":$p up" || p=":$p down"; fi
    printf '  %-8s %-8s %s\n' "$st" "$s" "$p"
  done
  { curl -s "localhost:$WEB_PORT/api/cluster" 2>/dev/null || true; } | "$VENV/python" -c "
import json, sys
try:
    d = json.load(sys.stdin)
except ValueError:
    sys.exit()
by = {}
for n in d['nodes']:
    by.setdefault(n['machine'], []).append(n['alive'])
print('  cluster:', ', '.join(f'{m}: {sum(v)} process(es) alive' for m, v in sorted(by.items())))
" || true
}

cmd_logs() {
  local files=() s
  [ $# -gt 0 ] || set -- "${SERVICES[@]}"
  for s; do known "$s"; files+=("$LOGS/$s.log"); done
  exec tail -n 40 -F "${files[@]}"
}

# SIGKILL a worker supervisor and its processes. Its tab stays open: Enter starts it again.
cmd_kill() {
  local s=${1:-worker1} p
  [[ "$s" =~ ^worker[12]$ ]] || s=worker$s
  known "$s"
  pid_of "$s" >/dev/null || die "$s is not running"
  p=$(cat "$PIDS/$s.svc" 2>/dev/null) || die "$s has no service pid yet"
  # shellcheck disable=SC2046
  kill -KILL $(tree "$p") 2>/dev/null || true
  echo "$s killed (SIGKILL). Watch the other worker take over; Enter in its tab starts it again."
}

cmd_open() { xdg-open "http://127.0.0.1:$WEB_PORT/" >/dev/null 2>&1 || echo "http://127.0.0.1:$WEB_PORT/"; }

cmd="${1:-start}"; shift || true
case $cmd in
  start) cmd_start "$@" ;;  stop) cmd_stop "$@" ;;  status) cmd_status ;;  logs) cmd_logs "$@" ;;
  run) cmd_run "$@" ;;  kill) cmd_kill "$@" ;;  open) cmd_open ;;
  *) known "$cmd"; cmd_start "$cmd" "$@" ;;   # ./dev.sh web → start just that one
esac
