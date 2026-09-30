#!/usr/bin/env bash
set -Eeuo pipefail

SRC=/opt/app-platform/src/efra-sniper-v2
STACK=/opt/app-platform/stacks/efra-sniper-v2
ENV=/opt/app-platform/secrets/efra-sniper-v2.env
STATE=/opt/app-platform/state/efra-sniper-v2
REPO=https://github.com/TechCodinz/efra-bot.git

echo "===== EFRA SNIPER v2 — VPS PAPER INSTALL ====="

for cmd in git docker curl python3 ss; do
  command -v "$cmd" >/dev/null || { echo "ABORT: missing $cmd"; exit 1; }
done

mkdir -p "$(dirname "$SRC")" "$STACK" "$(dirname "$ENV")" "$STATE"

PORT_FILE="$STATE/host-port"
if [ -s "$PORT_FILE" ]; then
  EFRA_HOST_PORT="$(tr -dc '0-9' < "$PORT_FILE")"
else
  EFRA_HOST_PORT=""
  for candidate in 18085 18086 18087 18088 18089 18090 18185 18186; do
    if ! ss -ltnH 2>/dev/null | awk '{print $4}' | grep -Eq "(^|:)${candidate}$"; then
      EFRA_HOST_PORT="$candidate"
      printf '%s\n' "$EFRA_HOST_PORT" > "$PORT_FILE"
      break
    fi
  done
fi

if ! [[ "$EFRA_HOST_PORT" =~ ^[0-9]+$ ]]; then
  echo "ABORT: could not select a free EFRA host port"
  exit 1
fi
export EFRA_HOST_PORT
echo "EFRA host port: $EFRA_HOST_PORT"

if [ -d "$SRC/.git" ]; then
  git -C "$SRC" fetch origin master
  git -C "$SRC" checkout master
  git -C "$SRC" reset --hard origin/master
else
  rm -rf "$SRC"
  git clone --branch master --single-branch "$REPO" "$SRC"
fi

cp "$SRC/deploy/vps/compose.yml" "$STACK/compose.yml"

if [ ! -f "$ENV" ]; then
  umask 077
  cat >"$ENV" <<'EOF'
EFRA_EXCHANGE=gate
EFRA_MODE=taker
EFRA_PAPER=true
EFRA_START_BALANCE=100.0
EFRA_STATE_FILE=/data/efra_state.json
EFRA_LOG_FILE=/data/efra_trades.csv
EFRA_STATE_KEY=gate:taker:paper:vps
EFRA_TP_BPS=200.0
EFRA_SL_BPS=40.0
EFRA_BREAKEVEN_BPS=80.0
EFRA_TRAIL_TRIGGER_BPS=100.0
EFRA_TRAIL_BPS=30.0
EFRA_MIN_CONFLUENCE=55.0
EFRA_MIN_IMPULSE_COST_RATIO=0.30
EFRA_MIN_ENTRY_CONFIDENCE=65.0
EFRA_MIN_SIGNAL_PATHS=2
EFRA_INTER_TRADE_PAUSE_S=8.0
EFRA_POSITION_FRAC=0.40
EFRA_DAILY_LOSS_LIMIT_FRAC=0.08
EFRA_WATCH_N=18
EFRA_MAX_HOLD_S=420
EFRA_LOG_LEVEL=INFO
EOF
  chmod 600 "$ENV"
fi

cd "$STACK"
docker compose -p efra-sniper-v2 -f compose.yml up -d --build --remove-orphans

echo
echo "===== WAITING FOR LOCAL HEALTH ====="
for i in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:$EFRA_HOST_PORT/health >/tmp/efra-health.json 2>/dev/null; then
    cat /tmp/efra-health.json
    echo
    break
  fi
  sleep 2
done

if ! curl -fsS http://127.0.0.1:$EFRA_HOST_PORT/health >/dev/null; then
  echo "ABORT: EFRA did not become healthy"
  docker logs --tail 100 efra-sniper-v2 || true
  exit 1
fi

PUBLIC_IP="$(curl -4 -fsS https://api.ipify.org || true)"
if [ -n "$PUBLIC_IP" ] && command -v caddy >/dev/null && [ -f /etc/caddy/Caddyfile ]; then
  HOST="efra-sniper-v2.$(printf '%s' "$PUBLIC_IP" | tr . -).nip.io"
  BEGIN="# BEGIN EFRA-SNIPER-V2"
  END="# END EFRA-SNIPER-V2"
  python3 - "$HOST" "$EFRA_HOST_PORT" <<'PY'
from pathlib import Path
import sys
path = Path("/etc/caddy/Caddyfile")
host = sys.argv[1]
port = sys.argv[2]
begin = "# BEGIN EFRA-SNIPER-V2"
end = "# END EFRA-SNIPER-V2"
text = path.read_text()
block = f"""
{begin}
{host} {{
    reverse_proxy 127.0.0.1:{port}
}}
{end}
"""
if begin in text and end in text:
    before, rest = text.split(begin, 1)
    _, after = rest.split(end, 1)
    text = before.rstrip() + "\n\n" + block.strip() + "\n" + after.lstrip()
else:
    text = text.rstrip() + "\n\n" + block.strip() + "\n"
path.write_text(text)
PY
  caddy validate --config /etc/caddy/Caddyfile
  systemctl reload caddy
  echo
  echo "EFRA VPS API: https://$HOST"
  echo "Health:       https://$HOST/health"
else
  echo
  echo "Caddy/public host was not auto-configured."
  echo "Local API is healthy on 127.0.0.1:$EFRA_HOST_PORT."
fi

echo
echo "===== EFRA VPS STATUS ====="
docker ps --filter name=efra-sniper-v2 --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
docker logs --tail 35 efra-sniper-v2 || true
