#!/usr/bin/env bash
set -Eeuo pipefail

BRANCH=vps-smart-exit-20261004
SRC=/opt/app-platform/src/efra-sniper-v2
STACK=/opt/app-platform/stacks/efra-sniper-v2
ENV=/opt/app-platform/secrets/efra-sniper-v2.env
STATE=/opt/app-platform/state/efra-sniper-v2
REPO=https://github.com/TechCodinz/efra-bot.git

echo "===== EFRA SMART-EXIT VPS UPDATE ====="
echo "Branch: $BRANCH"
echo "Mode: PAPER"
echo "Research containers are left untouched."

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
[[ "$EFRA_HOST_PORT" =~ ^[0-9]+$ ]] || { echo "ABORT: no EFRA port"; exit 1; }
export EFRA_HOST_PORT
echo "EFRA host port: $EFRA_HOST_PORT"

if [ -d "$SRC/.git" ]; then
  git -C "$SRC" fetch origin "$BRANCH"
  git -C "$SRC" checkout -B "$BRANCH" "origin/$BRANCH"
else
  rm -rf "$SRC"
  git clone --branch "$BRANCH" --single-branch "$REPO" "$SRC"
fi

echo "Source commit: $(git -C "$SRC" rev-parse HEAD)"
cp "$SRC/deploy/vps/compose.yml" "$STACK/compose.yml"

umask 077
touch "$ENV"
python3 - "$ENV" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
existing = {}
order = []
for raw in path.read_text().splitlines():
    line = raw.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    if k not in existing:
        order.append(k)
    existing[k] = v

# These values are copied from the smart-exit engine's own Cfg defaults.
# This is deployment parity, not strategy invention.
updates = {
    "EFRA_EXCHANGE": "gate",
    "EFRA_MODE": "taker",
    "EFRA_PAPER": "true",
    "EFRA_START_BALANCE": "100.0",
    "EFRA_STATE_FILE": "/data/efra_state.json",
    "EFRA_LOG_FILE": "/data/efra_trades.csv",
    "EFRA_STATE_KEY": "gate:taker:paper:vps-smart-exit",
    "EFRA_TP_BPS": "250.0",
    "EFRA_SL_BPS": "35.0",
    "EFRA_BREAKEVEN_BPS": "120.0",
    "EFRA_TRAIL_TRIGGER_BPS": "130.0",
    "EFRA_TRAIL_BPS": "15.0",
    "EFRA_MIN_CONFLUENCE": "62.0",
    "EFRA_MIN_CVD": "0.65",
    "EFRA_IMBALANCE_ENTRY": "0.72",
    "EFRA_MIN_MOM_BPS": "3.5",
    "EFRA_MAX_MOM_BPS": "18.0",
    "EFRA_MIN_MOM_ACCEL": "0.0",
    "EFRA_MIN_WALL_RATIO": "0.0",
    "EFRA_VOL_SURGE_FACTOR": "1.5",
    "EFRA_SMART_REENTRY": "true",
    "EFRA_SLOW_BAIL_HOLD_S": "75.0",
    "EFRA_SLOW_BAIL_RET_BPS": "-8.0",
    "EFRA_SLOW_BAIL_MOM_BPS": "-4.0",
    "EFRA_SLOW_BAIL_CVD": "0.38",
    "EFRA_FLIP_BAIL_HOLD_S": "60.0",
    "EFRA_FLIP_BAIL_IMB": "0.15",
    "EFRA_FLIP_BAIL_RET_BPS": "-5.0",
    "EFRA_SL_WICK_DEBOUNCE_S": "1.5",
    "EFRA_SL_CLUSTER_WINDOW_S": "90.0",
    "EFRA_SL_CLUSTER_COUNT": "2",
    "EFRA_SL_CLUSTER_PAUSE_S": "120.0",
    "EFRA_STREAK_CONF_BOOST": "8.0",
    "EFRA_STREAK_OBI_BOOST": "0.05",
    "EFRA_INTER_TRADE_PAUSE_S": "45.0",
    "EFRA_POSITION_FRAC": "0.40",
    "EFRA_DAILY_LOSS_LIMIT_FRAC": "0.08",
    "EFRA_WATCH_N": "20",
    "EFRA_MAX_HOLD_S": "180",
    "EFRA_LOG_LEVEL": "INFO",
}
for k, v in updates.items():
    if k not in existing:
        order.append(k)
    existing[k] = v

# Remove obsolete gates from the previous VPS policy so they cannot imply
# behavior the new engine no longer uses.
for stale in ("EFRA_MIN_IMPULSE_COST_RATIO", "EFRA_MIN_ENTRY_CONFIDENCE", "EFRA_MIN_SIGNAL_PATHS"):
    existing.pop(stale, None)
    if stale in order:
        order.remove(stale)

path.write_text("\n".join(f"{k}={existing[k]}" for k in order if k in existing) + "\n")
PY
chmod 600 "$ENV"

echo
echo "===== BUILD + RESTART EXECUTION ONLY ====="
cd "$STACK"
docker compose -p efra-sniper-v2 -f compose.yml up -d --build --remove-orphans

echo
echo "===== WAIT FOR HEALTH ====="
for i in $(seq 1 45); do
  if curl -fsS "http://127.0.0.1:$EFRA_HOST_PORT/health" >/tmp/efra-health.json 2>/dev/null; then
    cat /tmp/efra-health.json
    echo
    break
  fi
  sleep 2
done

curl -fsS "http://127.0.0.1:$EFRA_HOST_PORT/health" >/dev/null || {
  echo "ABORT: smart-exit EFRA failed health check"
  docker logs --tail 160 efra-sniper-v2 || true
  exit 1
}

echo
echo "===== RUNTIME SNAPSHOT ====="
curl -fsS "http://127.0.0.1:$EFRA_HOST_PORT/api/status" | python3 -m json.tool | head -120 || true

echo
echo "===== CONTAINERS ====="
docker ps --filter name=efra-sniper-v2 --filter name=efra-research   --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

echo
echo "===== ENGINE LOG ====="
docker logs --tail 80 efra-sniper-v2 2>&1 || true
