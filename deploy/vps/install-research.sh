#!/usr/bin/env bash
set -Eeuo pipefail

SRC=/opt/app-platform/src/efra-sniper-v2
STACK=/opt/app-platform/stacks/efra-research
STATE=/opt/app-platform/state/efra-research
REPO=https://github.com/TechCodinz/efra-bot.git

echo "===== EFRA SHADOW RESEARCH — INSTALL ====="
echo "Execution engine will NOT be restarted or modified."

for cmd in git docker; do
  command -v "$cmd" >/dev/null || { echo "ABORT: missing $cmd"; exit 1; }
done

mkdir -p "$(dirname "$SRC")" "$STACK" "$STATE"

if [ -d "$SRC/.git" ]; then
  git -C "$SRC" fetch origin \
    vps-smart-exit-20261004:refs/remotes/origin/vps-smart-exit-20261004
  git -C "$SRC" checkout -B vps-smart-exit-20261004 \
    refs/remotes/origin/vps-smart-exit-20261004
else
  rm -rf "$SRC"
  git clone --branch vps-smart-exit-20261004 --single-branch "$REPO" "$SRC"
fi

cp "$SRC/deploy/vps/research-compose.yml" "$STACK/compose.yml"

cd "$STACK"
docker compose -p efra-research -f compose.yml up -d --build

echo
echo "===== EXECUTION ENGINE (UNCHANGED) ====="
docker ps --filter name=efra-sniper-v2   --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

echo
echo "===== SHADOW RESEARCH ====="
docker ps --filter name=efra-research   --format 'table {{.Names}}\t{{.Status}}'

echo
echo "Research data: $STATE/efra_research.db"
echo "Latest report: $STATE/latest-analysis.txt"
echo "History:       $STATE/analysis-history.log"
echo
echo "Recorder tail:"
docker logs --tail 25 efra-research-recorder 2>&1 || true
echo
echo "The analyzer waits 1 hour between reports."
echo "It cannot modify EFRA trading parameters or place orders."
