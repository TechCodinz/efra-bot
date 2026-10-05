#!/usr/bin/env bash
set -Eeuo pipefail

SRC=/opt/app-platform/src/efra-sniper-v2
STATE=/opt/app-platform/state/efra-research
VENV="$STATE/venv-v2"
DB="$STATE/efra_v2.db"
UNIT=/etc/systemd/system/efra-research-v2.service

echo "===== EFRA RESEARCH V2 — SYSTEMD INSTALL ====="
echo "This manages research only. It does NOT enable or restart PAPER execution."

for path in "$SRC/efra_research_v2.py" "$VENV/bin/python"; do
  [ -e "$path" ] || { echo "ABORT: missing $path"; exit 1; }
done

mkdir -p "$STATE"

cat > "$UNIT" <<EOF
[Unit]
Description=EFRA Research v2 Gate tape/CVD recorder
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
WorkingDirectory=$SRC
ExecStart=$VENV/bin/python $SRC/efra_research_v2.py record --exchange gate --quote USDT --hours 120 --db $DB
Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=20
Environment=PYTHONUNBUFFERED=1
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload

# Stop only the old ad-hoc Research v2 recorder, never the EFRA execution container.
if ! systemctl is-active --quiet efra-research-v2.service; then
  mapfile -t old_pids < <(pgrep -f "$VENV/bin/python .*efra_research_v2.py record" || true)
  if [ "${#old_pids[@]}" -gt 0 ]; then
    echo "Stopping old nohup Research v2 PID(s): ${old_pids[*]}"
    kill "${old_pids[@]}" || true
    sleep 2
  fi
fi

systemctl enable --now efra-research-v2.service

echo
echo "===== SERVICE ====="
systemctl --no-pager --full status efra-research-v2.service | sed -n '1,22p'

echo
echo "===== EXECUTION SAFETY ====="
docker ps --filter name=efra-sniper-v2 \
  --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' || true

echo
echo "===== RECORDER LOG ====="
journalctl -u efra-research-v2.service -n 30 --no-pager || true

echo
echo "Research DB preserved at: $DB"
