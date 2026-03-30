#!/usr/bin/env bash
# restart.sh — Frena todo, actualiza codigo y reinicia polyedge + MiroFish limpiamente
set -euo pipefail

echo "==> Frenando polyedge..."
sudo systemctl stop polyedge 2>/dev/null || true

echo "==> Matando procesos huerfanos..."
pkill -f "python main.py" 2>/dev/null || true
pkill -f "node.*mirofish" 2>/dev/null || true
pkill -f "npm.*dev" 2>/dev/null || true
sleep 2

echo "==> Actualizando codigo..."
cd ~/polyedge && git pull

echo "==> Iniciando MiroFish en background..."
cd ~/MiroFish
PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 npm run dev >> ~/polyedge/logs/mirofish.log 2>&1 &
MIROFISH_PID=$!
echo "    MiroFish PID: $MIROFISH_PID"

echo "==> Esperando que MiroFish levante..."
sleep 6

echo "==> Iniciando polyedge..."
sudo systemctl start polyedge

echo ""
echo "Todo corriendo. Para ver logs:"
echo "  polyedge:  sudo journalctl -u polyedge -f"
echo "  mirofish:  tail -f ~/polyedge/logs/mirofish.log"
