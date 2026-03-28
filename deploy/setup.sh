#!/usr/bin/env bash
# polyedge — Oracle Cloud ARM Ubuntu 22.04 setup
# Run once as ubuntu user: bash setup.sh
set -euo pipefail

REPO="https://github.com/nachoilero7/bot-arbitraje.git"
APP_DIR="$HOME/polyedge"

echo "==> Updating system packages..."
sudo apt-get update -qq
sudo apt-get upgrade -y -qq

echo "==> Installing Python 3.11 and dependencies..."
sudo apt-get install -y -qq python3.11 python3.11-venv python3-pip git

echo "==> Cloning repository..."
if [ -d "$APP_DIR" ]; then
    echo "    Directory exists, pulling latest..."
    git -C "$APP_DIR" pull
else
    git clone "$REPO" "$APP_DIR"
fi

echo "==> Creating virtual environment..."
python3.11 -m venv "$APP_DIR/.venv"
source "$APP_DIR/.venv/bin/activate"

echo "==> Installing Python packages..."
pip install --upgrade pip -q
pip install -r "$APP_DIR/requirements.txt" -q

echo "==> Creating data and logs directories..."
mkdir -p "$APP_DIR/data" "$APP_DIR/logs"

echo "==> Installing systemd service..."
sudo cp "$APP_DIR/deploy/polyedge.service" /etc/systemd/system/polyedge.service
sudo sed -i "s|__HOME__|$HOME|g" /etc/systemd/system/polyedge.service
sudo sed -i "s|__USER__|$USER|g"  /etc/systemd/system/polyedge.service
sudo systemctl daemon-reload
sudo systemctl enable polyedge

echo ""
echo "==> Setup complete!"
echo ""
echo "Next steps:"
echo "  1. Upload your .env file:  scp .env ubuntu@<VM_IP>:$APP_DIR/.env"
echo "  2. Start the bot:          sudo systemctl start polyedge"
echo "  3. Check logs:             sudo journalctl -u polyedge -f"
echo "  4. Stop the bot:           sudo systemctl stop polyedge"
echo ""
