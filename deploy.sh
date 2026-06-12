#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Tanzania Fuel Price API — one-shot Ubuntu 24.04 deployment
# Run as root: bash deploy.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

APP_DIR="/opt/apps/tz-fuel-api"
VENV="$APP_DIR/venv"
SERVICE_NAME="tz-fuel-api"
NGINX_CONF="/etc/nginx/sites-available/$SERVICE_NAME"

GREEN='\033[0;32m'; NC='\033[0m'
info() { echo -e "${GREEN}[deploy]${NC} $*"; }

# ── 1. System packages ────────────────────────────────────────────────────────
info "Installing system dependencies..."
apt-get update -qq
apt-get install -y --no-install-recommends \
    python3.12 python3.12-venv python3-pip \
    libpq-dev build-essential nginx curl

# ── 2. Python virtual env ─────────────────────────────────────────────────────
info "Setting up Python venv at $VENV..."
python3.12 -m venv "$VENV"
"$VENV/bin/pip" install --upgrade pip --quiet
"$VENV/bin/pip" install --no-cache-dir -r "$APP_DIR/requirements.txt" --quiet
info "Python packages installed."

# ── 3. Environment file ───────────────────────────────────────────────────────
if [ ! -f "$APP_DIR/.env" ]; then
    cp "$APP_DIR/.env.example" "$APP_DIR/.env"
    SECRET=$(python3 -c "import secrets; print(secrets.token_hex(32))")
    sed -i "s/change-me-to-a-strong-secret/$SECRET/" "$APP_DIR/.env"
    info ".env created with a generated ADMIN_SECRET_KEY. Edit $APP_DIR/.env to set DATABASE_URL."
    echo ""
    echo "  ADMIN_SECRET_KEY = $SECRET"
    echo "  (also saved in $APP_DIR/.env)"
    echo ""
fi

# ── 4. Permissions ────────────────────────────────────────────────────────────
chown -R www-data:www-data "$APP_DIR"
chmod 640 "$APP_DIR/.env"

# ── 5. systemd service ────────────────────────────────────────────────────────
info "Installing systemd service..."
cp "$APP_DIR/$SERVICE_NAME.service" "/etc/systemd/system/$SERVICE_NAME.service"
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"
info "Service status:"
systemctl status "$SERVICE_NAME" --no-pager || true

# ── 6. Nginx ──────────────────────────────────────────────────────────────────
info "Configuring nginx reverse proxy..."
cp "$APP_DIR/nginx.conf" "$NGINX_CONF"
ln -sf "$NGINX_CONF" "/etc/nginx/sites-enabled/$SERVICE_NAME"
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl reload nginx
info "Nginx configured."

# ── 7. Health check ───────────────────────────────────────────────────────────
info "Waiting for API to start..."
for i in $(seq 1 15); do
    if curl -fsS http://127.0.0.1:8000/health > /dev/null 2>&1; then
        info "API is healthy!"
        break
    fi
    sleep 2
done

echo ""
info "Deployment complete."
echo "  API:  http://$(hostname -I | awk '{print $1}')/"
echo "  Docs: http://$(hostname -I | awk '{print $1}')/docs"
echo ""
echo "  Trigger first sync:"
echo "  curl -X POST http://localhost:8000/api/v1/admin/trigger-sync \\"
echo "       -H 'X-API-KEY: \$(grep ADMIN_SECRET_KEY $APP_DIR/.env | cut -d= -f2)'"
