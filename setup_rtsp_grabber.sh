#!/usr/bin/env bash
# ============================================================
# setup_rtsp_grabber.sh
# ------------------------------------------------------------
# Crea toda la estructura necesaria para el grabador horario
# Python + systemd + logs + permisos correctos
# ============================================================

set -e
USER_NAME=${SUDO_USER:-$USER}
APP_DIR="/opt/rtsp_grabber"
ENV_FILE="/etc/rtsp-grabber.env"
SERVICE_FILE="/etc/systemd/system/rtsp-grabber.service"
VIDEOS_DIR="/var/videos/camara1"
LOG_DIR="/var/log/rtsp_grab"

echo "=== Instalador automático de RTSP Grabber (Kamasys) ==="
echo "[*] Usuario actual: $USER_NAME"

# --- Crear directorios principales ---
echo "[*] Creando estructura de carpetas..."
sudo mkdir -p "$APP_DIR" "$VIDEOS_DIR" "$LOG_DIR"

# --- Copiar script principal ---
if [ -f "./grabador_horario.py" ]; then
  sudo cp ./grabador_horario.py "$APP_DIR/"
else
  echo "[ERROR] No se encontró grabador_horario.py en el directorio actual."
  exit 1
fi
sudo chmod +x "$APP_DIR/grabador_horario.py"

# --- Archivo .env con configuración base ---
echo "[*] Creando archivo de entorno..."
sudo tee "$ENV_FILE" > /dev/null <<EOF
# === Configuración RTSP Grabber ===
RTSP_URL=
OUT_ROOT=$VIDEOS_DIR
LOG_DIR=$LOG_DIR
CONTAINER=mkv
AUDIO_MODE=copy
VIDEO_MODE=copy
RETENTION_DAYS=7
FFMPEG_BIN=ffmpeg
SOCK_TIMEOUT_US=15000000
MAX_RETRIES=5
RETRY_SLEEP=5
EOF

sudo chmod 600 "$ENV_FILE"

# --- Crear servicio systemd ---
echo "[*] Creando servicio systemd..."
sudo tee "$SERVICE_FILE" > /dev/null <<EOF
[Unit]
Description=Grabador RTSP horario (Kamasys Technologies)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER_NAME
WorkingDirectory=$APP_DIR
EnvironmentFile=$ENV_FILE
ExecStart=/usr/bin/python3 $APP_DIR/grabador_horario.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# --- Ajustar permisos ---
echo "[*] Ajustando permisos..."
sudo chown -R "$USER_NAME":"$USER_NAME" "$VIDEOS_DIR" "$LOG_DIR" "$APP_DIR"

# --- Recargar y activar el servicio ---
echo "[*] Activando servicio..."
sudo systemctl daemon-reload
sudo systemctl enable rtsp-grabber.service
sudo systemctl restart rtsp-grabber.service

echo "[✓] Instalación completa."
echo "Verifica el estado con:   sudo systemctl status rtsp-grabber.service"
echo "Ver logs en vivo con:      sudo journalctl -u rtsp-grabber.service -f"
