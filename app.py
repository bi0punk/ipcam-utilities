#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import time
import os
import sys
import logging
import signal
from datetime import datetime, timedelta

# ================== CONFIG ==================
RTSP_URL   = "rtsp://admin:9H)p5x84@192.168.1.64:554/Streaming/Channels/101"
OUTPUT_DIR = "recordings"
LOG_FILE   = "recording.log"

# Si tu cámara tiene audio y quieres guardarlo, pon True.
# OJO: muchas Hikvision no traen audio en el canal principal; si no hay audio,
# mapearlo puede romper la grabación. Por eso va en False por defecto.
ENABLE_AUDIO = False

# Reintentos si FFmpeg se cae:
MAX_RETRIES          = 0            # 0 = ilimitados
RETRY_BACKOFF_FIRST  = 5            # segundos (primer intento)
RETRY_BACKOFF_MAX    = 60           # segundos (máx entre intentos)
# ============================================

os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ],
)
log = logging.getLogger("rtsp_hourly_recorder")

current_proc = None
stop_flag = False

def build_ffmpeg_cmd():
    """
    Crea el comando ffmpeg que organiza grabaciones por día,
    con nombres del tipo: Lunes_2025-10-13_00-00-00.mp4
    """
    now = datetime.now()
    day_dir = now.strftime("%Y-%m-%d")
    day_name = now.strftime("%A").capitalize()  # Ej: Lunes, Martes...
    output_day_dir = os.path.join(OUTPUT_DIR, day_dir)
    os.makedirs(output_day_dir, exist_ok=True)

    # Ruta de salida con formato strftime
    out_pattern = os.path.join(output_day_dir, f"{day_name}_%Y-%m-%d_%H-%M-%S.mp4")

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "info",
        "-rtsp_transport", "tcp",
        "-use_wallclock_as_timestamps", "1",
        "-i", RTSP_URL,
        "-map", "0:v:0",
        "-c:v", "copy",
    ]

    if ENABLE_AUDIO:
        cmd += ["-map", "0:a:0?", "-c:a", "aac", "-b:a", "128k"]
    else:
        cmd += ["-an"]

    cmd += [
        "-f", "segment",
        "-segment_time", "3600",
        "-segment_atclocktime", "1",
        "-reset_timestamps", "1",
        "-segment_format_options", "movflags=+faststart",
        "-strftime", "1",
        out_pattern
    ]

    return cmd



def start_ffmpeg():
    cmd = build_ffmpeg_cmd()
    log.info("Iniciando FFmpeg con segmentación horaria alineada al reloj…")
    log.info("CMD: %s", " ".join(cmd))
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        bufsize=1
    )
    return proc

def stream_ffmpeg_logs(proc):
    """
    Lee stderr de ffmpeg en tiempo real para tener visibilidad.
    """
    assert proc is not None
    for line in proc.stderr:
        line = line.strip()
        if line:
            log.info("FFmpeg: %s", line)
        if stop_flag:
            break

def sig_handler(sig, frame):
    global stop_flag, current_proc
    log.info("Señal %s recibida. Deteniendo con gracia…", sig)
    stop_flag = True
    if current_proc and current_proc.poll() is None:
        try:
            current_proc.terminate()
            current_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            log.warning("FFmpeg no cerró a tiempo; forzando kill()")
            current_proc.kill()

    log.info("Grabador detenido. Bye.")
    sys.exit(0)

signal.signal(signal.SIGINT, sig_handler)
signal.signal(signal.SIGTERM, sig_handler)

def wait_until_next_full_minute():
    """
    (Opcional) Si quieres que el primer corte esté más limpio, puedes esperar
    a que cambie de minuto antes de lanzar FFmpeg. No es estrictamente necesario
    porque -segment_atclocktime 1 ya hará el primer corte a la próxima hora,
    pero ayuda a que los timestamps arranquen “bonitos”.
    """
    now = datetime.now()
    sleep_s = 60 - now.second
    if sleep_s and sleep_s < 60:
        log.info("Sincronizando al siguiente minuto (%ds)…", sleep_s)
        time.sleep(sleep_s)

def main():
    log.info("=== RTSP Hourly Recorder ===")
    log.info("Fuente: %s", RTSP_URL)
    log.info("Salida: %s", os.path.abspath(OUTPUT_DIR))
    log.info("Hora de inicio: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("Audio: %s", "ON (aac)" if ENABLE_AUDIO else "OFF")

    # No es obligatorio, pero hace el arranque un pelín más ordenado.
    # Si no lo quieres, comenta la siguiente línea.
    # wait_until_next_full_minute()

    retries = 0
    backoff = RETRY_BACKOFF_FIRST

    while not stop_flag:
        try:
            global current_proc
            current_proc = start_ffmpeg()

            # Leer logs en “tiempo real”
            stream_ffmpeg_logs(current_proc)

            # Si llegó aquí, el proceso terminó/rompió
            rc = current_proc.poll()
            if rc is None:
                # Lo matamos nosotros
                log.info("FFmpeg finalizado por señal local.")
                break
            elif rc == 0:
                log.info("FFmpeg salió normalmente (rc=0). Finalizando.")
                break
            else:
                log.error("FFmpeg terminó con error (rc=%s).", rc)

            # Reintentos
            retries += 1
            if (MAX_RETRIES > 0) and (retries > MAX_RETRIES):
                log.error("Se alcanzó el máximo de reintentos (%d). Saliendo.", MAX_RETRIES)
                break

            log.info("Reintentando en %ds… (intento %d)", backoff, retries)
            time.sleep(backoff)
            backoff = min(backoff * 2, RETRY_BACKOFF_MAX)

        except KeyboardInterrupt:
            sig_handler("SIGINT", None)
        except Exception as e:
            log.exception("Error inesperado en el lazo principal: %s", e)
            time.sleep(5)

    log.info("=== Grabación finalizada ===")

if __name__ == "__main__":
    main()
