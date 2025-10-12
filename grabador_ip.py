#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
grabador_horario.py (env-aware)
-------------------------------
Grabador automático de cámara RTSP alineado por hora, configurable por
variables de entorno.

ENV VARS:
  RTSP_URL              : URL RTSP (codifica caracteres especiales)
  OUT_ROOT              : Carpeta raíz de grabaciones (default: /var/videos/camara1)
  CONTAINER             : mkv | mp4 (default: mkv)
  AUDIO_MODE            : copy | aac | none (default: copy)
  VIDEO_MODE            : copy | libx264 ... (default: copy)
  RETENTION_DAYS        : días de retención (0 = no limpiar) (default: 7)
  MAX_RETRIES           : intentos ffmpeg por segmento (default: 5)
  RETRY_SLEEP           : espera entre intentos (s) (default: 5)
  LOG_DIR               : carpeta de logs (default: /var/log/rtsp_grab)
  SOCK_TIMEOUT_US       : timeouts RTSP en microsegundos (default: 15000000)
  FFMPEG_BIN            : ruta a ffmpeg (default: ffmpeg)

Autor: Kamasys Technologies
"""

import os
import sys
import time
import logging
import subprocess
from datetime import datetime, timedelta

# ============ Helpers de entorno ============
def env_str(name, default):
    return os.environ.get(name, str(default)).strip()

def env_int(name, default):
    val = os.environ.get(name, None)
    if val is None or str(val).strip() == "":
        return int(default)
    try:
        return int(str(val).strip())
    except ValueError:
        return int(default)

def env_choice(name, default, allowed):
    val = os.environ.get(name, str(default)).strip().lower()
    return val if val in allowed else default

# ============ Config desde ENV ============
RTSP_URL        = env_str("RTSP_URL", "")
OUT_ROOT        = env_str("OUT_ROOT", "/var/videos/camara1")
CONTAINER       = env_choice("CONTAINER", "mkv", {"mkv", "mp4"})
AUDIO_MODE      = env_choice("AUDIO_MODE", "copy", {"copy", "aac", "none"})
VIDEO_MODE      = env_str("VIDEO_MODE", "copy")  # libre; validamos abajo si es "copy"
RETENTION_DAYS  = env_int("RETENTION_DAYS", 7)
MAX_RETRIES     = env_int("MAX_RETRIES", 5)
RETRY_SLEEP     = env_int("RETRY_SLEEP", 5)
LOG_DIR         = env_str("LOG_DIR", "/var/log/rtsp_grab")
SOCK_TIMEOUT_US = env_int("SOCK_TIMEOUT_US", 15000000)
FFMPEG_BIN      = env_str("FFMPEG_BIN", "ffmpeg")

if not RTSP_URL:
    print("ERROR: Debes definir RTSP_URL (export RTSP_URL='rtsp://...')", file=sys.stderr)
    sys.exit(1)

# --- Preparar entorno ---
os.makedirs(OUT_ROOT, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# --- Configurar logging (archivo + consola) ---
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "grabador.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
logging.getLogger("").addHandler(console)

def log(msg, level=logging.INFO):
    logging.log(level, msg)

# ============ Utilidades ============
def secs_to_next_hour():
    """Segundos hasta la próxima hora en punto (hora local)."""
    now = datetime.now()
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    return int((next_hour - now).total_seconds())

def build_outfile():
    """Ruta de salida: OUT_ROOT/YYYY-MM-DD/cam1_YYYY-MM-DD_HH-MM-SS.<ext>"""
    date_dir = datetime.now().strftime("%Y-%m-%d")
    base_dir = os.path.join(OUT_ROOT, date_dir)
    os.makedirs(base_dir, exist_ok=True)
    filename = f"cam1_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.{CONTAINER}"
    return os.path.join(base_dir, filename)

def cleanup_old():
    """Elimina archivos con más de RETENTION_DAYS días (0 = no limpiar)."""
    if RETENTION_DAYS <= 0:
        return
    cutoff = time.time() - RETENTION_DAYS * 86400
    for root, dirs, files in os.walk(OUT_ROOT):
        for f in files:
            path = os.path.join(root, f)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
                    log(f"[LIMPIEZA] Borrado antiguo: {path}")
            except Exception as e:
                log(f"[ERROR] No se pudo borrar {path}: {e}", logging.ERROR)
        for d in list(dirs):
            full_d = os.path.join(root, d)
            try:
                if not os.listdir(full_d):
                    os.rmdir(full_d)
            except Exception:
                pass

def record_segment(duration, outfile):
    """Graba un segmento con ffmpeg, con reintentos."""
    # Códecs segun config
    codec_args = []
    if VIDEO_MODE.lower() == "copy":
        codec_args += ["-c:v", "copy"]
    else:
        codec_args += ["-c:v", VIDEO_MODE]

    if AUDIO_MODE == "copy":
        codec_args += ["-c:a", "copy"]
    elif AUDIO_MODE == "aac":
        codec_args += ["-c:a", "aac", "-b:a", "64k", "-ar", "8000", "-ac", "1"]
    elif AUDIO_MODE == "none":
        codec_args += ["-an"]

    mp4_flags = ["-movflags", "+faststart"] if CONTAINER == "mp4" else []

    seg_log = os.path.join(LOG_DIR, f"{os.path.basename(outfile)}.log")

    cmd = [
        FFMPEG_BIN,
        "-stimeout", str(SOCK_TIMEOUT_US),
        "-rw_timeout", str(SOCK_TIMEOUT_US),
        "-rtsp_transport", "tcp",
        "-fflags", "+genpts",
        "-flags", "+global_header",
        "-i", RTSP_URL,
        *codec_args,
        *mp4_flags,
        "-t", str(duration),
        "-y",
        outfile
    ]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log(f"[FFMPEG] Intento {attempt}/{MAX_RETRIES} ({duration}s) → {outfile}")
            with open(seg_log, "a") as lf:
                lf.write(f"\n---- {datetime.now().isoformat()} INICIO ----\nCMD: {' '.join(cmd)}\n")
                result = subprocess.run(cmd, stdout=lf, stderr=lf)
                lf.write(f"---- {datetime.now().isoformat()} FIN ----\n")
            if result.returncode == 0:
                log(f"[OK] Segmento grabado: {outfile}")
                return True
            else:
                log(f"[WARN] ffmpeg terminó con código {result.returncode}", logging.WARNING)
        except Exception as e:
            log(f"[ERROR] Excepción ejecutando ffmpeg: {e}", logging.ERROR)
        time.sleep(RETRY_SLEEP)

    log(f"[ERROR] Segmento fallido tras {MAX_RETRIES} intentos: {outfile}", logging.ERROR)
    return False

# ============ Bucle principal ============
def main():
    log("=== Iniciando grabador horario (Python/env) ===")
    log(f"RTSP_URL        : {RTSP_URL}")
    log(f"OUT_ROOT        : {OUT_ROOT}")
    log(f"CONTAINER       : {CONTAINER}")
    log(f"AUDIO_MODE      : {AUDIO_MODE}")
    log(f"VIDEO_MODE      : {VIDEO_MODE}")
    log(f"RETENTION_DAYS  : {RETENTION_DAYS}")
    log(f"MAX_RETRIES     : {MAX_RETRIES}")
    log(f"RETRY_SLEEP     : {RETRY_SLEEP}")
    log(f"LOG_DIR         : {LOG_DIR}")
    log(f"SOCK_TIMEOUT_US : {SOCK_TIMEOUT_US}")
    log(f"FFMPEG_BIN      : {FFMPEG_BIN}")

    try:
        while True:
            duration = secs_to_next_hour()
            if duration <= 0:
                duration = 3600

            outfile = build_outfile()
            log(f"[NUEVO] Segmento {duration}s → {outfile}")

            ok = record_segment(duration, outfile)
            cleanup_old()

            if not ok:
                log("[ERROR] Segmento no completado; continúo con el siguiente.", logging.ERROR)

            time.sleep(2)
    except KeyboardInterrupt:
        log("[STOP] Grabador detenido manualmente.")
    except Exception as e:
        log(f"[FATAL] Error general: {e}", logging.ERROR)

    log("=== Grabador finalizado ===")

if __name__ == "__main__":
    import os
    import time
    main()
