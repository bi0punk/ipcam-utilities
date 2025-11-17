#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import time
import os
import sys
import logging
import signal
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from colorama import init, Fore, Style
import boto3
from botocore.exceptions import BotoCoreError, ClientError

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Inicializar colorama
init(autoreset=True)

# ================== CONFIG ==================
RTSP_URL   = ""
OUTPUT_DIR = "recordings"
LOG_FILE   = "recording.log"

ENABLE_AUDIO = False

# Reintentos
MAX_RETRIES          = 0
RETRY_BACKOFF_FIRST  = 5
RETRY_BACKOFF_MAX    = 60

# Config MinIO (S3 compatible)
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS   = ""
MINIO_SECRET   = ""
MINIO_BUCKET   = "cctv"

# Zona horaria
TZ = ZoneInfo("America/Santiago")

DOW_ES = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]

MESES_ES = {
    "01": "enero",
    "02": "febrero",
    "03": "marzo",
    "04": "abril",
    "05": "mayo",
    "06": "junio",
    "07": "julio",
    "08": "agosto",
    "09": "septiembre",
    "10": "octubre",
    "11": "noviembre",
    "12": "diciembre"
}

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

# =============== MinIO S3 CLIENT =================
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS,
    aws_secret_access_key=MINIO_SECRET,
    region_name="us-east-1"
)
# =================================================


def color_info(msg):
    print(Fore.CYAN + "[INFO] " + Style.RESET_ALL + msg)


def color_ok(msg):
    print(Fore.GREEN + "[OK] " + Style.RESET_ALL + msg)


def color_warn(msg):
    print(Fore.YELLOW + "[WARN] " + Style.RESET_ALL + msg)


def color_err(msg):
    print(Fore.RED + "[ERROR] " + Style.RESET_ALL + msg)


# =====================================================================
#                  UTILIDAD: ESPERAR ARCHIVO COMPLETO
# =====================================================================
def wait_until_file_complete(path, check_interval=10, stable_checks=3):
    """
    Espera hasta que el archivo deje de crecer.
    - check_interval: segundos entre revisiones
    - stable_checks : cuántas veces seguidas debe mantenerse igual el tamaño
    """
    last_size = -1
    stable_count = 0

    color_info(f"Esperando a que termine la grabación de: {os.path.basename(path)}")

    while not stop_flag:
        if not os.path.exists(path):
            color_warn(f"El archivo desapareció mientras se esperaba: {path}")
            return False

        size = os.path.getsize(path)

        if size == last_size and size > 0:
            stable_count += 1
            if stable_count >= stable_checks:
                color_ok(
                    f"Archivo estable (sin cambios): {os.path.basename(path)} "
                    f"(size={size} bytes)"
                )
                return True
        else:
            stable_count = 0
            last_size = size

        time.sleep(check_interval)

    color_warn(f"Se detuvo la espera de completitud para: {path}")
    return False


# =====================================================================
#                      WATCHER PARA SUBIR A MINIO
# =====================================================================
class UploadHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        filepath = event.src_path

        threading.Thread(
            target=upload_to_minio,
            args=(filepath,),
            daemon=True
        ).start()


def upload_to_minio(filepath):
    """
    Sube un archivo a MinIO con estructura: año/mes/día/archivo.mp4
    Espera primero a que el archivo deje de crecer (segmento finalizado).
    """
    try:
        filename = os.path.basename(filepath)

        # 1) Esperar a que el archivo deje de crecer (grabación terminada)
        if not wait_until_file_complete(filepath):
            color_warn(f"No se logró confirmar que el archivo terminara: {filename}")
            return

        # 2) Parsear fecha del archivo (Lunes_2025-01-01_13-00-00.mp4)
        try:
            parts = filename.split("_")
            fecha = parts[1]  # YYYY-MM-DD
            año, mes_num, dia = fecha.split("-")
            mes = MESES_ES.get(mes_num, mes_num)   # Convertir a nombre
        except Exception as e:
            color_warn(f"No se pudo inferir fecha desde nombre: {filename} ({e})")
            return

        key = f"{año}/{mes}/{dia}/{filename}"

        color_info(f"Subiendo a MinIO: {key}")

        with open(filepath, "rb") as f:
            s3.upload_fileobj(f, MINIO_BUCKET, key)

        color_ok(f"Archivo subido correctamente: {filename}")

        # Si quieres borrar el archivo local después de subirlo, descomenta:
        # os.remove(filepath)
        # color_info(f"Archivo local eliminado: {filename}")

    except (BotoCoreError, ClientError) as e:
        color_err(f"Fallo al subir {filename}: {e}")
    except Exception as e:
        color_err(f"Error inesperado en upload_to_minio({filename}): {e}")


def start_watcher():
    event_handler = UploadHandler()
    observer = Observer()
    observer.schedule(event_handler, OUTPUT_DIR, recursive=True)
    observer.start()
    color_ok("Watcher MinIO activo (subida automática).")
    return observer


# =====================================================================
#                          UTILIDADES
# =====================================================================
def today_info():
    now = datetime.now(TZ)
    return now.strftime("%Y-%m-%d"), DOW_ES[now.weekday()]


def ensure_day_dir(day_dir):
    path = os.path.join(OUTPUT_DIR, day_dir)
    os.makedirs(path, exist_ok=True)
    return path


def build_ffmpeg_cmd(day_dir, day_name):
    output_day_dir = ensure_day_dir(day_dir)
    out_pattern = os.path.join(output_day_dir, f"{day_name}_%Y-%m-%d_%H-%M-%S.mp4")

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "info",
        "-nostats",                  # <--- evita mostrar "frame= ... fps= ..."
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



def start_ffmpeg(day_dir, day_name):
    cmd = build_ffmpeg_cmd(day_dir, day_name)
    color_info(f"Grabando día: {day_dir} ({day_name})")
    color_info("Comando FFmpeg listo.")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        bufsize=1
    )
    return proc


def stream_ffmpeg_logs(proc):
    for line in proc.stderr:
        if stop_flag:
            break
        line = line.strip()
        if line:
            print(Fore.MAGENTA + "[FFmpeg] " + Style.RESET_ALL + line)


def terminate_ffmpeg(proc):
    if proc and proc.poll() is None:
        color_warn("Terminando FFmpeg suavemente…")
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            color_err("FFmpeg no respondió, matando proceso.")
            proc.kill()


def sig_handler(sig, frame):
    global stop_flag, current_proc
    color_warn(f"Señal recibida ({sig}). Deteniendo…")
    stop_flag = True
    terminate_ffmpeg(current_proc)
    color_ok("Grabador apagado correctamente.")
    sys.exit(0)


# =====================================================================
#                             MAIN LOOP
# =====================================================================
def main():
    color_ok("=== Grabador RTSP Mejorado ===")
    color_info(f"Fuente RTSP: {RTSP_URL}")
    color_info(f"MinIO Bucket: {MINIO_BUCKET}")

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    # iniciar watcher
    observer = start_watcher()

    retries = 0
    backoff = RETRY_BACKOFF_FIRST

    current_day_dir, current_day_name = today_info()

    while not stop_flag:
        try:
            global current_proc

            # Iniciar grabación FFmpeg
            current_proc = start_ffmpeg(current_day_dir, current_day_name)

            t = threading.Thread(target=stream_ffmpeg_logs, args=(current_proc,), daemon=True)
            t.start()

            # Supervisión
            while current_proc.poll() is None and not stop_flag:
                time.sleep(15)
                day_dir, day_name = today_info()

                # Cambio de día
                if day_dir != current_day_dir:
                    color_info(f"Cambio de día detectado → {day_dir}")
                    current_day_dir, current_day_name = day_dir, day_name
                    ensure_day_dir(current_day_dir)
                    terminate_ffmpeg(current_proc)
                    break

            rc = current_proc.poll()
            if rc is None and stop_flag:
                break

            if rc != 0:
                color_err(f"FFmpeg terminó con error rc={rc}")
            else:
                color_warn("FFmpeg terminó inesperadamente (rc=0).")

            retries += 1
            if (MAX_RETRIES > 0) and (retries > MAX_RETRIES):
                color_err("Máximo de reintentos alcanzado.")
                break

            color_warn(f"Reintentando en {backoff}s…")
            time.sleep(backoff)
            backoff = min(backoff * 2, RETRY_BACKOFF_MAX)

        except Exception as e:
            color_err(f"Error: {e}")
            time.sleep(5)

    # apagar watcher
    observer.stop()
    observer.join()

    color_ok("=== Grabación finalizada ===")


if __name__ == "__main__":
    main()
