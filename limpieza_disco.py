#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
limpieza_disco.py
-----------------
Mantiene libre un porcentaje o cantidad mínima de espacio en el filesystem
que contiene un árbol de grabaciones (p. ej. /var/videos/camara1),
eliminando archivos antiguos primero hasta cumplir el objetivo.

Características:
- Umbral por porcentaje libre (--min-free-percent) y/o por GB libres (--min-free-gb)
- Orden de borrado: del más antiguo al más nuevo
- Filtrado por extensiones (por defecto: mkv, mp4, avi)
- Logs detallados a archivo: análisis de espacio, % libre, sin borrados,
  y para cada archivo eliminado: fecha/hora, ruta y tamaño.
- Modo simulación (--dry-run) para revisar qué pasaría sin borrar nada.
"""

import argparse
import os
import sys
import time
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

# ================== Config por defecto ==================
DEFAULT_SCAN_PATH = "/var/videos/camara1"
DEFAULT_LOG_DIR   = "/var/log/rtsp_grab"
DEFAULT_LOG_FILE  = "limpieza.log"
DEFAULT_EXTS      = ["mkv", "mp4", "avi"]  # extensiones a considerar
DEFAULT_MIN_FREE_PERCENT = 15.0            # % libre deseado (filesystem)
DEFAULT_MIN_FREE_GB      = 0.0             # GB libres deseados (0 = ignora)
DEFAULT_MAX_DELETE       = 500             # safety: máximo archivos a borrar en una pasada
# ========================================================

def human_size(num_bytes: int) -> str:
    """Convierte bytes a string legible."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024.0:
            return f"{num_bytes:,.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:,.2f} PB"

def fs_space_stats(path: Path) -> Tuple[float, float, float, float]:
    """
    Retorna (total_gb, used_gb, free_gb, free_percent) del FS que contiene 'path'.
    """
    usage = shutil.disk_usage(path)
    total = usage.total / (1024**3)
    free  = usage.free  / (1024**3)
    used  = total - free
    free_percent = (free / total * 100.0) if total > 0 else 0.0
    return total, used, free, free_percent

def collect_files(root: Path, exts: List[str]) -> List[Path]:
    """
    Lista de archivos bajo root con extensiones permitidas.
    """
    exts_lower = {e.lower().lstrip(".") for e in exts}
    files: List[Path] = []
    for p in root.rglob("*"):
        if p.is_file():
            if p.suffix.lower().lstrip(".") in exts_lower:
                files.append(p)
    return files

def setup_logger(log_dir: Path, log_name: str) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_name

    logger = logging.getLogger("limpieza_disco")
    logger.setLevel(logging.INFO)

    # Evitar handlers duplicados si se invoca varias veces
    if not logger.handlers:
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        fh.setFormatter(fmt)

        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.INFO)
        sh.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", "%Y-%m-%d %H:%M:%S"))

        logger.addHandler(fh)
        logger.addHandler(sh)

    return logger

def needs_cleanup(free_percent: float, free_gb: float,
                  min_percent: float, min_gb: float) -> bool:
    """Determina si hay que limpiar por % o por GB (se cumple si falta cualquiera de los dos umbrales)."""
    if min_percent > 0 and free_percent < min_percent:
        return True
    if min_gb > 0 and free_gb < min_gb:
        return True
    return False

def main():
    ap = argparse.ArgumentParser(description="Limpieza de grabaciones por espacio libre.")
    ap.add_argument("--path", default=DEFAULT_SCAN_PATH, help="Directorio raíz a limpiar (grabaciones).")
    ap.add_argument("--log-dir", default=DEFAULT_LOG_DIR, help="Directorio de logs.")
    ap.add_argument("--log-name", default=DEFAULT_LOG_FILE, help="Nombre de archivo de log.")
    ap.add_argument("--ext", nargs="+", default=DEFAULT_EXTS, help="Extensiones a considerar (ej: mkv mp4 avi).")
    ap.add_argument("--min-free-percent", type=float, default=DEFAULT_MIN_FREE_PERCENT,
                    help="Porcentaje mínimo libre deseado en el FS.")
    ap.add_argument("--min-free-gb", type=float, default=DEFAULT_MIN_FREE_GB,
                    help="GB libres mínimos deseados en el FS.")
    ap.add_argument("--max-delete", type=int, default=DEFAULT_MAX_DELETE,
                    help="Máximo de archivos a borrar en una pasada (safety).")
    ap.add_argument("--dry-run", action="store_true", help="Simulación: no borra, solo informa.")
    args = ap.parse_args()

    root = Path(args.path).resolve()
    log_dir = Path(args.log_dir).resolve()

    logger = setup_logger(log_dir, args.log_name)

    if not root.exists():
        logger.error(f"Directorio no existe: {root}")
        sys.exit(1)

    # ----- Análisis inicial -----
    total_gb, used_gb, free_gb, free_percent = fs_space_stats(root)
    logger.info("=== INICIO LIMPIEZA ===")
    logger.info(f"Ruta de trabajo     : {root}")
    logger.info(f"Extensiones         : {', '.join(args.ext)}")
    logger.info(f"Umbral % libre      : {args.min_free_percent:.2f}%")
    logger.info(f"Umbral GB libres    : {args.min_free_gb:.2f} GB")
    logger.info(f"Máx. archivos borrar: {args.max_delete} (safety)")
    logger.info(f"Modo simulación     : {'ON' if args.dry_run else 'OFF'}")
    logger.info(f"Espacio total       : {total_gb:,.2f} GB")
    logger.info(f"Espacio usado       : {used_gb:,.2f} GB")
    logger.info(f"Espacio libre       : {free_gb:,.2f} GB  ({free_percent:.2f}%)")

    if not needs_cleanup(free_percent, free_gb, args.min_free_percent, args.min_free_gb):
        logger.info("Sin elementos borrados: umbrales cumplidos. ✅")
        logger.info("=== FIN LIMPIEZA ===")
        return

    # ----- Recolectar y ordenar por antigüedad -----
    files = collect_files(root, args.ext)
    files.sort(key=lambda p: p.stat().st_mtime)  # más antiguos primero

    if not files:
        logger.warning("No se encontraron archivos con las extensiones indicadas.")
        logger.info("=== FIN LIMPIEZA ===")
        return

    # ----- Borrado progresivo hasta cumplir umbral -----
    deleted_count = 0
    for f in files:
        if deleted_count >= args.max_delete:
            logger.warning(f"Límite de borrado alcanzado (max-delete={args.max_delete}). Deteniendo.")
            break

        # Recalcular espacio antes de decidir el siguiente borrado
        _, _, free_gb, free_percent = fs_space_stats(root)
        if not needs_cleanup(free_percent, free_gb, args.min_free_percent, args.min_free_gb):
            break

        try:
            size_bytes = f.stat().st_size
        except FileNotFoundError:
            continue

        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if args.dry_run:
            logger.info(f"[SIMULACIÓN] {ts} Borraría: {f}  ({human_size(size_bytes)})")
        else:
            try:
                f.unlink()
                deleted_count += 1
                # Recalcular para informar nuevo % libre
                _, _, new_free_gb, new_free_percent = fs_space_stats(root)
                logger.info(f"{ts} BORRADO: {f}  ({human_size(size_bytes)})  "
                            f"→ Libre: {new_free_gb:,.2f} GB ({new_free_percent:.2f}%)")
                # Intentar limpiar directorios vacíos ascendiendo
                parent = f.parent
                while parent != root and not any(parent.iterdir()):
                    parent.rmdir()
                    parent = parent.parent
            except Exception as e:
                logger.error(f"{ts} ERROR al borrar {f}: {e}")

    # ----- Resumen final -----
    total_gb, used_gb, free_gb, free_percent = fs_space_stats(root)
    logger.info(f"Archivos eliminados : {deleted_count}")
    logger.info(f"Estado final        : Libre {free_gb:,.2f} GB ({free_percent:.2f}%)")
    if needs_cleanup(free_percent, free_gb, args.min_free_percent, args.min_free_gb):
        logger.warning("Aún no se alcanzó el umbral deseado. Considera revisar max-delete o ampliar umbral/extensiones.")
    logger.info("=== FIN LIMPIEZA ===")

if __name__ == "__main__":
    main()
