#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TUI (Textual) para grabación RTSP con FFmpeg.
- Estado en tiempo real: Conectando -> Conectado -> Iniciando grabación del día XX -> Grabando...
- Botón para mostrar/ocultar la salida de log en vivo.
- Mantiene segmentación horaria alineada al reloj y logging a archivo.

Instala dependencias:
    pip install textual
"""

import asyncio
import os
import sys
import logging
import signal
import contextlib
from datetime import datetime
from typing import Optional, List

# ================== CONFIG ==================
RTSP_URL   = "rtsp://admin:9H)p5x84@192.168.1.64:554/Streaming/Channels/101"
OUTPUT_DIR = "recordings"
LOG_FILE   = "recording.log"

# Si tu cámara tiene audio y quieres guardarlo, pon True.
ENABLE_AUDIO = False

# Reintentos si FFmpeg se cae:
MAX_RETRIES          = 0            # 0 = ilimitados
RETRY_BACKOFF_FIRST  = 5            # segundos (primer intento)
RETRY_BACKOFF_MAX    = 60           # segundos (máx entre intentos)
# ============================================

# ---------- Logging ----------
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

# ---------- Textual ----------
from textual.app import App, ComposeResult
from textual.containers import Vertical, Horizontal
from textual.widgets import Header, Footer, Static, Button

# TextLog cambió de lugar según la versión; y en versiones sin TextLog usamos Log.
try:
    # Textual <= ~0.49
    from textual.widgets import TextLog  # type: ignore
except Exception:
    try:
        # Textual ~0.50+ (algunas builds)
        from textual.widgets.text_log import TextLog  # type: ignore
    except Exception:
        # Fallback universal: usar Log con la misma referencia
        from textual.widgets import Log as TextLog  # type: ignore

from textual.reactive import reactive

# ---------- Lógica FFmpeg (async) ----------

def build_ffmpeg_cmd() -> List[str]:
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


class RecorderEvents:
    """Etiquetas simples para la UI."""
    CONNECTING = "Conectando a cámara…"
    CONNECTED = "Conectado."
    STARTING_DAY = "Iniciando grabación del día {day}…"
    RECORDING = "Grabando…"
    STOPPING = "Deteniendo…"
    STOPPED = "Grabación finalizada."
    RETRYING = "FFmpeg falló. Reintentando en {sec}s (intento {n})…"
    EXITED_OK = "FFmpeg salió normalmente (rc=0)."
    EXITED_ERR = "FFmpeg terminó con error (rc={rc})."


class Recorder:
    """Gestiona FFmpeg y emite eventos/logs a una cola para la UI."""
    def __init__(self, queue: asyncio.Queue[str]) -> None:
        self.queue = queue
        self.proc: Optional[asyncio.subprocess.Process] = None
        self.stop_flag = False

    async def _emit(self, msg: str) -> None:
        # Enviar a UI y al log de archivo
        await self.queue.put(msg)
        log.info(msg)

    async def start(self) -> None:
        await self._emit("=== RTSP Hourly Recorder ===")
        await self._emit(f"Fuente: {RTSP_URL}")
        await self._emit(f"Salida: {os.path.abspath(OUTPUT_DIR)}")
        await self._emit(f"Hora de inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        await self._emit(f"Audio: {'ON (aac)' if ENABLE_AUDIO else 'OFF'}")

        retries = 0
        backoff = RETRY_BACKOFF_FIRST

        while not self.stop_flag:
            try:
                await self._emit(RecorderEvents.CONNECTING)
                cmd = build_ffmpeg_cmd()
                await self._emit("Iniciando FFmpeg con segmentación horaria alineada al reloj…")
                await self._emit("CMD: " + " ".join(cmd))

                self.proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )

                # Tan pronto como empiece a emitir algo por stderr, consideramos "conectado".
                connected_already = False
                day_announced = False

                assert self.proc.stderr is not None
                while True:
                    line = await self.proc.stderr.readline()
                    if not line:
                        break
                    text = line.decode(errors="ignore").rstrip()

                    # Pasar todo al log y a la UI
                    await self.queue.put(f"FFmpeg: {text}")
                    log.info("FFmpeg: %s", text)

                    # Heurísticas baratas para estados:
                    if not connected_already:
                        # Aparición de cabeceras/streams suele ocurrir al conectar
                        if "Input #" in text or "Stream mapping:" in text or "frame=" in text:
                            connected_already = True
                            await self._emit(RecorderEvents.CONNECTED)

                    if not day_announced and ("Opening '" in text and text.endswith(".mp4' for writing")):
                        day_str = datetime.now().strftime("%Y-%m-%d")
                        await self._emit(RecorderEvents.STARTING_DAY.format(day=day_str))
                        await self._emit(RecorderEvents.RECORDING)
                        day_announced = True

                    if self.stop_flag:
                        break

                rc = await self.proc.wait()
                self.proc = None

                if self.stop_flag:
                    await self._emit(RecorderEvents.STOPPED)
                    break

                if rc == 0:
                    await self._emit(RecorderEvents.EXITED_OK)
                    break
                else:
                    await self._emit(RecorderEvents.EXITED_ERR.format(rc=rc))

                # Reintentos
                retries += 1
                if (MAX_RETRIES > 0) and (retries > MAX_RETRIES):
                    await self._emit(f"Se alcanzó el máximo de reintentos ({MAX_RETRIES}). Saliendo.")
                    break

                await self._emit(RecorderEvents.RETRYING.format(sec=backoff, n=retries))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, RETRY_BACKOFF_MAX)

            except asyncio.CancelledError:
                break
            except Exception as e:
                await self._emit(f"Error inesperado: {e}")
                await asyncio.sleep(5)

        await self._emit("=== Grabación finalizada ===")

    async def stop(self) -> None:
        if self.stop_flag:
            return
        self.stop_flag = True
        await self._emit(RecorderEvents.STOPPING)

        proc = self.proc
        if not proc:
            return

        if proc.returncode is None:
            # Intenta terminar con gracia
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()

            try:
                await asyncio.wait_for(proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                # No cerró a tiempo: forzamos kill y esperamos
                await self._emit("FFmpeg no cerró a tiempo; forzando kill()")
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                # Asegura que finaliza
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(proc.wait(), timeout=5)

        self.proc = None


# ---------- TUI ----------

class Estado(Static):
    texto = reactive("Preparado.")

    def watch_texto(self, value: str) -> None:
        self.update(value)


class GrabadorTUI(App):
    CSS = """
    Screen {
        align: center middle;
    }
    #panel {
        width: 100%;
        max-width: 100;
        height: auto;
        border: round $primary;
        padding: 1 2;
    }
    #titulo {
        content-align: center middle;
        height: 3;
    }
    #estado {
        height: auto;
        padding: 1 0;
    }
    #botonera {
        height: auto;
        padding: 1 0;
        content-align: center middle;
    }
    #log {
        height: 20;
        margin-top: 1;
    }
    """

    BINDINGS = [
        ("l", "toggle_log", "Mostrar/Ocultar log"),
        ("q", "quit", "Salir"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.recorder = Recorder(self.queue)
        self.queue_task: Optional[asyncio.Task] = None
        self.recorder_task: Optional[asyncio.Task] = None
        self.log_visible = True

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Vertical(id="panel"):
            yield Static("Grabador RTSP — Estado en tiempo real", id="titulo")
            self.estado = Estado(id="estado")
            yield self.estado
            with Horizontal(id="botonera"):
                yield Button("Mostrar/Ocultar Log (L)", id="toggle", variant="primary")
                yield Button("Detener", id="stop", variant="error")

            # Widget de log (TextLog o Log, según versión disponible)
            try:
                # Si es TextLog clásico:
                self.textlog = TextLog(id="log", highlight=False, wrap=False, auto_scroll=True)  # type: ignore
            except Exception:
                # Si es Log (fallback) o TextLog con firma distinta:
                self.textlog = TextLog(id="log")  # type: ignore

            yield self.textlog
        yield Footer()

    async def on_mount(self) -> None:
        # Iniciar tareas en segundo plano
        self.queue_task = asyncio.create_task(self._drain_queue())
        self.recorder_task = asyncio.create_task(self.recorder.start())
        # Mensaje inicial
        self.estado.texto = "Conectando a cámara…"

        # Señales del SO
        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(self._stop_from_signal("SIGINT")))
            loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(self._stop_from_signal("SIGTERM")))
        except NotImplementedError:
            # En Windows, add_signal_handler no está disponible para SIGINT/SIGTERM en ProactorEventLoop.
            pass

    async def _stop_from_signal(self, sig_name: str) -> None:
        await self._append_log(f"Señal {sig_name} recibida. Deteniendo con gracia…")
        await self.action_quit()

    async def _drain_queue(self) -> None:
        """Consume mensajes de la cola: actualiza estado y agrega a TextLog/Log."""
        while True:
            msg = await self.queue.get()
            # Heurística: si es un mensaje "de estado", refleja en la banda superior
            if any(msg.startswith(prefix) for prefix in [
                "Conectando", "Conectado.", "Iniciando", "Grabando", "Deteniendo", "Grabación finalizada",
                "FFmpeg falló.", "FFmpeg salió", "FFmpeg terminó", "Se alcanzó el máximo",
            ]) or msg.startswith("==="):
                self.estado.texto = msg
            await self._append_log(msg)

    async def _append_log(self, line: str) -> None:
        # Evita saturar la UI con líneas ultra largas
        text = line[:2000]
        # TextLog clásico tiene .write(); Log tiene .write_line()
        if hasattr(self.textlog, "write"):
            self.textlog.write(text)               # Textual TextLog
        elif hasattr(self.textlog, "write_line"):
            self.textlog.write_line(text)          # Textual Log (fallback)
        else:
            # Último recurso: intentar update
            try:
                self.textlog.update(getattr(self.textlog, "renderable", "") + "\n" + text)
            except Exception:
                pass

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "toggle":
            await self.action_toggle_log()
        elif event.button.id == "stop":
            await self.action_quit()

    async def action_toggle_log(self) -> None:
        self.log_visible = not self.log_visible
        self.textlog.display = self.log_visible

    async def action_quit(self) -> None:
        # Detener recorder primero
        await self.recorder.stop()
        if self.recorder_task:
            with contextlib.suppress(asyncio.CancelledError):
                await self.recorder_task
        self.exit()

    async def on_unmount(self) -> None:
        # Limpieza
        if self.queue_task:
            self.queue_task.cancel()
        if self.recorder_task:
            self.recorder_task.cancel()


if __name__ == "__main__":
    GrabadorTUI().run()
