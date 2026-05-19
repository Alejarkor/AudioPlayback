from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Callable, Optional

import gi

gi.require_version("Gst", "1.0")
gi.require_version("GLib", "2.0")
from gi.repository import GLib, Gst  # noqa: E402

logger = logging.getLogger(__name__)
Gst.init(None)


class PipelineState:
    STOPPED = "STOPPED"
    STARTING = "STARTING"
    WAITING_SOURCE = "WAITING_SOURCE"
    RUNNING = "RUNNING"
    IDLE = "IDLE"
    ERROR = "ERROR"


class _FrameAccumulator:
    def __init__(self, frame_bytes: int) -> None:
        self._frame_bytes = int(frame_bytes)
        self._buffer = bytearray()

    def append(self, data: bytes) -> list[bytes]:
        if not data:
            return []
        self._buffer.extend(data)
        frames: list[bytes] = []
        while len(self._buffer) >= self._frame_bytes:
            frames.append(bytes(self._buffer[: self._frame_bytes]))
            del self._buffer[: self._frame_bytes]
        return frames

    def clear(self) -> None:
        self._buffer.clear()


class AudioPlaybackPipeline:
    def __init__(self,
                 on_state_change: Optional[Callable[[str], None]] = None,
                 on_error: Optional[Callable[[str], None]] = None) -> None:
        self._on_state_change = on_state_change or (lambda *_: None)
        self._on_error = on_error or (lambda *_: None)
        self._state = PipelineState.STOPPED
        self._pipeline = None
        self._appsrc = None
        self._volume = None
        self._loop = None
        self._loop_thread = None
        self._lock = threading.Lock()

        self._cfg = None
        self._frame_bytes = 0
        self._frame_duration_s = 0.0
        self._frame_accumulator = None
        self._pending_frames = deque()
        self._buffer_cond = threading.Condition()
        self._pump_running = False
        self._pump_thread = None
        self._change_state(PipelineState.STOPPED)

    @property
    def state(self) -> str:
        return self._state

    def start(self, cfg) -> bool:
        with self._lock:
            self.stop()
            try:
                self._cfg = cfg
                samples_per_channel = int(cfg.sample_rate * cfg.buffer_frame_ms / 1000)
                bytes_per_sample = 3 if cfg.bit_depth == 24 else 2
                self._frame_bytes = samples_per_channel * cfg.channels * bytes_per_sample
                self._frame_duration_s = float(cfg.buffer_frame_ms) / 1000.0
                self._frame_accumulator = _FrameAccumulator(self._frame_bytes)
                self._pending_frames.clear()

                self._ensure_loop()
                self._pipeline = Gst.Pipeline.new("audio-playback")
                self._appsrc = Gst.ElementFactory.make("appsrc", "source")
                queue = Gst.ElementFactory.make("queue", "queue")
                convert = Gst.ElementFactory.make("audioconvert", "convert")
                resample = Gst.ElementFactory.make("audioresample", "resample")
                self._volume = Gst.ElementFactory.make("volume", "vol")
                sink = Gst.ElementFactory.make("alsasink", "sink")

                elements = [self._appsrc, queue, convert, resample, self._volume, sink]
                if any(e is None for e in elements):
                    raise RuntimeError("No se pudieron crear todos los elementos GStreamer")

                caps = Gst.Caps.from_string(
                    f"audio/x-raw,format={cfg.gst_format},rate={cfg.sample_rate},channels={cfg.channels},layout=interleaved"
                )
                self._appsrc.set_property("caps", caps)
                self._appsrc.set_property("format", Gst.Format.TIME)
                self._appsrc.set_property("is-live", True)
                self._appsrc.set_property("block", True)
                self._appsrc.set_property("do-timestamp", True)
                self._appsrc.set_property("emit-signals", False)
                self._appsrc.set_property("stream-type", 0)
                self._appsrc.set_property("max-bytes", self._frame_bytes * max(1, cfg.max_buffered_frames))

                queue.set_property("max-size-time", max(1, cfg.buffer_frame_ms) * 1000000)
                queue.set_property("max-size-bytes", 0)
                queue.set_property("max-size-buffers", max(1, cfg.max_buffered_frames))
                queue.set_property("leaky", 2)  # downstream
                self._volume.set_property("volume", float(cfg.volume))
                sink.set_property("sync", False)
                sink.set_property("async", False)
                if cfg.output_device:
                    sink.set_property("device", cfg.output_device)

                for element in elements:
                    self._pipeline.add(element)

                link_chain = [
                    (self._appsrc, queue, "appsrc->queue"),
                    (queue, convert, "queue->audioconvert"),
                    (convert, resample, "audioconvert->audioresample"),
                    (resample, self._volume, "audioresample->volume"),
                    (self._volume, sink, "volume->alsasink"),
                ]
                for src, dst, name in link_chain:
                    if not src.link(dst):
                        raise RuntimeError(f"No se pudo enlazar la pipeline GStreamer en {name}")

                bus = self._pipeline.get_bus()
                bus.add_signal_watch()
                bus.connect("message", self._on_bus_message)

                self._change_state(PipelineState.STARTING)
                result = self._pipeline.set_state(Gst.State.PLAYING)
                if result == Gst.StateChangeReturn.FAILURE:
                    raise RuntimeError("No se pudo pasar la pipeline a PLAYING")

                self._pump_running = True
                self._pump_thread = threading.Thread(target=self._pump_loop, name="audio-playback-pump", daemon=True)
                self._pump_thread.start()

                self._change_state(PipelineState.WAITING_SOURCE)
                logger.info("Pipeline de playback activa y esperando datos")
                return True
            except Exception as e:
                logger.error(f"Error iniciando pipeline de playback: {e}")
                self._stop_pump_thread()
                self._change_state(PipelineState.ERROR)
                self._on_error(str(e))
                return False

    def stop(self) -> None:
        self._stop_pump_thread()
        if self._pipeline is None:
            self._clear_buffer_internal()
            self._change_state(PipelineState.STOPPED)
            return
        try:
            self._pipeline.set_state(Gst.State.NULL)
        except Exception as e:
            logger.debug(f"Error deteniendo pipeline: {e}")
        self._pipeline = None
        self._appsrc = None
        self._volume = None
        self._clear_buffer_internal()
        self._cfg = None
        self._change_state(PipelineState.STOPPED)

    def restart(self, cfg) -> bool:
        self.stop()
        return self.start(cfg)

    def set_volume(self, volume: float) -> bool:
        if self._volume is None:
            return False
        try:
            self._volume.set_property("volume", float(volume))
            return True
        except Exception as e:
            logger.warning(f"No se pudo aplicar volumen: {e}")
            return False

    def push_packet(self, data: bytes) -> bool:
        if not data or self._appsrc is None or self._frame_accumulator is None:
            return False
        try:
            frames = self._frame_accumulator.append(data)
            if not frames:
                return True
            with self._buffer_cond:
                for frame in frames:
                    self._pending_frames.append(frame)
                self._trim_backlog_locked()
                self._buffer_cond.notify_all()
            return True
        except Exception as e:
            logger.error(f"Error encolando paquete para playback: {e}")
            self._change_state(PipelineState.ERROR)
            self._on_error(str(e))
            return False

    def mark_waiting_source(self) -> None:
        self.clear_buffer(reason="source_timeout")
        if self._state != PipelineState.ERROR:
            self._change_state(PipelineState.WAITING_SOURCE)

    def clear_buffer(self, reason: str = "manual") -> None:
        with self._buffer_cond:
            dropped = len(self._pending_frames)
            self._clear_buffer_internal()
            self._buffer_cond.notify_all()
        logger.info(f"Playback buffer limpiado ({reason}). Frames descartados: {dropped}")

    def _trim_backlog_locked(self) -> None:
        if self._cfg is None:
            return
        queued = len(self._pending_frames)
        if queued >= max(1, self._cfg.hard_reset_buffered_frames):
            latest_frames = list(self._pending_frames)[-max(1, self._cfg.target_buffered_frames):]
            self._pending_frames.clear()
            self._pending_frames.extend(latest_frames)
            logger.warning(
                f"Playback backlog excedido (hard reset). Conservando {len(self._pending_frames)} frames frescos"
            )
            return

        if queued > max(1, self._cfg.max_buffered_frames):
            target = max(1, self._cfg.target_buffered_frames)
            dropped = 0
            while len(self._pending_frames) > target:
                self._pending_frames.popleft()
                dropped += 1
            if dropped > 0:
                logger.warning(f"Playback backlog recortado. Frames descartados: {dropped}")

    def _clear_buffer_internal(self) -> None:
        if self._frame_accumulator is not None:
            self._frame_accumulator.clear()
        self._pending_frames.clear()

    def _pump_loop(self) -> None:
        while self._pump_running:
            frame = None
            with self._buffer_cond:
                if not self._pending_frames and self._pump_running:
                    self._buffer_cond.wait(timeout=0.25)
                if not self._pump_running:
                    break
                if self._pending_frames:
                    frame = self._pending_frames.popleft()

            if frame is None:
                continue

            try:
                buffer = Gst.Buffer.new_allocate(None, len(frame), None)
                buffer.fill(0, frame)
                ret = self._appsrc.emit("push-buffer", buffer)
                if ret != Gst.FlowReturn.OK:
                    raise RuntimeError(f"push-buffer devolvió {ret}")
                if self._state != PipelineState.RUNNING:
                    self._change_state(PipelineState.RUNNING)
            except Exception as e:
                logger.error(f"Error empujando frame a playback pipeline: {e}")
                self._change_state(PipelineState.ERROR)
                self._on_error(str(e))
                break

    def _stop_pump_thread(self) -> None:
        self._pump_running = False
        with self._buffer_cond:
            self._buffer_cond.notify_all()
        if self._pump_thread is not None and self._pump_thread.is_alive():
            self._pump_thread.join(timeout=1.0)
        self._pump_thread = None

    def _ensure_loop(self) -> None:
        if self._loop is not None:
            return
        self._loop = GLib.MainLoop()
        self._loop_thread = threading.Thread(target=self._loop.run, daemon=True)
        self._loop_thread.start()

    def _on_bus_message(self, bus, message) -> None:
        msg_type = message.type
        if msg_type == Gst.MessageType.ERROR:
            err, debug = message.parse_error()
            full = f"{err}: {debug}" if debug else str(err)
            logger.error(f"Bus ERROR: {full}")
            self._change_state(PipelineState.ERROR)
            self._on_error(full)
        elif msg_type == Gst.MessageType.EOS:
            logger.warning("Bus EOS recibido")
            self._change_state(PipelineState.STOPPED)

    def _change_state(self, new_state: str) -> None:
        if self._state == new_state:
            return
        old = self._state
        self._state = new_state
        logger.debug(f"Playback pipeline state: {old} -> {new_state}")
        self._on_state_change(new_state)
