from __future__ import annotations

import logging
import threading
from typing import Callable, Optional

import gi

from .shared_imports import ensure_audio_shared_path

ensure_audio_shared_path()

from audio_shared.pcm import PcmAudioFormat, PcmFrameAccumulator, apply_gain_pcm16, downmix_pcm16_to_mono
from audio_shared.reference_bus import AudioReferenceBus

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
        self._ref_bus = None
        self._ref_accumulator = None
        self._ref_gain = 1.0
        self._change_state(PipelineState.STOPPED)

    @property
    def state(self) -> str:
        return self._state

    def start(self, cfg) -> bool:
        with self._lock:
            self.stop()
            try:
                self._cfg = cfg
                self._ref_gain = float(cfg.volume)
                if cfg.reference_bus_enabled and cfg.bit_depth == 16:
                    bus_format = PcmAudioFormat(
                        sample_rate=cfg.sample_rate,
                        channels=1,
                        bit_depth=16,
                        frame_ms=cfg.reference_frame_ms,
                    )
                    source_format = PcmAudioFormat(
                        sample_rate=cfg.sample_rate,
                        channels=cfg.channels,
                        bit_depth=16,
                        frame_ms=cfg.reference_frame_ms,
                    )
                    self._ref_bus = AudioReferenceBus(
                        path=cfg.reference_bus_path,
                        audio_format=bus_format,
                        capacity_frames=64,
                    )
                    self._ref_accumulator = PcmFrameAccumulator(source_format.frame_bytes)
                else:
                    self._ref_bus = None
                    self._ref_accumulator = None

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

                queue.set_property("max-size-time", 1000000)
                queue.set_property("max-size-bytes", 0)
                queue.set_property("max-size-buffers", 0)
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

                self._change_state(PipelineState.WAITING_SOURCE)
                logger.info("Pipeline de playback activa y esperando datos")
                return True
            except Exception as e:
                logger.error(f"Error iniciando pipeline de playback: {e}")
                self._close_reference_bus()
                self._change_state(PipelineState.ERROR)
                self._on_error(str(e))
                return False

    def stop(self) -> None:
        if self._pipeline is None:
            self._change_state(PipelineState.STOPPED)
            self._close_reference_bus()
            return
        try:
            self._pipeline.set_state(Gst.State.NULL)
        except Exception as e:
            logger.debug(f"Error deteniendo pipeline: {e}")
        self._pipeline = None
        self._appsrc = None
        self._volume = None
        self._close_reference_bus()
        self._change_state(PipelineState.STOPPED)

    def restart(self, cfg) -> bool:
        self.stop()
        return self.start(cfg)

    def set_volume(self, volume: float) -> bool:
        if self._volume is None:
            return False
        try:
            self._volume.set_property("volume", float(volume))
            self._ref_gain = float(volume)
            return True
        except Exception as e:
            logger.warning(f"No se pudo aplicar volumen: {e}")
            return False

    def push_packet(self, data: bytes) -> bool:
        if not data or self._appsrc is None:
            return False
        try:
            if self._ref_bus is not None and self._ref_accumulator is not None and self._cfg and self._cfg.bit_depth == 16:
                self._export_reference_frames(data)
            buffer = Gst.Buffer.new_allocate(None, len(data), None)
            buffer.fill(0, data)
            ret = self._appsrc.emit("push-buffer", buffer)
            if ret != Gst.FlowReturn.OK:
                raise RuntimeError(f"push-buffer devolvió {ret}")
            if self._state != PipelineState.RUNNING:
                self._change_state(PipelineState.RUNNING)
            return True
        except Exception as e:
            logger.error(f"Error empujando paquete a playback pipeline: {e}")
            self._change_state(PipelineState.ERROR)
            self._on_error(str(e))
            return False

    def mark_waiting_source(self) -> None:
        if self._state != PipelineState.ERROR:
            self._change_state(PipelineState.WAITING_SOURCE)

    def _export_reference_frames(self, data: bytes) -> None:
        frame_chunks = self._ref_accumulator.append(data)
        for chunk in frame_chunks:
            gained = apply_gain_pcm16(chunk, self._ref_gain, channels=self._cfg.channels)
            mono = downmix_pcm16_to_mono(gained, channels=self._cfg.channels)
            self._ref_bus.write_frame(mono)

    def _close_reference_bus(self) -> None:
        try:
            if self._ref_bus is not None:
                self._ref_bus.close()
        except Exception:
            pass
        self._ref_bus = None
        if self._ref_accumulator is not None:
            self._ref_accumulator.clear()
        self._ref_accumulator = None
        self._cfg = None

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
