from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from typing import Optional

from .config import AudioPlaybackConfig
from .mqtt_adapter import AudioPlaybackServiceAdapter
from .node_runtime import NexorNodeRuntimeConfig
from .playback_pipeline import AudioPlaybackPipeline, PipelineState
from .udp_receiver import UdpAudioReceiver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("audio_playback_service")


class AudioPlaybackService:
    def __init__(self, cfg: AudioPlaybackConfig, simulate: bool = False) -> None:
        self._cfg = cfg
        self._simulate = simulate
        self._start_time: Optional[float] = None
        self._last_error: Optional[str] = None
        self._config_path: Optional[str] = None
        self._shutdown_event = threading.Event()
        self._source_active = False
        self._idle = False

        self._pipeline = AudioPlaybackPipeline(
            on_state_change=self._on_pipeline_state_change,
            on_error=self._on_pipeline_error,
        )
        self._receiver = UdpAudioReceiver(
            on_packet=self._on_udp_packet,
            on_first_packet=self._on_first_packet,
        )
        self._mqtt = AudioPlaybackServiceAdapter(
            cfg=cfg,
            on_start=self._handle_start,
            on_resume=self._handle_resume,
            on_standby=self._handle_standby,
            on_stop=self._handle_stop,
            on_restart=self._handle_restart,
            on_clear_buffer=self._handle_clear_buffer,
            on_apply_config=self._handle_apply_config,
            get_state_cb=self._get_state_dict,
        )

        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def set_config_path(self, path: Optional[str]) -> None:
        self._config_path = path

    def run(self) -> int:
        logger.info("=" * 60)
        logger.info(f"  Audio Playback Service — node_id={self._cfg.node_id}")
        logger.info(f"  Listen UDP: {self._cfg.listen_bind_ip}:{self._cfg.listen_port} ({self._cfg.protocol})")
        logger.info(f"  Audio: {self._cfg.sample_rate}Hz {self._cfg.channels}ch {self._cfg.bit_depth}bit")
        logger.info(f"  Output device: {self._cfg.output_device}")
        logger.info(
            f"  Buffer: frame={self._cfg.buffer_frame_ms}ms target={self._cfg.target_buffered_frames} max={self._cfg.max_buffered_frames} hard_reset={self._cfg.hard_reset_buffered_frames}"
        )
        if self._simulate:
            logger.info("  MODO SIMULACIÓN — sin reproducción real")
        logger.info("=" * 60)

        errors = self._cfg.validate()
        if errors:
            for e in errors:
                logger.error(f"Config inválida: {e}")
            return 1

        if not self._mqtt.start():
            logger.warning("No se pudo conectar a MQTT — continuando sin control remoto")

        self._mqtt.publish_state("STARTING", healthy=False)
        self._mqtt.publish_event("service_starting", details={"simulate": self._simulate})
        self._mqtt.publish_capabilities()
        self._mqtt.publish_config_reported(self._cfg)
        self._mqtt.publish_endpoint(self._cfg)

        if not self._start_runtime(trigger="startup"):
            self._mqtt.stop()
            return 1

        logger.info("Servicio corriendo. Esperando shutdown...")
        try:
            while not self._shutdown_event.is_set():
                self._shutdown_event.wait(timeout=1.0)
                self._tick_source_timeout()
                if self._mqtt.is_connected():
                    self._mqtt.publish_state(
                        status=self._runtime_status(),
                        healthy=self._runtime_healthy(),
                        pid=os.getpid(),
                        uptime_s=self._uptime_seconds(),
                        last_error=self._last_error if self._runtime_status() == PipelineState.ERROR else None,
                    )
        except Exception as e:
            logger.error(f"Error inesperado en bucle principal: {e}")
        return self._shutdown()

    def _shutdown(self) -> int:
        logger.info("Iniciando shutdown gracioso...")
        self._mqtt.publish_state("STOPPING", healthy=False)
        self._mqtt.publish_event("service_stopping")
        self._receiver.stop()
        if not self._simulate:
            self._pipeline.stop()
        self._mqtt.stop()
        logger.info("Servicio detenido.")
        return 0

    def _signal_handler(self, signum, frame) -> None:
        logger.info(f"Señal recibida: {signal.Signals(signum).name}")
        self._shutdown_event.set()

    def _start_runtime(self, trigger: str) -> bool:
        if not self._receiver.start(self._cfg.listen_bind_ip, self._cfg.listen_port):
            self._last_error = "UDP receiver start failed"
            self._mqtt.publish_state("ERROR", healthy=False, last_error=self._last_error)
            self._mqtt.publish_event("receiver_start_failed", severity="error", details={"trigger": trigger})
            return False

        if not self._simulate and not self._pipeline.start(self._cfg):
            self._receiver.stop()
            self._last_error = "Playback pipeline start failed"
            self._mqtt.publish_state("ERROR", healthy=False, last_error=self._last_error)
            self._mqtt.publish_event("pipeline_start_failed", severity="error", details={"trigger": trigger})
            return False

        self._start_time = time.monotonic()
        self._source_active = False
        self._idle = False
        self._mqtt.publish_state("WAITING_SOURCE", healthy=True, pid=os.getpid())
        self._mqtt.publish_event("waiting_for_source", details={"trigger": trigger, "listen_port": self._cfg.listen_port})
        return True

    def _stop_runtime(self, reason: str = "stop") -> None:
        self._receiver.stop()
        if not self._simulate:
            self._pipeline.clear_buffer(reason=reason)
            self._pipeline.stop()
        self._source_active = False
        self._start_time = None

    def _handle_start(self) -> None:
        self._handle_resume()

    def _handle_resume(self) -> None:
        if self._idle or self._runtime_status() == "STOPPED":
            logger.info("Reanudando servicio de playback")
            self._start_runtime(trigger="resume_command")

    def _handle_standby(self) -> None:
        logger.info("Pasando servicio a IDLE")
        self._stop_runtime(reason="standby")
        self._idle = True
        self._mqtt.publish_state("IDLE", healthy=True, pid=os.getpid())
        self._mqtt.publish_event("service_standby", details={"trigger": "mqtt_command"})

    def _handle_stop(self) -> None:
        logger.info("Parando servicio")
        self._stop_runtime(reason="stop_command")
        self._idle = False
        self._mqtt.publish_state("STOPPED", healthy=False, pid=os.getpid())
        self._mqtt.publish_event("service_stopped", details={"trigger": "mqtt_command"})

    def _handle_restart(self) -> None:
        logger.info("Reiniciando servicio")
        self._stop_runtime(reason="restart_command")
        self._start_runtime(trigger="restart_command")
        self._mqtt.publish_event("service_restarted", details={"trigger": "mqtt_command", "pid": os.getpid()})

    def _handle_clear_buffer(self) -> None:
        if self._simulate:
            return
        self._pipeline.clear_buffer(reason="mqtt_command")
        self._mqtt.publish_event("buffer_cleared", details={"trigger": "mqtt_command"})

    def _handle_apply_config(self, delta: dict) -> None:
        logger.info(f"Aplicando config delta: {delta}")
        hot_fields = {"volume"}
        try:
            new_cfg = self._cfg.apply_delta(delta)
        except (TypeError, ValueError) as e:
            logger.error(f"Delta inválido: {e}")
            self._mqtt.publish_event("config_apply_failed", severity="error", details={"error": str(e), "delta": delta})
            return

        errors = new_cfg.validate()
        if errors:
            logger.error(f"Config resultante inválida: {errors}")
            self._mqtt.publish_event("config_apply_failed", severity="error", details={"errors": errors, "delta": delta})
            return

        hot_changes = {k: v for k, v in delta.items() if k in hot_fields}
        cold_changes = {k: v for k, v in delta.items() if k not in hot_fields}

        self._cfg = new_cfg
        self._mqtt._cfg = new_cfg

        if "volume" in hot_changes and not self._simulate:
            self._pipeline.set_volume(self._cfg.volume)

        if self._config_path:
            self._cfg.save(self._config_path)
        else:
            self._cfg.save()

        if cold_changes and self._runtime_status() not in ("IDLE", "STOPPED"):
            logger.info(f"Reiniciando runtime por cambio de config: {list(cold_changes.keys())}")
            self._stop_runtime(reason="config_change")
            if not self._start_runtime(trigger="config_applied"):
                return
        elif self._idle:
            self._mqtt.publish_state("IDLE", healthy=True, pid=os.getpid())
        elif self._runtime_status() == "STOPPED":
            self._mqtt.publish_state("STOPPED", healthy=False, pid=os.getpid())

        self._mqtt.publish_config_reported(self._cfg)
        self._mqtt.publish_endpoint(self._cfg)
        self._mqtt.publish_event("config_applied", details={"delta": delta})

    def _on_udp_packet(self, data: bytes) -> None:
        if self._simulate:
            return
        if self._idle:
            return
        ok = self._pipeline.push_packet(data)
        if not ok:
            self._last_error = "push_packet_failed"

    def _on_first_packet(self) -> None:
        self._source_active = True
        self._mqtt.publish_state("RUNNING", healthy=True, pid=os.getpid(), uptime_s=self._uptime_seconds())
        self._mqtt.publish_event("source_active", details={"packet_count": self._receiver.packet_count})

    def _tick_source_timeout(self) -> None:
        if self._idle or self._runtime_status() == "STOPPED":
            return
        if self._source_active and self._receiver.has_timed_out(self._cfg.inactivity_timeout_s):
            self._source_active = False
            if not self._simulate:
                if self._cfg.clear_buffer_on_timeout:
                    self._pipeline.clear_buffer(reason="source_timeout")
                self._pipeline.mark_waiting_source()
            self._mqtt.publish_state("WAITING_SOURCE", healthy=True, pid=os.getpid(), uptime_s=self._uptime_seconds())
            self._mqtt.publish_event("source_timeout", severity="warning", details={"timeout_s": self._cfg.inactivity_timeout_s})

    def _runtime_status(self) -> str:
        if self._idle:
            return "IDLE"
        if self._start_time is None:
            return "STOPPED"
        if not self._source_active:
            return "WAITING_SOURCE"
        if self._simulate:
            return "RUNNING"
        return self._pipeline.state if self._pipeline.state != PipelineState.WAITING_SOURCE else "WAITING_SOURCE"

    def _runtime_healthy(self) -> bool:
        return self._runtime_status() in ("WAITING_SOURCE", "RUNNING", "IDLE")

    def _on_pipeline_state_change(self, new_state: str) -> None:
        logger.debug(f"Pipeline state -> {new_state}")

    def _on_pipeline_error(self, msg: str) -> None:
        self._last_error = msg
        self._mqtt.publish_event("pipeline_error", severity="error", details={"message": msg})
        self._mqtt.publish_state("ERROR", healthy=False, pid=os.getpid(), last_error=msg)

    def _get_state_dict(self) -> dict:
        return {
            "status": self._runtime_status(),
            "healthy": self._runtime_healthy(),
            "pid": os.getpid(),
            "uptime_s": self._uptime_seconds(),
            "last_error": self._last_error,
        }

    def _uptime_seconds(self) -> Optional[int]:
        if self._start_time is None:
            return None
        return int(time.monotonic() - self._start_time)


def main() -> int:
    parser = argparse.ArgumentParser(description="Servicio de recepción y reproducción de audio (Nexor)")
    parser.add_argument("--config", "-c", default=None, help="Ruta al fichero de configuración JSON")
    parser.add_argument("--simulate", "-s", action="store_true", help="Modo simulación")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Nivel de log")
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))

    config_path = args.config
    if config_path:
        cfg = AudioPlaybackConfig.load(config_path)
    else:
        cfg = AudioPlaybackConfig.load()

    node_runtime = NexorNodeRuntimeConfig.load_with_env_overrides()
    runtime_errors = node_runtime.validate()
    if runtime_errors:
        for err in runtime_errors:
            logger.warning(f"Node runtime inválida: {err}")
    cfg = cfg.apply_common_overrides(node_runtime.to_playback_overrides())

    service = AudioPlaybackService(cfg, simulate=args.simulate)
    service.set_config_path(config_path)
    return service.run()


if __name__ == "__main__":
    sys.exit(main())
