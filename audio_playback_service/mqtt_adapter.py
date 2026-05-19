from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Optional

logger = logging.getLogger(__name__)

SERVICE_NAME = "audio_playback"

CAPABILITIES = {
    "service": SERVICE_NAME,
    "actions": [
        "start",
        "resume",
        "standby",
        "stop",
        "restart",
        "get_state",
        "apply_config",
        "clear_buffer",
    ],
    "config_schema": {
        "protocol": {"type": "enum", "values": ["raw_udp"]},
        "listen_bind_ip": {"type": "string"},
        "listen_port": {"type": "integer", "min": 1, "max": 65535},
        "sample_rate": {"type": "enum", "values": [44100, 48000]},
        "channels": {"type": "integer", "min": 1, "max": 2},
        "bit_depth": {"type": "enum", "values": [16, 24]},
        "volume": {"type": "float", "min": 0.0, "max": 4.0},
        "output_device": {"type": "string"},
        "inactivity_timeout_s": {"type": "float", "min": 0.1, "max": 60.0},
        "buffer_frame_ms": {"type": "enum", "values": [5, 10, 20]},
        "target_buffered_frames": {"type": "integer", "min": 1, "max": 50},
        "max_buffered_frames": {"type": "integer", "min": 1, "max": 100},
        "hard_reset_buffered_frames": {"type": "integer", "min": 1, "max": 200},
        "clear_buffer_on_timeout": {"type": "boolean"}
    },
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class AudioPlaybackServiceAdapter:
    def __init__(self, cfg,
                 on_start: Optional[Callable] = None,
                 on_resume: Optional[Callable] = None,
                 on_standby: Optional[Callable] = None,
                 on_stop: Optional[Callable] = None,
                 on_restart: Optional[Callable] = None,
                 on_clear_buffer: Optional[Callable[[], None]] = None,
                 on_apply_config: Optional[Callable[[dict], None]] = None,
                 get_state_cb: Optional[Callable[[], dict]] = None) -> None:
        self._cfg = cfg
        self._client = None
        self._connected = False
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._on_start = on_start or (lambda: None)
        self._on_resume = on_resume or self._on_start
        self._on_standby = on_standby or (lambda: None)
        self._on_stop = on_stop or (lambda: None)
        self._on_restart = on_restart or (lambda: None)
        self._on_clear_buffer = on_clear_buffer or (lambda: None)
        self._on_apply_config = on_apply_config or (lambda d: None)
        self._get_state_cb = get_state_cb or (lambda: {})

    def start(self) -> bool:
        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            logger.error("paho-mqtt no instalado. Ejecutar: pip install paho-mqtt")
            return False

        cfg = self._cfg
        client_id = f"audio-playback-{cfg.node_id}"
        client = mqtt.Client(client_id=client_id, clean_session=True)
        if cfg.mqtt_user:
            client.username_pw_set(cfg.mqtt_user, cfg.mqtt_password)

        lwt_payload = json.dumps({
            "service": SERVICE_NAME,
            "status": "OFFLINE",
            "healthy": False,
            "pid": os.getpid(),
            "ts": _now_iso(),
        })
        client.will_set(cfg.mqtt_state_topic, payload=lwt_payload, qos=1, retain=True)

        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        self._client = client

        try:
            client.connect(cfg.mqtt_broker, cfg.mqtt_port, cfg.mqtt_keepalive)
        except (OSError, ConnectionRefusedError) as e:
            logger.error(f"No se puede conectar al broker MQTT {cfg.mqtt_broker}:{cfg.mqtt_port}: {e}")
            self._client = None
            return False
        except Exception as e:
            logger.error(f"Error MQTT inesperado: {e}")
            self._client = None
            return False

        client.loop_start()
        logger.info(f"MQTT adapter iniciado -> {cfg.mqtt_broker}:{cfg.mqtt_port}")
        return True

    def stop(self) -> None:
        self._stop_event.set()
        if self._client:
            self.publish_state("STOPPED", healthy=False)
            time.sleep(0.2)
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception as e:
                logger.debug(f"MQTT disconnect: {e}")
        logger.info("MQTT adapter detenido")

    def is_connected(self) -> bool:
        return self._connected

    def publish_state(self, status: str, healthy: bool = True,
                      pid: Optional[int] = None, uptime_s: Optional[int] = None,
                      last_error: Optional[str] = None) -> None:
        payload = {
            "service": SERVICE_NAME,
            "status": status,
            "healthy": healthy,
            "transport": self._cfg.protocol,
            "volume": self._cfg.volume,
            "buffer_frame_ms": self._cfg.buffer_frame_ms,
            "queued_target_frames": self._cfg.target_buffered_frames,
            "queued_max_frames": self._cfg.max_buffered_frames,
            "ts": _now_iso(),
        }
        if pid is not None:
            payload["pid"] = pid
        if uptime_s is not None:
            payload["uptime_s"] = uptime_s
        if last_error is not None:
            payload["last_error"] = last_error
        self._publish(self._cfg.mqtt_state_topic, payload, qos=1, retain=True)

    def publish_config_reported(self, cfg) -> None:
        payload = {"service": SERVICE_NAME, "config": cfg.to_report_dict(), "ts": _now_iso()}
        self._publish(self._cfg.mqtt_config_reported_topic, payload, qos=1, retain=True)

    def publish_capabilities(self) -> None:
        caps = dict(CAPABILITIES)
        caps["ts"] = _now_iso()
        self._publish(self._cfg.mqtt_capabilities_topic, caps, qos=1, retain=True)

    def publish_endpoint(self, cfg) -> None:
        payload = {
            "service": SERVICE_NAME,
            "transport": cfg.protocol,
            "direction": "inbound_listen",
            "host": cfg.effective_listen_host,
            "port": cfg.listen_port,
            "sample_rate": cfg.sample_rate,
            "channels": cfg.channels,
            "bit_depth": cfg.bit_depth,
            "output_device": cfg.output_device,
            "buffer_frame_ms": cfg.buffer_frame_ms,
            "pid": os.getpid(),
            "ts": _now_iso(),
        }
        self._publish(self._cfg.mqtt_endpoint_topic, payload, qos=1, retain=True)

    def publish_event(self, event: str, severity: str = "info", details: Optional[dict] = None, qos: int = 1) -> None:
        payload = {
            "severity": severity,
            "event": event,
            "service": SERVICE_NAME,
            "details": details or {},
            "pid": os.getpid(),
            "ts": _now_iso(),
        }
        self._publish(self._cfg.mqtt_events_topic, payload, qos=qos)

    def _on_connect(self, client, userdata, flags, rc) -> None:
        if rc == 0:
            self._connected = True
            logger.info(f"MQTT conectado a {self._cfg.mqtt_broker}:{self._cfg.mqtt_port}")
            client.subscribe(self._cfg.mqtt_cmd_topic, qos=1)
            client.subscribe(self._cfg.mqtt_config_desired_topic, qos=1)
            self.publish_capabilities()
            self.publish_config_reported(self._cfg)
            self.publish_endpoint(self._cfg)
        else:
            logger.error(f"MQTT conexión rechazada: rc={rc}")

    def _on_disconnect(self, client, userdata, rc) -> None:
        self._connected = False
        if rc != 0 and not self._stop_event.is_set():
            logger.warning(f"MQTT desconectado inesperadamente (rc={rc})")
        else:
            logger.info("MQTT desconectado")

    def _on_message(self, client, userdata, message) -> None:
        topic = message.topic
        try:
            payload = json.loads(message.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Mensaje MQTT malformado en {topic}: {e}")
            return

        if topic == self._cfg.mqtt_cmd_topic:
            self._handle_command(payload)
        elif topic == self._cfg.mqtt_config_desired_topic:
            self._handle_desired_config(payload)
        else:
            logger.debug(f"Mensaje en topic no manejado: {topic}")

    def _handle_command(self, payload: dict) -> None:
        action = payload.get("action", "").lower()
        params = payload.get("params", {})
        msg_id = payload.get("msg_id", "unknown")
        logger.info(f"Comando recibido: action={action} msg_id={msg_id}")

        handlers = {
            "start": self._cmd_start,
            "resume": self._cmd_resume,
            "standby": self._cmd_standby,
            "stop": self._cmd_stop,
            "restart": self._cmd_restart,
            "get_state": self._cmd_get_state,
            "apply_config": self._cmd_apply_config,
            "clear_buffer": self._cmd_clear_buffer,
        }
        handler = handlers.get(action)
        if handler is None:
            self.publish_event("unknown_action", severity="warning", details={"action": action, "msg_id": msg_id})
            return
        try:
            handler(params, msg_id)
        except Exception as e:
            logger.error(f"Error ejecutando acción '{action}': {e}")
            self.publish_event("command_error", severity="error", details={"action": action, "error": str(e), "msg_id": msg_id})

    def _cmd_start(self, params: dict, msg_id: str) -> None:
        self.publish_event("start_requested", details={"msg_id": msg_id})
        self._on_start()

    def _cmd_resume(self, params: dict, msg_id: str) -> None:
        self.publish_event("resume_requested", details={"msg_id": msg_id})
        self._on_resume()

    def _cmd_standby(self, params: dict, msg_id: str) -> None:
        self.publish_event("standby_requested", details={"msg_id": msg_id})
        self._on_standby()

    def _cmd_stop(self, params: dict, msg_id: str) -> None:
        self.publish_event("stop_requested", details={"msg_id": msg_id})
        self._on_stop()

    def _cmd_restart(self, params: dict, msg_id: str) -> None:
        self.publish_event("restart_requested", details={"msg_id": msg_id})
        self._on_restart()

    def _cmd_get_state(self, params: dict, msg_id: str) -> None:
        state = self._get_state_cb()
        self.publish_state(
            status=state.get("status", "UNKNOWN"),
            healthy=state.get("healthy", False),
            pid=state.get("pid"),
            uptime_s=state.get("uptime_s"),
            last_error=state.get("last_error"),
        )
        self.publish_endpoint(self._cfg)

    def _cmd_apply_config(self, params: dict, msg_id: str) -> None:
        if not params:
            logger.warning("apply_config recibido sin parámetros")
            return
        self.publish_event("config_applying", details={"delta": params, "msg_id": msg_id})
        self._on_apply_config(params)

    def _cmd_clear_buffer(self, params: dict, msg_id: str) -> None:
        self.publish_event("clear_buffer_requested", details={"msg_id": msg_id})
        self._on_clear_buffer()

    def _handle_desired_config(self, payload: dict) -> None:
        config_delta = payload.get("config", {})
        if not config_delta:
            return
        logger.info(f"Config desired recibida: {config_delta}")
        self._on_apply_config(config_delta)

    def _publish(self, topic: str, payload: dict, qos: int = 0, retain: bool = False) -> None:
        if not self._client:
            logger.debug(f"_publish ignorado (sin cliente MQTT): {topic}")
            return
        try:
            self._client.publish(topic, payload=json.dumps(payload), qos=qos, retain=retain)
            logger.debug(f"-> MQTT [{topic}] {json.dumps(payload)[:160]}")
        except Exception as e:
            logger.warning(f"Error publicando en {topic}: {e}")
