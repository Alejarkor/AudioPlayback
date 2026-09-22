from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, replace
from pathlib import Path

DEFAULT_CONFIG_PATH = "/etc/nexor/audio-playback.json"
_SECRET_FIELDS = {"mqtt_user", "mqtt_password"}
_STATIC_FIELDS = {
    "schema_version", "node_id", "mqtt_namespace", "mqtt_broker", "mqtt_port",
    "mqtt_user", "mqtt_password", "mqtt_keepalive", "mqtt_tls_enabled", "mqtt_tls_ca_file", "listen_bind_ip", "listen_port",
    "advertise_host",
}


@dataclass(frozen=True)
class AudioPlaybackConfig:
    schema_version: int = 1
    node_id: str = "nexor-01"
    mqtt_namespace: str = "nexor/v1"
    mqtt_broker: str = "127.0.0.1"
    mqtt_port: int = 1883
    mqtt_user: str = ""
    mqtt_password: str = ""
    mqtt_keepalive: int = 60
    mqtt_reconnect_min_s: int = 1
    mqtt_reconnect_max_s: int = 30
    mqtt_tls_enabled: bool = False
    mqtt_tls_ca_file: str | None = None
    advertise_host: str = "127.0.0.1"
    protocol: str = "raw_udp"
    listen_bind_ip: str = "0.0.0.0"
    listen_port: int = 1236
    sample_rate: int = 48000
    channels: int = 1
    bit_depth: int = 16
    volume: float = 1.0
    output_device: str = "default"
    inactivity_timeout_s: float = 2.0
    buffer_frame_ms: int = 10
    target_buffered_frames: int = 3
    max_buffered_frames: int = 6
    hard_reset_buffered_frames: int = 10
    clear_buffer_on_timeout: bool = True

    @classmethod
    def load(cls, path: str = DEFAULT_CONFIG_PATH) -> "AudioPlaybackConfig":
        config_path = Path(path)
        if not config_path.is_file():
            raise ValueError(f"No existe el archivo de configuración: {config_path}")
        try:
            data = json.loads(config_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise ValueError(f"JSON de configuración inválido: {error}") from error
        if not isinstance(data, dict):
            raise ValueError("La configuración debe ser un objeto JSON")
        known = set(cls.__dataclass_fields__)
        normalized = dict(data)
        if "mqtt_reconnect_delay" in normalized:
            normalized.setdefault("mqtt_reconnect_min_s", normalized["mqtt_reconnect_delay"])
            normalized.setdefault("mqtt_reconnect_max_s", max(30, int(normalized["mqtt_reconnect_delay"])))
        cfg = cls(**{key: value for key, value in normalized.items() if key in known}).with_shared_mqtt_environment()
        errors = cfg.validate()
        if errors:
            raise ValueError("Configuración inválida: " + "; ".join(errors))
        return cfg

    def with_shared_mqtt_environment(self) -> "AudioPlaybackConfig":
        """Obtiene la conexión MQTT únicamente de /etc/nexor/mqtt.env."""
        def required(name: str) -> str:
            value = os.environ.get(name, "").strip()
            if not value:
                raise ValueError(f"Falta {name} en /etc/nexor/mqtt.env")
            return value

        def optional_int(name: str, default: int) -> int:
            value = os.environ.get(name)
            return default if value in (None, "") else int(value)

        def optional_bool(name: str, default: bool) -> bool:
            value = os.environ.get(name)
            if value in (None, ""):
                return default
            if value.lower() in {"1", "true", "yes"}:
                return True
            if value.lower() in {"0", "false", "no"}:
                return False
            raise ValueError(f"{name} debe ser true o false")

        try:
            return replace(
                self,
                node_id=required("NEXOR_NODE_ID"),
                mqtt_namespace=required("NEXOR_MQTT_NAMESPACE"),
                mqtt_broker=required("NEXOR_MQTT_HOST"),
                mqtt_port=int(required("NEXOR_MQTT_PORT")),
                mqtt_user=os.environ.get("NEXOR_MQTT_USERNAME", ""),
                mqtt_password=os.environ.get("NEXOR_MQTT_PASSWORD", ""),
                mqtt_keepalive=optional_int("NEXOR_MQTT_KEEPALIVE_SECONDS", self.mqtt_keepalive),
                mqtt_reconnect_min_s=optional_int("NEXOR_MQTT_RECONNECT_MIN_SECONDS", self.mqtt_reconnect_min_s),
                mqtt_reconnect_max_s=optional_int("NEXOR_MQTT_RECONNECT_MAX_SECONDS", self.mqtt_reconnect_max_s),
                mqtt_tls_enabled=optional_bool("NEXOR_MQTT_TLS_ENABLED", self.mqtt_tls_enabled),
                mqtt_tls_ca_file=os.environ.get("NEXOR_MQTT_TLS_CA_FILE") or None,
            )
        except ValueError as exc:
            raise ValueError(f"Configuración MQTT compartida inválida: {exc}") from exc

    def to_dict(self) -> dict:
        return asdict(self)

    def to_report_dict(self) -> dict:
        return {key: value for key, value in self.to_dict().items() if key not in _SECRET_FIELDS}

    def apply_runtime_delta(self, delta: dict) -> "AudioPlaybackConfig":
        if not isinstance(delta, dict):
            raise ValueError("La actualización debe ser un objeto JSON")
        rejected = sorted(set(delta) & _STATIC_FIELDS)
        if rejected:
            raise ValueError("No se pueden cambiar en ejecución: " + ", ".join(rejected))
        unknown = sorted(set(delta) - set(self.__dataclass_fields__))
        if unknown:
            raise ValueError("Campos desconocidos: " + ", ".join(unknown))
        cfg = replace(self, **delta)
        errors = cfg.validate()
        if errors:
            raise ValueError("Configuración dinámica inválida: " + "; ".join(errors))
        return cfg

    @property
    def gst_format(self) -> str:
        return "S24LE" if self.bit_depth == 24 else "S16LE"

    @property
    def mqtt_base_topic(self) -> str:
        return f"{self.mqtt_namespace}/nodes/{self.node_id}/services/audio_playback_streaming"

    @property
    def mqtt_cmd_topic(self) -> str:
        return f"{self.mqtt_base_topic}/cmd"

    @property
    def mqtt_state_topic(self) -> str:
        return f"{self.mqtt_base_topic}/state"

    @property
    def mqtt_events_topic(self) -> str:
        return f"{self.mqtt_base_topic}/events"

    @property
    def mqtt_capabilities_topic(self) -> str:
        return f"{self.mqtt_base_topic}/capabilities"

    @property
    def mqtt_config_desired_topic(self) -> str:
        return f"{self.mqtt_base_topic}/config/desired"

    @property
    def mqtt_config_reported_topic(self) -> str:
        return f"{self.mqtt_base_topic}/config/reported"

    @property
    def mqtt_endpoint_topic(self) -> str:
        return f"{self.mqtt_base_topic}/endpoint"

    @property
    def effective_listen_host(self) -> str:
        return self.advertise_host

    def validate(self) -> list[str]:
        errors: list[str] = []
        if self.schema_version != 1:
            errors.append("schema_version no soportada")
        if not self.node_id or not self.mqtt_namespace or not self.mqtt_broker:
            errors.append("node_id, mqtt_namespace y mqtt_broker son obligatorios")
        if any(char.isspace() for char in self.mqtt_broker) or "#" in self.mqtt_broker:
            errors.append("mqtt_broker no puede contener espacios ni comentarios")
        if not 1 <= int(self.mqtt_port) <= 65535 or not 1 <= int(self.listen_port) <= 65535:
            errors.append("puerto inválido")
        if self.protocol != "raw_udp":
            errors.append("protocol debe ser raw_udp")
        if self.sample_rate not in (8000, 16000, 32000, 44100, 48000, 96000):
            errors.append("sample_rate inválido")
        if self.channels not in (1, 2) or self.bit_depth not in (16, 24):
            errors.append("formato de audio inválido")
        if not 0 <= float(self.volume) <= 4:
            errors.append("volume fuera de rango")
        if self.buffer_frame_ms not in (5, 10, 20):
            errors.append("buffer_frame_ms inválido")
        if self.target_buffered_frames < 1 or self.max_buffered_frames < self.target_buffered_frames:
            errors.append("límites de buffer inválidos")
        if self.hard_reset_buffered_frames < self.max_buffered_frames:
            errors.append("hard_reset_buffered_frames inválido")
        if not self.output_device:
            errors.append("output_device vacío")
        return errors
