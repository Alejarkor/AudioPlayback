from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = os.environ.get(
    "AUDIO_PLAYBACK_CONFIG",
    "/etc/nexor/audio_playback.json",
)

PUBLIC_REPORTED_CONFIG_EXCLUDE_KEYS = {
    "mqtt_user",
    "mqtt_password",
}


@dataclass
class AudioPlaybackConfig:
    protocol: str = "raw_udp"
    listen_bind_ip: str = "0.0.0.0"
    listen_port: int = 1236

    sample_rate: int = 48000
    channels: int = 2
    bit_depth: int = 16
    volume: float = 1.0

    output_device_name: str = ""
    output_device_override: str = ""
    output_device_resolved: str = "default"

    inactivity_timeout_s: float = 2.0

    mqtt_broker: str = "127.0.0.1"
    mqtt_port: int = 1883
    mqtt_user: str = ""
    mqtt_password: str = ""
    mqtt_keepalive: int = 60
    mqtt_reconnect_delay: int = 5
    node_id: str = "nexor-01"
    mqtt_namespace: str = "nexor/v1"
    advertise_host: str = "127.0.0.1"

    def to_dict(self) -> dict:
        return asdict(self)

    def to_report_dict(self) -> dict:
        data = self.to_dict()
        for key in PUBLIC_REPORTED_CONFIG_EXCLUDE_KEYS:
            data.pop(key, None)
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "AudioPlaybackConfig":
        normalized = dict(data)
        legacy_output_device = normalized.pop("output_device", None)
        if legacy_output_device is not None and "output_device_override" not in normalized and "output_device_name" not in normalized:
            normalized["output_device_override"] = legacy_output_device

        known = set(cls.__dataclass_fields__.keys())
        return cls(**{k: v for k, v in normalized.items() if k in known})

    @classmethod
    def load(cls, path: str = DEFAULT_CONFIG_PATH) -> "AudioPlaybackConfig":
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            cfg = cls.from_dict(data)
            logger.info(f"Config cargada desde {path}")
            return cfg
        except FileNotFoundError:
            logger.info(f"Config no encontrada en {path} — usando valores por defecto")
            return cls()
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.warning(f"Error parseando config en {path}: {e} — usando valores por defecto")
            return cls()

    def apply_common_overrides(self, overrides: dict) -> "AudioPlaybackConfig":
        current = self.to_dict()
        current.update(overrides)
        return self.from_dict(current)

    def apply_delta(self, delta: dict) -> "AudioPlaybackConfig":
        current = self.to_dict()
        current.update(delta)
        return self.from_dict(current)

    def save(self, path: str = DEFAULT_CONFIG_PATH) -> None:
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)
        os.replace(tmp, path)
        logger.info(f"Config guardada en {path}")

    @property
    def gst_format(self) -> str:
        return "S24LE" if self.bit_depth == 24 else "S16LE"

    @property
    def effective_output_device(self) -> str:
        return self.output_device_resolved or self.output_device_override or "default"

    @property
    def mqtt_base_topic(self) -> str:
        return f"{self.mqtt_namespace}/nodes/{self.node_id}/services/audio_playback"

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
        if self.protocol not in ("raw_udp",):
            errors.append(f"protocol inválido: {self.protocol}")
        if not self.listen_bind_ip:
            errors.append("listen_bind_ip vacío")
        if not (1 <= int(self.listen_port) <= 65535):
            errors.append(f"listen_port inválido: {self.listen_port}")
        if self.sample_rate not in (8000, 16000, 32000, 44100, 48000, 96000):
            errors.append(f"sample_rate inusual: {self.sample_rate}")
        if self.channels not in (1, 2):
            errors.append(f"channels inválido: {self.channels}")
        if self.bit_depth not in (16, 24):
            errors.append(f"bit_depth inválido: {self.bit_depth}")
        if not (0.0 <= float(self.volume) <= 4.0):
            errors.append(f"volume fuera de rango: {self.volume}")
        if not (0.1 <= float(self.inactivity_timeout_s) <= 60.0):
            errors.append(f"inactivity_timeout_s fuera de rango: {self.inactivity_timeout_s}")
        if not self.node_id:
            errors.append("node_id vacío")
        if not self.mqtt_namespace:
            errors.append("mqtt_namespace vacío")
        if not (self.output_device_name or self.output_device_override or self.output_device_resolved):
            errors.append("No se ha definido ningún selector de dispositivo de salida")
        return errors
