from __future__ import annotations

import json
import logging
import os
import socket
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

DEFAULT_NODE_RUNTIME_PATH = os.environ.get(
    "NEXOR_NODE_CONFIG",
    "/etc/nexor/node_runtime.json",
)


@dataclass
class NexorNodeRuntimeConfig:
    node_id: str = "nexor-01"
    mqtt_namespace: str = "nexor/v1"
    manager_id: str = "serviceManager"

    mqtt_broker: str = "127.0.0.1"
    mqtt_port: int = 1883
    mqtt_user: str = ""
    mqtt_password: str = ""
    mqtt_keepalive: int = 60
    mqtt_reconnect_delay: int = 5

    advertise_host: str = "127.0.0.1"
    hostname: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "NexorNodeRuntimeConfig":
        known = set(cls.__dataclass_fields__.keys())
        return cls(**{k: v for k, v in data.items() if k in known})

    @classmethod
    def load(cls, path: str = DEFAULT_NODE_RUNTIME_PATH) -> "NexorNodeRuntimeConfig":
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            cfg = cls.from_dict(data)
            logger.info(f"Node runtime cargada desde {path}")
            return cfg
        except FileNotFoundError:
            logger.info(f"Node runtime no encontrada en {path} — usando valores por defecto")
            return cls()
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.warning(f"Error parseando node runtime en {path}: {e} — usando valores por defecto")
            return cls()

    @classmethod
    def load_with_env_overrides(cls, path: str = DEFAULT_NODE_RUNTIME_PATH) -> "NexorNodeRuntimeConfig":
        cfg = cls.load(path)
        env_map = {
            "NODE_ID": ("node_id", str),
            "MQTT_NAMESPACE": ("mqtt_namespace", str),
            "MQTT_BROKER": ("mqtt_broker", str),
            "MQTT_PORT": ("mqtt_port", int),
            "MQTT_USER": ("mqtt_user", str),
            "MQTT_PASSWORD": ("mqtt_password", str),
            "MQTT_KEEPALIVE": ("mqtt_keepalive", int),
            "MQTT_RECONNECT_DELAY": ("mqtt_reconnect_delay", int),
            "NEXOR_ADVERTISE_HOST": ("advertise_host", str),
            "NEXOR_HOSTNAME": ("hostname", str),
        }

        delta = {}
        for env_key, (field_name, cast) in env_map.items():
            val = os.environ.get(env_key)
            if val is None:
                continue
            try:
                delta[field_name] = cast(val)
            except (TypeError, ValueError) as e:
                logger.warning(f"Variable de entorno {env_key}={val!r} inválida: {e}")

        if delta:
            cfg = cls.from_dict({**cfg.to_dict(), **delta})

        if not cfg.hostname:
            cfg.hostname = socket.gethostname()
        if not cfg.advertise_host:
            cfg.advertise_host = "127.0.0.1"
        return cfg

    def to_playback_overrides(self) -> dict:
        return {
            "node_id": self.node_id,
            "mqtt_namespace": self.mqtt_namespace,
            "mqtt_broker": self.mqtt_broker,
            "mqtt_port": self.mqtt_port,
            "mqtt_user": self.mqtt_user,
            "mqtt_password": self.mqtt_password,
            "mqtt_keepalive": self.mqtt_keepalive,
            "mqtt_reconnect_delay": self.mqtt_reconnect_delay,
            "advertise_host": self.advertise_host,
        }

    def validate(self) -> list[str]:
        errors: list[str] = []
        if not self.node_id:
            errors.append("node_id vacío")
        if not self.mqtt_namespace:
            errors.append("mqtt_namespace vacío")
        if not self.mqtt_broker:
            errors.append("mqtt_broker vacío")
        if not (1 <= int(self.mqtt_port) <= 65535):
            errors.append(f"mqtt_port inválido: {self.mqtt_port}")
        if not self.advertise_host:
            errors.append("advertise_host vacío")
        return errors
