from __future__ import annotations

import logging
import subprocess
from typing import Optional

logger = logging.getLogger(__name__)

PREFERRED_PLAYBACK_PREFIXES = (
    "plughw:",
    "sysdefault:",
    "front:",
    "default",
    "hw:",
)


def list_alsa_playback_devices() -> list[dict]:
    try:
        proc = subprocess.run(
            ["aplay", "-L"],
            capture_output=True,
            text=True,
            check=True,
        )
    except Exception as e:
        logger.warning(f"No se pudo ejecutar 'aplay -L': {e}")
        return []

    devices: list[dict] = []
    current_id: Optional[str] = None
    description_lines: list[str] = []

    def flush_current() -> None:
        nonlocal current_id, description_lines
        if not current_id:
            return
        description = " ".join(line.strip() for line in description_lines if line.strip())
        devices.append({
            "alsa_id": current_id,
            "description": description,
        })
        current_id = None
        description_lines = []

    for raw_line in proc.stdout.splitlines():
        line = raw_line.rstrip("\n")
        if not line.strip():
            continue
        if not raw_line.startswith((" ", "\t")):
            flush_current()
            current_id = line.strip()
            description_lines = []
        else:
            description_lines.append(line)

    flush_current()
    return devices


def find_alsa_playback_device_by_name(name: str) -> Optional[str]:
    if not name:
        return None

    needle = name.lower().strip()
    matches: list[dict] = []
    for dev in list_alsa_playback_devices():
        haystack = f"{dev['alsa_id']} {dev['description']}".lower()
        if needle in haystack:
            matches.append(dev)

    if not matches:
        return None

    def score(dev: dict) -> tuple[int, int]:
        alsa_id = dev["alsa_id"].lower()
        prefix_rank = len(PREFERRED_PLAYBACK_PREFIXES)
        for idx, prefix in enumerate(PREFERRED_PLAYBACK_PREFIXES):
            if alsa_id.startswith(prefix):
                prefix_rank = idx
                break
        desc_len = len(dev.get("description", ""))
        return (prefix_rank, -desc_len)

    matches.sort(key=score)
    chosen = matches[0]["alsa_id"]
    logger.info(f"Dispositivo ALSA de reproducción resuelto: '{name}' -> {chosen}")
    return chosen
