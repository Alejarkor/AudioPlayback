from __future__ import annotations

import os
import sys
from pathlib import Path


def ensure_audio_shared_path() -> None:
    env_path = os.environ.get("NEXOR_AUDIO_SHARED_PATH")
    candidates = []
    if env_path:
        candidates.append(Path(env_path))

    current = Path(__file__).resolve()
    candidates.extend([
        current.parents[2] / "AudioBinStream",
        current.parents[3] / "AudioBinStream",
        current.parents[2] / "AudioBinaural",
        current.parents[3] / "AudioBinaural",
    ])

    for candidate in candidates:
        if not candidate:
            continue
        if (candidate / "audio_shared").exists():
            candidate_str = str(candidate)
            if candidate_str not in sys.path:
                sys.path.insert(0, candidate_str)
            return
