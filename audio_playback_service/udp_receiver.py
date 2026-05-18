from __future__ import annotations

import logging
import socket
import threading
import time
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class UdpAudioReceiver:
    def __init__(self,
                 on_packet: Callable[[bytes], None],
                 on_first_packet: Optional[Callable[[], None]] = None) -> None:
        self._on_packet = on_packet
        self._on_first_packet = on_first_packet or (lambda: None)
        self._sock = None
        self._thread = None
        self._running = False
        self._received_first_packet = False
        self._last_packet_ts = None
        self._packet_count = 0
        self._lock = threading.Lock()

    def start(self, bind_ip: str, port: int) -> bool:
        self.stop()
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._sock.bind((bind_ip, int(port)))
            self._sock.settimeout(1.0)
            self._running = True
            self._received_first_packet = False
            self._last_packet_ts = None
            self._packet_count = 0
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
            logger.info(f"UDP receiver escuchando en {bind_ip}:{port}")
            return True
        except Exception as e:
            logger.error(f"No se pudo iniciar UDP receiver en {bind_ip}:{port}: {e}")
            self._running = False
            try:
                if self._sock is not None:
                    self._sock.close()
            except Exception:
                pass
            self._sock = None
            return False

    def stop(self) -> None:
        self._running = False
        try:
            if self._sock is not None:
                self._sock.close()
        except Exception:
            pass
        self._sock = None
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=1.0)
        self._thread = None

    def has_timed_out(self, timeout_s: float) -> bool:
        if self._last_packet_ts is None:
            return True
        return (time.monotonic() - self._last_packet_ts) > float(timeout_s)

    @property
    def packet_count(self) -> int:
        return self._packet_count

    def _run(self) -> None:
        while self._running:
            try:
                data, _addr = self._sock.recvfrom(65535)
                if not data:
                    continue
                self._last_packet_ts = time.monotonic()
                self._packet_count += 1
                if not self._received_first_packet:
                    self._received_first_packet = True
                    logger.info("Primer paquete UDP recibido")
                    self._on_first_packet()
                self._on_packet(data)
            except socket.timeout:
                continue
            except OSError:
                if self._running:
                    logger.warning("UDP receiver interrumpido por cierre de socket")
                break
            except Exception as e:
                logger.error(f"Error en UDP receiver: {e}")
                break
