# Audio Playback

Servicio residente que recibe PCM `raw_udp` y lo reproduce por ALSA en la Jetson.

- Configuración de audio: `/etc/nexor/audio-playback.json` (`root:nexor`, `0640`).
- Configuración MQTT común: `/etc/nexor/mqtt.env`, compartida con vídeo, captura y `serviceController`.
- Unidad systemd: `itcl_audio_playback_streaming.service`, con aliases temporales de compatibilidad.
- MQTT: `nexor/v1/nodes/{node_id}/services/audio_playback_streaming`.
- El proceso publica `WAITING_SOURCE` hasta recibir UDP y `RUNNING` al reproducir.

Instalación:

```bash
sudo bash systemd/install.sh
sudoedit /etc/nexor/audio-playback.json
sudo systemctl start itcl_audio_playback_streaming.service
sudo journalctl -u itcl_audio_playback_streaming.service -f
```

El emisor debe usar el mismo puerto, frecuencia, canales y profundidad de bits que
la configuración. Broker y puerto se toman del fichero MQTT común; por MQTT se
controlan volumen, buffer y estado operativo.
