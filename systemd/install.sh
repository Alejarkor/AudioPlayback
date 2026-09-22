#!/usr/bin/env bash
set -euo pipefail

app_dir=/opt/nexor/audio-playback
config_dir=/etc/nexor
config_file=$config_dir/audio-playback.json
unit_name=itcl_audio_playback_streaming.service
legacy_unit=audio-playback.service

id -u nexor >/dev/null 2>&1 || useradd --system --create-home --shell /usr/sbin/nologin --groups audio nexor
install -d -o nexor -g audio -m 0755 "$app_dir"
rsync -a --delete --exclude __pycache__ --exclude '*.pyc' ./audio_playback_service/ "$app_dir/audio_playback_service/"
python3 -m venv --system-site-packages "$app_dir/venv"
install -d -o root -g nexor -m 0750 "$config_dir"
if [[ ! -f "$config_file" ]]; then
  install -o root -g nexor -m 0640 config/audio-playback.example.json "$config_file"
fi
chown root:nexor "$config_file"
chmod 0640 "$config_file"
install -o root -g root -m 0644 systemd/itcl_audio_playback_streaming.service "/etc/systemd/system/$unit_name"
if systemctl cat "$legacy_unit" >/dev/null 2>&1; then
  systemctl disable "$legacy_unit" || true
  rm -f "/etc/systemd/system/$legacy_unit"
fi
systemctl daemon-reload
systemctl enable "$unit_name"
systemd-analyze verify "/etc/systemd/system/$unit_name"
echo "Instalación terminada. Revisa $config_file y arranca con: sudo systemctl start $unit_name"
