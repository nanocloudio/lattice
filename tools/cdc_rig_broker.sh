#!/usr/bin/env bash
# CDC rig broker (tests/hardware/lattice_pi5_cdc_diag.toml prerequisite 1):
# an MQTT 3.1.1 broker on 0.0.0.0:47090 for the DUT's cdc_mqtt_sink.
# Uses the mosquitto image from the nanocloud registry; anonymous
# access (rig LAN only). `up` starts (idempotent), `down` removes.
set -euo pipefail
NAME=lattice-cdc-broker
case "${1:-up}" in
  up)
    if docker ps --format '{{.Names}}' | grep -q "^${NAME}$"; then
      echo "broker already running"; exit 0
    fi
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    CONF=$(mktemp -d)/mosquitto.conf
    printf 'listener 1883\nallow_anonymous true\n' > "$CONF"
    # Bypass the image's nanocloud entrypoint (it mandates the managed
    # TLS binding envs); the rig broker is plain MQTT on the bench LAN.
    docker run -d --name "$NAME" -p 47090:1883 \
      -v "$CONF":/mosquitto/config/mosquitto.conf:ro \
      --entrypoint /usr/sbin/mosquitto \
      registry.nanocloud.io/mosquitto:latest \
      -c /mosquitto/config/mosquitto.conf >/dev/null
    echo "broker up on :47090"
    ;;
  down)
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    echo "broker removed"
    ;;
  *) echo "usage: $0 up|down" >&2; exit 2 ;;
esac
