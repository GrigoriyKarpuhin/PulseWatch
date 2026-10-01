#!/usr/bin/env sh
set -eu

curl -fsS -X POST http://localhost:8080/monitors \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo target","url":"http://demo-target:8000/","interval_seconds":10}'
printf '\nWait 25 seconds, then run:\n'
printf '  docker compose stop demo-target\n'
printf '  sleep 25; curl -s http://localhost:8080/incidents\n'
printf '  docker compose start demo-target\n'
printf '  sleep 15; curl -s http://localhost:8080/incidents\n'
printf '  curl -s http://localhost:8080/deliveries\n'
printf '  curl -s http://localhost:8081/events\n'
