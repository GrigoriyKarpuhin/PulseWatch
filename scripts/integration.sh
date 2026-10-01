#!/usr/bin/env sh
set -eu

if [ "${SKIP_BUILD:-0}" = "1" ]; then
  docker compose up -d --remove-orphans
else
  docker compose up --build -d --remove-orphans
fi
docker compose start demo-target
trap 'docker compose start demo-target >/dev/null 2>&1 || true' EXIT

attempt=0
until curl -fsS http://localhost:8080/healthz >/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    docker compose logs --tail=100
    exit 1
  fi
  sleep 2
done

monitor_id=$(curl -fsS -X POST http://localhost:8080/monitors \
  -H 'Content-Type: application/json' \
  -d '{"name":"integration target","url":"http://demo-target:8000/","interval_seconds":10}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
export MONITOR_ID="$monitor_id"

tcp_id=$(curl -fsS -X POST http://localhost:8080/monitors \
  -H 'Content-Type: application/json' \
  -d '{"name":"integration tcp","kind":"tcp","url":"tcp://postgres:5432","interval_seconds":10,"timeout_ms":1000,"failure_threshold":3}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
export TCP_ID="$tcp_id"

attempt=0
until curl -fsS "http://localhost:8080/monitors/$monitor_id/checks" \
  | python3 -c 'import json,sys; sys.exit(not any(x["success"] for x in json.load(sys.stdin)))'; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then exit 1; fi
  sleep 2
done

attempt=0
until curl -fsS "http://localhost:8080/monitors/$tcp_id/checks" \
  | python3 -c 'import json,sys; sys.exit(not any(x["kind"] == "tcp" and x["success"] for x in json.load(sys.stdin)))'; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then exit 1; fi
  sleep 2
done

docker compose stop demo-target
attempt=0
until curl -fsS http://localhost:8080/incidents \
  | python3 -c 'import json,sys,os; sys.exit(not any(x["monitor_id"] == int(os.environ["MONITOR_ID"]) and x["resolved_at"] is None for x in json.load(sys.stdin)))'; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then exit 1; fi
  sleep 2
done

attempt=0
until curl -fsS http://localhost:8080/deliveries \
  | python3 -c 'import json,sys,os; sys.exit(not any(x["monitor_id"] == int(os.environ["MONITOR_ID"]) and x["event_type"] == "incident.opened" and x["delivered_at"] is not None for x in json.load(sys.stdin)))'; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then exit 1; fi
  sleep 2
done

docker compose start demo-target
attempt=0
until curl -fsS http://localhost:8080/incidents \
  | python3 -c 'import json,sys,os; sys.exit(not any(x["monitor_id"] == int(os.environ["MONITOR_ID"]) and x["resolved_at"] is not None for x in json.load(sys.stdin)))'; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then exit 1; fi
  sleep 2
done

curl -fsS "http://localhost:8080/monitors/$monitor_id/stats" \
  | python3 -c 'import json,sys; x=json.load(sys.stdin); sys.exit(not (x["checks"] >= 3 and x["successful"] >= 1))'

curl -fsS -X PATCH "http://localhost:8080/monitors/$monitor_id/enabled" \
  -H 'Content-Type: application/json' -d '{"enabled":false}' \
  | python3 -c 'import json,sys; sys.exit(json.load(sys.stdin)["enabled"])'
curl -fsS -X PATCH "http://localhost:8080/monitors/$monitor_id/enabled" \
  -H 'Content-Type: application/json' -d '{"enabled":true}' \
  | python3 -c 'import json,sys; sys.exit(not json.load(sys.stdin)["enabled"])'

echo "integration test passed for HTTP monitor $monitor_id and TCP monitor $tcp_id"
