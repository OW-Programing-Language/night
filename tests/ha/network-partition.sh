#!/usr/bin/env bash
# Deliberately sever ONLY Docker networking for living brokers.
# Never use this script against a production Docker project.
set -euo pipefail

if [ "$#" -lt 4 ]; then
  echo "Usage: $0 <isolate|heal> <compose-project> <compose-file> <service> [service ...]" >&2
  exit 64
fi
action="$1"
project="$2"
compose_file="$3"
shift 3

case "$action" in isolate|heal) ;; *) echo "Invalid action: $action" >&2; exit 64 ;; esac
case "$project" in night-kafka-partition|night-rmq-partition) ;;
  *) echo "Refusing to modify unexpected Docker project: $project" >&2; exit 64 ;;
esac
network="${project}_default"
docker network inspect "$network" >/dev/null

for service in "$@"; do
  case "$project:$service" in
    night-kafka-partition:kafka-[123]|night-rmq-partition:rmq-[123]) ;;
    *) echo "Invalid target: $project/$service" >&2; exit 64 ;;
  esac
  cid="$(docker compose -p "$project" -f "$compose_file" ps -q "$service")"
  if [ -z "$cid" ]; then echo "Missing broker container for $service" >&2; exit 1; fi

  running="$(docker inspect -f '{{.State.Running}}' "$cid")"
  if [ "$running" != true ]; then
    echo "$service stopped: not a live network partition" >&2
    exit 1
  fi
  attached="$(docker inspect "$cid" | jq -r --arg net "$network" '.[0].NetworkSettings.Networks | has($net)')"

  if [ "$action" = isolate ] && [ "$attached" = true ]; then
    docker network disconnect --force "$network" "$cid"
  elif [ "$action" = heal ] && [ "$attached" = false ]; then
    # Kafka KRaft voters and RabbitMQ Erlang distribution resolve these
    # well-known names inside the original Compose broker network.
    docker network connect --alias "$service" "$network" "$cid"
  fi

  still_running="$(docker inspect -f '{{.State.Running}}' "$cid")"
  now_attached="$(docker inspect "$cid" | jq -r --arg net "$network" '.[0].NetworkSettings.Networks | has($net)')"
  test "$still_running" = true || { echo "Broker $service stopped during partition!" >&2; exit 1; }
  if [ "$action" = isolate ]; then
    test "$now_attached" = false || { echo "Partition did not sever $service" >&2; exit 1; }
  else
    test "$now_attached" = true || { echo "Healing did not restore $service" >&2; exit 1; }
  fi
  echo "$action confirmed: $service (PID still running, broker-network attached=$now_attached)"
done
