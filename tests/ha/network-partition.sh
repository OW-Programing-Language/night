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
# Docker can assign a different IP on network reconnect; restoring the
# original IPv4 endpoint is important to Erlang distribution and Kafka
# peers that can cache each other's addresses during a network partition.
state_dir="${RUNNER_TEMP:-/tmp}/night-live-partition/${project}"
mkdir -p "$state_dir"

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
    original_ip="$(docker inspect "$cid" | jq -r --arg net "$network" '.[0].NetworkSettings.Networks[$net].IPAddress // empty')"
    if [[ ! "$original_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
      echo "Cannot preserve original Docker broker IPv4 for $service" >&2
      exit 1
    fi
    printf '%s\n' "$original_ip" > "$state_dir/$service.ip"
    docker network disconnect --force "$network" "$cid"
  elif [ "$action" = heal ] && [ "$attached" = false ]; then
    ip_file="$state_dir/$service.ip"
    if [ ! -s "$ip_file" ]; then
      echo "No recorded pre-partition broker IPv4 for $service" >&2
      exit 1
    fi
    original_ip="$(cat "$ip_file")"
    # Reattach with the SAME IP and DNS alias. A new arbitrary endpoint
    # can prevent automatic healing because existing broker sessions and
    # Erlang distribution cache the old peer IP.
    docker network connect --ip "$original_ip" --alias "$service" "$network" "$cid"
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
