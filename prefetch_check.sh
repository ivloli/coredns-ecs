#!/usr/bin/env bash
set -euo pipefail

# Usage example:
#   ./prefetch_check.sh
#   DNS_SERVER=127.0.0.1 DNS_PORT=445 DOMAIN=www.baidu.com ECS_SUBNET=183.224.33.196 ./prefetch_check.sh

DNS_SERVER="${DNS_SERVER:-127.0.0.1}"
DNS_PORT="${DNS_PORT:-445}"
DOMAIN="${DOMAIN:-www.baidu.com}"
ECS_SUBNET="${ECS_SUBNET:-183.224.33.196}"
PREFETCH_AHEAD_SEC="${PREFETCH_AHEAD_SEC:-5}"
BURST_COUNT="${BURST_COUNT:-12}"
SLOW_THRESHOLD_MS="${SLOW_THRESHOLD_MS:-80}"

dig_query() {
  # Output format: "<query_time_ms> <ttl>"
  local output qtime ttl
  output="$(dig @"${DNS_SERVER}" -p "${DNS_PORT}" "${DOMAIN}" +https +subnet="${ECS_SUBNET}" +tries=1 +time=2 +noquestion +answer +stats || true)"

  if printf '%s\n' "${output}" | awk '/connection refused|no servers could be reached|timed out/ {found=1} END {exit(found?0:1)}'; then
    printf 'DIG_ERROR %s\n' "$(printf '%s\n' "${output}" | tr '\n' ' ' | awk '{print substr($0,1,220)}')"
    return
  fi

  qtime="$(printf '%s\n' "${output}" | awk '/^;; Query time:/ {print $4; exit}')"
  ttl="$(printf '%s\n' "${output}" | awk 'NF >= 2 && $1 !~ /^;;/ {print $2; exit}')"

  if [[ -z "${qtime}" ]]; then
    qtime="-1"
  fi
  if [[ -z "${ttl}" ]]; then
    ttl="0"
  fi

  printf '%s %s\n' "${qtime}" "${ttl}"
}

echo "== Prefetch check start =="
echo "target: @${DNS_SERVER}:${DNS_PORT} ${DOMAIN} +https +subnet=${ECS_SUBNET}"
echo "prefetch_ahead: ${PREFETCH_AHEAD_SEC}s"
echo

echo "[1/5] Cold query (create/update cache)"
read -r cold_ms ttl <<< "$(dig_query)"
if [[ "${cold_ms}" == "DIG_ERROR" ]]; then
  echo "dig failed: ${ttl}"
  exit 1
fi
echo "cold query_time=${cold_ms}ms ttl=${ttl}s"
if [[ "${cold_ms}" -lt 0 ]]; then
  echo "dig failed: no query time parsed"
  exit 1
fi

if [[ "${ttl}" -le 0 ]]; then
  echo "warning: ttl parse failed; fallback ttl=30s"
  ttl=30
fi

echo
echo "[2/5] Warm cache quickly"
for i in 1 2 3; do
  read -r ms _ <<< "$(dig_query)"
  if [[ "${ms}" == "DIG_ERROR" ]]; then
    echo "warm #${i}: dig failed"
    exit 1
  fi
  echo "warm #${i}: ${ms}ms"
done

wait_sec=$(( ttl - PREFETCH_AHEAD_SEC ))
if [[ "${wait_sec}" -lt 1 ]]; then
  wait_sec=1
fi

echo
echo "[3/5] Sleep ${wait_sec}s to enter near-expiry window"
sleep "${wait_sec}"

echo
echo "[4/5] Trigger async prefetch with one request"
read -r trigger_ms _ <<< "$(dig_query)"
if [[ "${trigger_ms}" == "DIG_ERROR" ]]; then
  echo "trigger query: dig failed"
  exit 1
fi
echo "trigger query_time=${trigger_ms}ms (should still be cache-hit latency)"

echo
echo "[5/5] Burst queries during prefetch window"
slow=0
sum=0
for ((i=1; i<=BURST_COUNT; i++)); do
  read -r ms _ <<< "$(dig_query)"
  if [[ "${ms}" == "DIG_ERROR" ]]; then
    echo "burst #${i}: dig failed"
    exit 1
  fi
  echo "burst #${i}: ${ms}ms"
  if [[ "${ms}" -ge 0 ]]; then
    sum=$((sum + ms))
    if [[ "${ms}" -gt "${SLOW_THRESHOLD_MS}" ]]; then
      slow=$((slow + 1))
    fi
  fi
  sleep 0.2
done

avg=$((sum / BURST_COUNT))
echo
echo "== Result =="
echo "avg=${avg}ms, slow(>${SLOW_THRESHOLD_MS}ms)=${slow}/${BURST_COUNT}"
echo "If most burst queries stay low, async prefetch is working (request path not blocked by refresh)."
echo
echo "Optional log check (CoreDNS logs):"
echo "  ristretto cache prefetch trigger"
echo "  ristretto cache prefetch write"
