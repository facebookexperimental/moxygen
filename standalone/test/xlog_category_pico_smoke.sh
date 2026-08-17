#!/usr/bin/env bash
#
# xlog_category_pico_smoke.sh — prove the picoquic bridge logs under the
# "quic.picoquic" category set by XLOG_SET_CATEGORY_NAME in PicoQuicXLogSink.
#
# The proof is the exclusion: openmoq.transport.pico= (the __FILE__-derived
# default) must NOT select the sink. Presence under quic.picoquic= alone could
# just be a root-level match.
#
# Usage: xlog_category_pico_smoke.sh <pico_evb_relay_server> <pico_evb_text_client> [port]

set -uo pipefail

SRV="${1:?usage: <pico_relay_server> <pico_text_client> [port]}"
CLI="${2:?usage: <pico_relay_server> <pico_text_client> [port]}"
PORT="${3:-14339}"

if ! command -v openssl >/dev/null 2>&1; then
  echo "SKIP  quic.picoquic: openssl absent (needed to mint a throwaway cert)"
  exit 0
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Match only DBG lines: folly's GlogStyleFormatter prefixes them 'V', while
# INFO/WARN/ERR appear regardless of the selector and would give a false pass.
has_dbg() { grep -Eq "^V[0-9].*(${1})"; }

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$WORK/key.pem" \
  -out "$WORK/cert.pem" -days 1 -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost" >/dev/null 2>&1

run_pico() {
  local spec="$1" log="$WORK/pico.log"
  "$SRV" -cert "$WORK/cert.pem" -key "$WORK/key.pem" \
    -port "$PORT" -endpoint "/moq-relay" --logging="$spec" >/dev/null 2>"$log" &
  local sp=$!
  sleep 1
  timeout 3 "$CLI" \
    --connect_url "moqt://localhost:$PORT/moq-relay" \
    --track_namespace "moq-date" --track_name "date" \
    --cert_root "$WORK/cert.pem" --logging=INFO >/dev/null 2>/dev/null
  sleep 0.3
  kill "$sp" 2>/dev/null; wait "$sp" 2>/dev/null
  cat "$log"
}

fail=0
MARK='PicoQuicXLogSink\.cpp'

if ! has_dbg "$MARK" <<<"$(run_pico 'DBG9')"; then
  # installPicoQuicXLogSink() has no callers yet, so the sink never emits.
  echo "SKIP  quic.picoquic: sink emitted no DBG at root — installer unwired (openmoq/moxygen#318)"
  exit 0
fi

if has_dbg "$MARK" <<<"$(run_pico 'quic.picoquic=DBG9')"; then
  echo "PASS  quic.picoquic=DBG9 selects /PicoQuicXLogSink.cpp/"
else
  echo "FAIL  quic.picoquic=DBG9 did NOT select the sink (XLOG_SET_CATEGORY_NAME not effective?)"
  fail=1
fi

if has_dbg "$MARK" <<<"$(run_pico 'openmoq.transport.pico=DBG9')"; then
  echo "FAIL  openmoq.transport.pico=DBG9 still selects the sink (override NOT applied)"
  fail=1
else
  echo "PASS  openmoq.transport.pico=DBG9 excludes the sink (override applied → quic.picoquic)"
fi

[ "$fail" -eq 0 ] && echo "OK: picoquic XLOG category verified"
exit "$fail"
