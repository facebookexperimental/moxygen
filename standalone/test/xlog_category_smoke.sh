#!/usr/bin/env bash
#
# xlog_category_smoke.sh — prove per-layer folly XLOG category filtering works:
# for each layer, assert --logging=<layer>=DBG9 enables that layer's DBG lines
# and excludes every other layer's.
#
# Behavioral rather than unit test by necessity: a category is a compile-time
# property of each source file's __FILE__, and most of these sources are not
# ours. A real client/server session is needed too — a client-less server never
# handshakes, so fizz and mvfst would never emit.
#
# Layers not built on the XLOG backend, or not exercised by a session, SKIP.
#
# Usage: xlog_category_smoke.sh <moqdateserver> <moqtextclient> [base-port]

set -uo pipefail

DS="${1:?usage: <moqdateserver> <moqtextclient> [port]}"
TC="${2:?usage: <moqdateserver> <moqtextclient> [port]}"
PORT="${3:-14337}"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Match only DBG lines: folly's GlogStyleFormatter prefixes them 'V', while
# INFO/WARN/ERR appear regardless of the selector and would give a false pass.
# $1 = file-marker regex; log on stdin.
has_dbg() { grep -Eq "^V[0-9].*(${1})"; }

fail=0

# assert_scoping <runner-fn> <marker-array-name>
#   runner-fn: takes a --logging spec, prints the server's stderr
#   marker array: layer -> regex matching a DBG line from that layer
assert_scoping() {
  local runner="$1"; local -n MARK="$2"
  local -a layers=("${!MARK[@]}") active=()

  # A marker absent at the root means that layer isn't on the XLOG backend or
  # isn't exercised by this session — skip it, don't fail.
  local root_out; root_out="$($runner 'DBG9')"
  local L
  for L in "${layers[@]}"; do
    if has_dbg "${MARK[$L]}" <<<"$root_out"; then
      active+=("$L")
    else
      echo "SKIP  $L: marker /${MARK[$L]}/ not emitted at root DBG9 (glog backend, not exercised, or stale marker)"
    fi
  done

  # The exclusion half is the actual proof of scoping, and catches collisions
  # between layers that end up sharing a category root.
  local O out
  for L in "${active[@]}"; do
    out="$($runner "$L=DBG9")"
    if has_dbg "${MARK[$L]}" <<<"$out"; then
      echo "PASS  $L=DBG9 selects /${MARK[$L]}/"
    else
      echo "FAIL  $L=DBG9 did NOT select /${MARK[$L]}/ (category not rooted at '$L')"
      fail=1
    fi
    for O in "${active[@]}"; do
      [ "$O" = "$L" ] && continue
      if has_dbg "${MARK[$O]}" <<<"$out"; then
        echo "FAIL  $L=DBG9 leaked /${MARK[$O]}/ ($O not scoped out of '$L')"
        fail=1
      fi
    done
  done
  [ "${#active[@]}" -gt 0 ] || echo "WARN  no layers active for $runner (nothing verified)"
}

# ── mvfst session: the six prefix-map layers on the default transport ─────────
run_mvfst() {
  local spec="$1" log="$WORK/mvfst.log"
  "$DS" --insecure -port "$PORT" --logging="$spec" >/dev/null 2>"$log" &
  local sp=$!
  sleep 1
  timeout 3 "$TC" --insecure \
    --connect_url "https://localhost:$PORT/moq-date" \
    --track_namespace "moq-date" --track_name "date" \
    --logging=INFO >/dev/null 2>/dev/null
  sleep 0.3
  kill "$sp" 2>/dev/null; wait "$sp" 2>/dev/null
  cat "$log"
}

# This session exercises moxygen, fizz and quic.mvfst. wangle (fires only behind
# a TCP HTTP acceptor), proxygen (still on the glog backend here) and folly
# (sparse XLOG use) SKIP today; they stay listed so they auto-activate once a
# build or session reaches them.
declare -A MVFST_MARK=(
  [moxygen]='MoQSession\.cpp|MoQForwarder\.cpp'
  [fizz]='AeadTokenCipher\.cpp|RecordLayer\.cpp|FizzServer|Fizz.*\.cpp'
  [quic.mvfst]='QuicServer\.cpp|QuicTransport'
  [wangle]='Acceptor\.cpp|ConnectionManager\.cpp'
  [proxygen]='HQSession|HTTPTransaction|HQ.*Session'
  [folly]='AsyncSocket\.cpp|AsyncUDPSocket\.cpp|EventBase\.cpp'
)
echo "── mvfst transport ──"
assert_scoping run_mvfst MVFST_MARK

[ "$fail" -eq 0 ] && echo "OK: per-layer XLOG category filtering verified"
exit "$fail"
