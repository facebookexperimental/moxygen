/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

// Forward-decl to keep this header light. The C type comes from picoquic.h.
extern "C" {
typedef struct st_picoquic_quic_t picoquic_quic_t;
}

namespace moxygen::openmoq::pico {

/**
 * Install a folly XLOG sink on the given picoquic context. After this call,
 * picoquic's internal log events (per-packet, per-cnx lifecycle, drops, losses,
 * CC dumps, ALPN negotiation, etc.) are dispatched through folly's LoggerDB
 * and governed by the standard --logging=... config string under the
 * `quic.picoquic.*` category root.
 *
 * Coexists with picoquic's builtin loggers (picoquic_set_textlog,
 * picoquic_set_binlog, picoquic_set_qlog). picoquic dispatches each event to
 * every registered logger in its log-functions table; the XLog sink takes one
 * of three slots and runs in parallel with whichever builtins the application
 * also enabled. Typical moqx usage enables only the XLog sink and skips the
 * builtins entirely.
 *
 * The sink is registered with a static struct that has process-lifetime
 * storage, so this function may be called multiple times safely (subsequent
 * calls are no-ops once the slot is taken).
 *
 * Severity mapping summary:
 *   INFO    — app_message hooks (deliberate app-level logs)
 *   DBG1    — connection lifecycle, ALPN, drops, losses
 *   DBG2    — transport params, TLS ticket, CC dump
 *   DBG3    — per-packet (pdu in/out, decrypted packet, outgoing packet)
 *
 * Operators target the bridge via the standard XLOG config:
 *   --logging=quic.picoquic=INFO         // lifecycle + app msgs
 *   --logging=quic.picoquic=DBG1         // adds drops/losses
 *   --logging=quic.picoquic=DBG3         // adds per-packet firehose
 *
 * @param quic  the picoquic context to install the sink on. Must be non-null.
 */
void installPicoQuicXLogSink(picoquic_quic_t* quic);

} // namespace moxygen::openmoq::pico
