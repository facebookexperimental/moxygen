/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <proxygen/lib/transport/qmux/QmuxFramer.h>
#include <quic/state/TransportSettings.h>

namespace moxygen {

// Translate a caller-supplied quic::TransportSettings into the QMUX-shaped
// QxTransportParams we'll advertise to the peer.
proxygen::qmux::QxTransportParams qmuxParamsFromTransportSettings(
    const quic::TransportSettings& ts);

} // namespace moxygen
