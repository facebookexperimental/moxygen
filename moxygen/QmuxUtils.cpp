/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/QmuxUtils.h>

#include <quic/QuicConstants.h>

namespace moxygen {

proxygen::qmux::QxTransportParams qmuxParamsFromTransportSettings(
    const quic::TransportSettings& ts) {
  proxygen::qmux::QxTransportParams params{
      .maxIdleTimeout = static_cast<uint64_t>(ts.idleTimeout.count()),
      .initialMaxData = ts.advertisedInitialConnectionFlowControlWindow,
      .initialMaxStreamDataBidiLocal =
          ts.advertisedInitialBidiLocalStreamFlowControlWindow,
      .initialMaxStreamDataBidiRemote =
          ts.advertisedInitialBidiRemoteStreamFlowControlWindow,
      .initialMaxStreamDataUni = ts.advertisedInitialUniStreamFlowControlWindow,
      .initialMaxStreamsBidi = ts.advertisedInitialMaxStreamsBidi,
      .initialMaxStreamsUni = ts.advertisedInitialMaxStreamsUni,
  };
  if (ts.datagramConfig.enabled) {
    params.maxDatagramFrameSize = quic::kMaxDatagramFrameSize;
  }
  return params;
}

} // namespace moxygen
