/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/flv_parser/FlvCommon.h"
#include "moxygen/moq_mi/MoQMi.h"

namespace moxygen {

class MoQMiToFlv {
 public:
  MoQMiToFlv(bool writeHeadersFirst = false)
      : writeHeadersFirst_{writeHeadersFirst} {}
  ~MoQMiToFlv() = default;

  std::list<flv::FlvTag> MoQMiToFlvPayload(MoQMi::MoqMiItem moqMiItem);

 private:
  const uint32_t convertTsToFlv(uint64_t ts, uint64_t timescale);
  void saveAndWriteHeaderIfNeeded(
      std::list<flv::FlvTag>& ret,
      bool isVideoHeader);

  std::unique_ptr<flv::FlvVideoTag> videoHeader_;
  std::unique_ptr<flv::FlvAudioTag> audioHeader_;
  bool headersWritten_{false};

  bool firstIDRSeen_{false};
  bool writeHeadersFirst_{false};
};

} // namespace moxygen
