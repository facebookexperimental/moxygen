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
  MoQMiToFlv() = default;
  ~MoQMiToFlv() = default;

  std::list<flv::FlvTag> MoQMiToFlvPayload(MoQMi::MoqMiItem moqMiItem);

 private:
  bool videoHeaderSeen_{false};
  bool audioHeaderSeen_{false};
  bool firstIDRSeen_{false};
};

} // namespace moxygen
