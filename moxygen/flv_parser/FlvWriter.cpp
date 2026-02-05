/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/flv_parser/FlvWriter.h"
#include <netinet/in.h>

namespace moxygen::flv {

bool FlvWriter::writeTag(FlvTag tag) {
  auto buf = FlvCoder::writeTag(std::move(tag));
  if (!buf) {
    return false;
  }

  buf->coalesce();
  f_.write(reinterpret_cast<const char*>(buf->data()), buf->length());
  f_.flush();

  return true;
}

} // namespace moxygen::flv
