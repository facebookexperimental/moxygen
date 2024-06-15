/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQSession.h"

int main(int, char*[]) {
  moxygen::MoQSession sess(
      moxygen::MoQCodec::Direction::CLIENT, nullptr, nullptr);
  return 0;
}
