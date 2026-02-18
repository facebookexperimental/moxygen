/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <fmt/core.h>

#include <string>
#include <type_traits>

namespace moxygen {

struct MoQPublishError {
  // Do not add additional codes unless you know what you are doing
  enum Code {
    API_ERROR = 1,        // Semantic error (APIs called out of order)
    WRITE_ERROR = 2,      // The underlying write failed
    CANCELLED = 3,        // The subgroup was reset (implicitly by the error)
    TOO_FAR_BEHIND = 5,   // Subscriber exceeded buffer limit (subscribe only)
    BLOCKED = 4,          // Consumer cannot accept more data (fetch only),
                          //  or out of stream credit (subscribe and fetch)
    MALFORMED_TRACK = 12, // Track violated protocol ordering constraints
  };
  // Do not add additional codes unless you know what you are doing

  Code code;
  std::string msg;

  explicit MoQPublishError(Code inCode) : code(inCode) {}

  MoQPublishError(Code inCode, std::string inMsg)
      : code(inCode), msg(std::move(inMsg)) {}

  std::string describe() const {
    return fmt::format(
        "error={} msg={}",
        static_cast<std::underlying_type_t<Code>>(code),
        msg);
  }

  const char* what() const noexcept {
    return msg.c_str();
  }
};

enum class ObjectPublishStatus { IN_PROGRESS, DONE };

} // namespace moxygen
