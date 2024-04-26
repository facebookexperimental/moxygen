#pragma once

#include <folly/logging/xlog.h>
#include "moxygen/MoQFramer.h"

namespace moxygen {

struct AbsoluteLocation {
  uint64_t group;
  uint64_t object;

  std::strong_ordering operator<=>(const AbsoluteLocation& other) const {
    if (group < other.group) {
      return std::strong_ordering::less;
    } else if (group == other.group) {
      if (object < other.object) {
        return std::strong_ordering::less;
      } else if (object == other.object) {
        return std::strong_ordering::equivalent;
      } else {
        return std::strong_ordering::greater;
      }
    } else {
      return std::strong_ordering::greater;
    }
  }
};

inline AbsoluteLocation toAbsolulte(
    const Location& group,
    const Location& object,
    const uint64_t curGroup,
    const uint64_t curObj,
    std::function<uint64_t(uint64_t)> largestObjectForGroup = [](uint64_t) {
      return 0;
    }) {
  XLOG(DBG1) << "gm=" << uint64_t(group.locType) << " g=" << group.value
             << " om=" << uint64_t(object.locType) << " o=" << object.value;
  AbsoluteLocation result;
  switch (group.locType) {
    case LocationType::None:
      result.group = std::numeric_limits<uint64_t>::max();
      break;
    case LocationType::Absolute:
      result.group = group.value;
      break;
    case LocationType::RelativePrevious:
      if (group.value > curGroup) {
        result.group = 0;
      } else {
        result.group = curGroup - group.value;
      }
      break;
    case LocationType::RelativeNext:
      result.group = curGroup + group.value + 1;
      break;
  }
  switch (object.locType) {
    case LocationType::None:
      result.object = std::numeric_limits<uint64_t>::max();
      break;
    case LocationType::Absolute:
      result.object = object.value;
      break;
    case LocationType::RelativePrevious: {
      auto tailObj = curObj;
      if (result.group != curGroup) {
        tailObj = largestObjectForGroup(result.group);
      }
      if (object.value < tailObj) {
        result.object = 0;
      } else {
        result.object = tailObj - object.value;
      }
      break;
    }
    case LocationType::RelativeNext: {
      auto tailObj = 0;
      if (result.object == curGroup) {
        tailObj = curObj;
      }
      result.object = tailObj + object.value;
      break;
    }
  }
  XLOG(DBG1) << "g=" << result.group << " o=" << result.object;
  return result;
}
} // namespace moxygen
