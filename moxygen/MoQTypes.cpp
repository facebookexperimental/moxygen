/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQTypes.h>

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <folly/logging/xlog.h>

namespace {

const char* getFrameTypeString(moxygen::FrameType type) {
  switch (type) {
    case moxygen::FrameType::LEGACY_CLIENT_SETUP:
      return "LEGACY_CLIENT_SETUP";
    case moxygen::FrameType::LEGACY_SERVER_SETUP:
      return "LEGACY_SERVER_SETUP";
    case moxygen::FrameType::SUBSCRIBE:
      return "SUBSCRIBE";
    case moxygen::FrameType::SUBSCRIBE_OK:
      return "SUBSCRIBE_OK";
    case moxygen::FrameType::SUBSCRIBE_ERROR:
      return "SUBSCRIBE_ERROR";
    case moxygen::FrameType::SUBSCRIBE_DONE:
      return "SUBSCRIBE_DONE";
    case moxygen::FrameType::MAX_REQUEST_ID:
      return "MAX_REQUEST_ID";
    case moxygen::FrameType::UNSUBSCRIBE:
      return "UNSUBSCRIBE";
    case moxygen::FrameType::ANNOUNCE:
      return "ANNOUNCE";
    case moxygen::FrameType::ANNOUNCE_OK:
      return "ANNOUNCE_OK";
    case moxygen::FrameType::ANNOUNCE_ERROR:
      return "ANNOUNCE_ERROR";
    case moxygen::FrameType::UNANNOUNCE:
      return "UNANNOUNCE";
    case moxygen::FrameType::GOAWAY:
      return "GOAWAY";
    default:
      // can happen when type was cast from uint8_t
      return "Unknown";
  }
  LOG(FATAL) << "Unreachable";
}

const char* getStreamTypeString(moxygen::StreamType type) {
  switch (type) {
    case moxygen::StreamType::SUBGROUP_HEADER_SG:
    case moxygen::StreamType::SUBGROUP_HEADER_SG_EXT:
    case moxygen::StreamType::SUBGROUP_HEADER_SG_FIRST:
    case moxygen::StreamType::SUBGROUP_HEADER_SG_FIRST_EXT:
    case moxygen::StreamType::SUBGROUP_HEADER_SG_ZERO:
    case moxygen::StreamType::SUBGROUP_HEADER_SG_ZERO_EXT:
      return "SUBGROUP_HEADER";
    case moxygen::StreamType::FETCH_HEADER:
      return "FETCH_HEADER";
    default:
      // can happen when type was cast from uint8_t
      return "Unknown";
  }
  LOG(FATAL) << "Unreachable";
}

const char* getObjectStatusString(moxygen::ObjectStatus objectStatus) {
  switch (objectStatus) {
    case moxygen::ObjectStatus::NORMAL:
      return "NORMAL";
    case moxygen::ObjectStatus::OBJECT_NOT_EXIST:
      return "OBJECT_NOT_EXIST";
    case moxygen::ObjectStatus::GROUP_NOT_EXIST:
      return "GROUP_NOT_EXIST";
    case moxygen::ObjectStatus::END_OF_GROUP:
      return "END_OF_GROUP";
    case moxygen::ObjectStatus::END_OF_TRACK:
      return "END_OF_TRACK";
    default:
      // can happen when type was cast from uint8_t
      return "Unknown";
  }
  LOG(FATAL) << "Unreachable";
}

} // namespace

namespace moxygen {

std::string toString(LocationType loctype) {
  switch (loctype) {
    case LocationType::NextGroupStart: {
      return "NextGroupStart";
    }
    case LocationType::AbsoluteStart: {
      return "AbsoluteStart";
    }
    case LocationType::LargestObject: {
      return "LargestObject";
    }
    case LocationType::AbsoluteRange: {
      return "AbsoluteRange";
    }
    case LocationType::LargestGroup: {
      return "LargestGroup";
    }
    default: {
      return "Unknown";
    }
  }
}

std::ostream& operator<<(std::ostream& os, FrameType type) {
  os << getFrameTypeString(type);
  return os;
}

std::ostream& operator<<(std::ostream& os, StreamType type) {
  os << getStreamTypeString(type);
  return os;
}

std::ostream& operator<<(std::ostream& os, ObjectStatus status) {
  os << getObjectStatusString(status);
  return os;
}

std::ostream& operator<<(std::ostream& os, TrackAlias alias) {
  os << alias.value;
  return os;
}

std::ostream& operator<<(std::ostream& os, RequestID id) {
  os << id.value;
  return os;
}

std::ostream& operator<<(std::ostream& os, const ObjectHeader& header) {
  os << " group=" << header.group << " subgroup=" << header.subgroup
     << " id=" << header.id << " priority="
     << (header.priority.has_value()
             ? std::to_string(uint32_t(header.priority.value()))
             : "none")
     << " status=" << getObjectStatusString(header.status) << " length="
     << (header.length.has_value() ? std::to_string(header.length.value())
                                   : "none");
  return os;
}

//// Parameters ////

// Frame type sets for parameter allowlist
const folly::F14FastSet<FrameType> kAllowedFramesForAuthToken = {
    FrameType::PUBLISH,
    FrameType::SUBSCRIBE,
    FrameType::SUBSCRIBE_UPDATE,
    FrameType::SUBSCRIBE_ANNOUNCES,
    FrameType::ANNOUNCE,
    FrameType::TRACK_STATUS,
    FrameType::FETCH};

const folly::F14FastSet<FrameType> kAllowedFramesForDeliveryTimeout = {
    FrameType::PUBLISH_OK,
    FrameType::SUBSCRIBE,
    FrameType::SUBSCRIBE_UPDATE};

const folly::F14FastSet<FrameType> kAllowedFramesForSubscriberPriority = {
    FrameType::SUBSCRIBE,
    FrameType::FETCH,
    FrameType::SUBSCRIBE_UPDATE,
    FrameType::PUBLISH_OK};

const folly::F14FastSet<FrameType> kAllowedFramesForSubscriptionFilter = {
    FrameType::SUBSCRIBE,
    FrameType::PUBLISH_OK,
    FrameType::SUBSCRIBE_UPDATE};

const folly::F14FastSet<FrameType> kAllowedFramesForExpires = {
    FrameType::SUBSCRIBE_OK,
    FrameType::PUBLISH,
    FrameType::PUBLISH_OK,
    FrameType::REQUEST_OK};

const folly::F14FastSet<FrameType> kAllowedFramesForGroupOrder = {
    FrameType::SUBSCRIBE,
    FrameType::PUBLISH_OK,
    FrameType::FETCH};

const folly::F14FastSet<FrameType> kAllowedFramesForLargestObject = {
    FrameType::SUBSCRIBE_OK,
    FrameType::PUBLISH,
    FrameType::REQUEST_OK};

const folly::F14FastSet<FrameType> kAllowedFramesForForward = {
    FrameType::SUBSCRIBE,
    FrameType::SUBSCRIBE_UPDATE,
    FrameType::PUBLISH,
    FrameType::PUBLISH_OK,
    FrameType::SUBSCRIBE_ANNOUNCES};

// Allowlist mapping: TrackRequestParamKey -> set of allowed FrameTypes
// Empty set means allowed for all frame types
const folly::F14FastMap<TrackRequestParamKey, folly::F14FastSet<FrameType>>
    kParamAllowlist = {
        {TrackRequestParamKey::AUTHORIZATION_TOKEN, kAllowedFramesForAuthToken},
        {TrackRequestParamKey::DELIVERY_TIMEOUT,
         kAllowedFramesForDeliveryTimeout},
        {TrackRequestParamKey::MAX_CACHE_DURATION, {}},
        {TrackRequestParamKey::PUBLISHER_PRIORITY, {}},
        {TrackRequestParamKey::SUBSCRIBER_PRIORITY,
         kAllowedFramesForSubscriberPriority},
        {TrackRequestParamKey::SUBSCRIPTION_FILTER,
         kAllowedFramesForSubscriptionFilter},
        {TrackRequestParamKey::EXPIRES, kAllowedFramesForExpires},
        {TrackRequestParamKey::GROUP_ORDER, kAllowedFramesForGroupOrder},
        {TrackRequestParamKey::LARGEST_OBJECT, kAllowedFramesForLargestObject},
        {TrackRequestParamKey::FORWARD, kAllowedFramesForForward},
};

// Frame types that allow all parameters (no validation)
const folly::F14FastSet<FrameType> kAllowAllParamsFrameTypes = {
    FrameType::CLIENT_SETUP,
    FrameType::SERVER_SETUP,
    FrameType::LEGACY_CLIENT_SETUP,
    FrameType::LEGACY_SERVER_SETUP,
};

bool Parameters::isParamAllowed(TrackRequestParamKey key) const {
  // Setup frame types allow all parameters
  if (kAllowAllParamsFrameTypes.contains(frameType_)) {
    return true;
  }

  auto it = kParamAllowlist.find(key);
  if (it == kParamAllowlist.end()) {
    return false;
  }

  const auto& allowedFrameTypes = it->second;
  if (allowedFrameTypes.empty()) {
    // Empty set means allowed for all frame types
    return true;
  }

  return allowedFrameTypes.contains(frameType_);
}

Extensions noExtensions() {
  return Extensions();
}

std::pair<StandaloneFetch*, JoiningFetch*> fetchType(Fetch& fetch) {
  auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
  auto joining = std::get_if<JoiningFetch>(&fetch.args);
  return {standalone, joining};
}

std::pair<const StandaloneFetch*, const JoiningFetch*> fetchType(
    const Fetch& fetch) {
  auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
  auto joining = std::get_if<JoiningFetch>(&fetch.args);
  return {standalone, joining};
}

} // namespace moxygen
