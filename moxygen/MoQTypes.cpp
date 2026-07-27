/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQTypes.h>
#include <moxygen/MoQVersions.h>

#include <folly/Conv.h>
#include <folly/String.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <folly/logging/xlog.h>

namespace {

const char* getFrameTypeString(moxygen::FrameType type) {
  switch (type) {
    case moxygen::FrameType::SUBSCRIBE:
      return "SUBSCRIBE";
    case moxygen::FrameType::SUBSCRIBE_OK:
      return "SUBSCRIBE_OK";
    case moxygen::FrameType::SUBSCRIBE_ERROR:
      return "SUBSCRIBE_ERROR";
    case moxygen::FrameType::PUBLISH_DONE:
      return "PUBLISH_DONE";
    case moxygen::FrameType::MAX_REQUEST_ID:
      return "MAX_REQUEST_ID";
    case moxygen::FrameType::UNSUBSCRIBE:
      return "UNSUBSCRIBE";
    case moxygen::FrameType::PUBLISH_NAMESPACE:
      return "PUBLISH_NAMESPACE";
    case moxygen::FrameType::PUBLISH_NAMESPACE_OK:
      return "PUBLISH_NAMESPACE_OK";
    case moxygen::FrameType::PUBLISH_NAMESPACE_ERROR:
      return "PUBLISH_NAMESPACE_ERROR";
    case moxygen::FrameType::PUBLISH_NAMESPACE_DONE:
      return "PUBLISH_NAMESPACE_DONE";
    case moxygen::FrameType::GOAWAY:
      return "GOAWAY";
    default:
      // can happen when type was cast from uint8_t
      return "Unknown";
  }
#if defined(__APPLE__)
  __builtin_unreachable();
#else
  XLOG(FATAL) << "Unreachable";
#endif
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
#if defined(__APPLE__)
  __builtin_unreachable();
#else
  XLOG(FATAL) << "Unreachable";
#endif
}

const char* getObjectStatusString(moxygen::ObjectStatus objectStatus) {
  switch (objectStatus) {
    case moxygen::ObjectStatus::NORMAL:
      return "NORMAL";
    case moxygen::ObjectStatus::END_OF_GROUP:
      return "END_OF_GROUP";
    case moxygen::ObjectStatus::END_OF_TRACK:
      return "END_OF_TRACK";
    default:
      // can happen when type was cast from uint8_t
      return "Unknown";
  }
#if defined(__APPLE__)
  __builtin_unreachable();
#else
  XLOG(FATAL) << "Unreachable";
#endif
}

} // namespace

namespace moxygen {

PublishDoneStatusCode tooFarBehindCode(uint64_t negotiatedVersion) {
  return getDraftMajorVersion(negotiatedVersion) <= 16
      ? PublishDoneStatusCode::TOO_FAR_BEHIND_16
      : PublishDoneStatusCode::TOO_FAR_BEHIND;
}

std::string AbsoluteLocation::describe() const {
  return folly::to<std::string>("{", group, ",", object, "}");
}

TrackNamespace::TrackNamespace(
    const std::string& tns,
    const std::string& delimiter) {
  folly::split(delimiter, tns, trackNamespace);
}

std::string FullTrackName::describe() const {
  if (trackNamespace.empty()) {
    return trackName;
  }
  return folly::to<std::string>(trackNamespace.describe(), '/', trackName);
}

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
    FrameType::SUBSCRIBE_NAMESPACE,
    FrameType::SUBSCRIBE_TRACKS,
    FrameType::PUBLISH_NAMESPACE,
    FrameType::TRACK_STATUS,
    FrameType::FETCH};

const folly::F14FastSet<FrameType> kAllowedFramesForDeliveryTimeout = {
    FrameType::PUBLISH_OK,
    FrameType::SUBSCRIBE,
    FrameType::SUBSCRIBE_UPDATE};

// In v18+, key 0x02 is OBJECT_DELIVERY_TIMEOUT. REQUEST_OK is accepted at
// parse time because PUBLISH_OK is encoded on the wire as REQUEST_OK; the
// PUBLISH_OK conversion validates once the pending request resolves.
const folly::F14FastSet<FrameType> kAllowedFramesForObjectDeliveryTimeoutV18 = {
    FrameType::PUBLISH_OK,
    FrameType::REQUEST_OK,
    FrameType::SUBSCRIBE,
    FrameType::REQUEST_UPDATE};

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
    FrameType::SUBSCRIBE_NAMESPACE,
    FrameType::SUBSCRIBE_TRACKS};

const folly::F14FastSet<FrameType> kAllowedFramesForNewGroupRequest = {
    FrameType::SUBSCRIBE,
    FrameType::REQUEST_UPDATE,
    FrameType::PUBLISH_OK};

// v18+ SUBGROUP_DELIVERY_TIMEOUT (0x06): MAY appear in PUBLISH_OK,
// SUBSCRIBE, or REQUEST_UPDATE.
const folly::F14FastSet<FrameType> kAllowedFramesForSubgroupDeliveryTimeout = {
    FrameType::PUBLISH_OK,
    FrameType::REQUEST_OK,
    FrameType::SUBSCRIBE,
    FrameType::REQUEST_UPDATE};

// v18+ FILL_TIMEOUT (0x0A): MAY appear in FETCH only.
const folly::F14FastSet<FrameType> kAllowedFramesForFillTimeout = {
    FrameType::FETCH};

// v18+ TRACK_NAMESPACE_PREFIX (0x34): MAY appear in REQUEST_UPDATE only.
const folly::F14FastSet<FrameType> kAllowedFramesForTrackNamespacePrefix = {
    FrameType::REQUEST_UPDATE};

const folly::F14FastSet<FrameType> kAllowedFramesForTrackFilter = {
    FrameType::SUBSCRIBE_NAMESPACE};

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
        {TrackRequestParamKey::NEW_GROUP_REQUEST,
         kAllowedFramesForNewGroupRequest},
        {TrackRequestParamKey::SUBGROUP_DELIVERY_TIMEOUT,
         kAllowedFramesForSubgroupDeliveryTimeout},
        {TrackRequestParamKey::FILL_TIMEOUT, kAllowedFramesForFillTimeout},
        {TrackRequestParamKey::TRACK_NAMESPACE_PREFIX,
         kAllowedFramesForTrackNamespacePrefix},
        {TrackRequestParamKey::TRACK_FILTER, kAllowedFramesForTrackFilter},
};

// Frame types that allow all parameters (no validation)
const folly::F14FastSet<FrameType> kAllowAllParamsFrameTypes = {
    FrameType::CLIENT_SETUP,
    FrameType::SERVER_SETUP,
    FrameType::SETUP,
};

// Parameter keys introduced in draft 18 that reuse no earlier key value. They
// must be treated as unknown (and therefore rejected) when the negotiated draft
// is below 18.
static bool isV18OnlyParamKey(TrackRequestParamKey key) {
  switch (key) {
    case TrackRequestParamKey::SUBGROUP_DELIVERY_TIMEOUT:
    case TrackRequestParamKey::FILL_TIMEOUT:
    case TrackRequestParamKey::TRACK_NAMESPACE_PREFIX:
      return true;
    default:
      return false;
  }
}

bool Parameters::isKnownParamKey(uint64_t key, uint64_t majorVersion) {
  auto typedKey = static_cast<TrackRequestParamKey>(key);
  if (majorVersion < 18 && isV18OnlyParamKey(typedKey)) {
    return false;
  }
  return kParamAllowlist.find(typedKey) != kParamAllowlist.end();
}

bool Parameters::isParamAllowed(TrackRequestParamKey key) const {
  // Setup frame types allow all parameters
  if (kAllowAllParamsFrameTypes.contains(frameType_)) {
    return true;
  }

  if (key == TrackRequestParamKey::OBJECT_DELIVERY_TIMEOUT &&
      majorVersion_.has_value() && *majorVersion_ >= 18) {
    return kAllowedFramesForObjectDeliveryTimeoutV18.contains(frameType_);
  }

  // v16+ version-specific restrictions for track property params
  if (majorVersion_.has_value() && *majorVersion_ >= 16) {
    switch (key) {
      case TrackRequestParamKey::MAX_CACHE_DURATION:
        // Key 0x04 is reinterpreted as RENDEZVOUS_TIMEOUT in v18+; allowed
        // only on SUBSCRIBE. In v16/v17 it remains MAX_CACHE_DURATION which
        // is extensions-only.
        if (*majorVersion_ >= 18 && frameType_ == FrameType::SUBSCRIBE) {
          return true;
        }
        return false;
      case TrackRequestParamKey::PUBLISHER_PRIORITY:
        // Extensions-only in v16+, not allowed as a param.
        return false;
      default:
        break;
    }
  }

  // v18+: SUBSCRIBE_NAMESPACE no longer carries FORWARD; it was split out
  // into SUBSCRIBE_TRACKS.
  if (majorVersion_.has_value() && *majorVersion_ >= 18 &&
      key == TrackRequestParamKey::FORWARD &&
      frameType_ == FrameType::SUBSCRIBE_NAMESPACE) {
    return false;
  }

  // v18-only parameter keys.
  if (isV18OnlyParamKey(key) &&
      (!majorVersion_.has_value() || *majorVersion_ < 18)) {
    return false;
  }

  auto it = kParamAllowlist.find(key);
  if (it == kParamAllowlist.end()) {
    // TODO: Make this strict when we drop V15- support
    return true;
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

SubscribeRequest SubscribeRequest::make(
    const FullTrackName& fullTrackName,
    uint8_t priority,
    GroupOrder groupOrder,
    bool forward,
    LocationType locType,
    std::optional<AbsoluteLocation> start,
    uint64_t endGroup,
    const std::vector<Parameter>& inputParams) {
  SubscribeRequest req = SubscribeRequest{
      RequestID(), // Default constructed RequestID
      fullTrackName,
      priority,
      groupOrder,
      forward,
      locType,
      std::move(start),
      endGroup,
      TrackRequestParameters{FrameType::SUBSCRIBE}};

  for (const auto& param : inputParams) {
    auto result = req.params.insertParam(param);
    if (result.hasError()) {
      XLOG(ERR) << "SubscribeRequest::make: param not allowed, key="
                << param.key;
    }
  }
  return req;
}

Fetch::Fetch(
    RequestID su,
    FullTrackName ftn,
    AbsoluteLocation st,
    AbsoluteLocation e,
    uint8_t p,
    GroupOrder g,
    const std::vector<Parameter>& pa)
    : requestID(su),
      fullTrackName(std::move(ftn)),
      priority(p),
      groupOrder(g),
      args(StandaloneFetch(st, e)) {
  for (const auto& param : pa) {
    auto result = params.insertParam(param);
    if (result.hasError()) {
      XLOG(ERR) << "Fetch: param not allowed, key=" << param.key;
    }
  }
}

Fetch::Fetch(
    RequestID su,
    std::optional<RequestID> jsid,
    uint64_t joiningStart,
    FetchType fetchType,
    uint8_t p,
    GroupOrder g,
    const std::vector<Parameter>& pa)
    : requestID(su),
      priority(p),
      groupOrder(g),
      args(JoiningFetch(jsid, joiningStart, fetchType)) {
  XCHECK(
      fetchType == FetchType::RELATIVE_JOINING ||
      fetchType == FetchType::ABSOLUTE_JOINING);
  for (const auto& param : pa) {
    auto result = params.insertParam(param);
    if (result.hasError()) {
      XLOG(ERR) << "Fetch: param not allowed, key=" << param.key;
    }
  }
}

} // namespace moxygen
