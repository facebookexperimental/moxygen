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

constexpr uint32_t kLegacyDeliveryTimeout = 1;
constexpr uint32_t kLegacySessionClosed = 2;
constexpr uint32_t kLegacyCancelled = 3;
constexpr uint32_t kLegacyUnknownObjectStatus = 4;
constexpr uint32_t kLegacyTooFarBehind = 5;
constexpr uint32_t kLegacyExpired = 5;
constexpr uint32_t kLegacyPublishDoneTooFarBehind = 6;
constexpr uint32_t kLegacyExcessiveLoad = 9;
constexpr uint32_t kLegacyMalformedTrack = 12;

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

uint32_t toWirePublishDoneStatusCode(
    PublishDoneStatusCode statusCode,
    uint64_t negotiatedVersion) {
  if (getDraftMajorVersion(negotiatedVersion) >= 18) {
    return static_cast<uint32_t>(statusCode);
  }
  if (statusCode == PublishDoneStatusCode::TOO_FAR_BEHIND) {
    return kLegacyPublishDoneTooFarBehind;
  }
  if (statusCode == PublishDoneStatusCode::EXPIRED) {
    return kLegacyExpired;
  }
  return static_cast<uint32_t>(statusCode);
}

PublishDoneStatusCode fromWirePublishDoneStatusCode(
    uint32_t statusCode,
    uint64_t negotiatedVersion) {
  if (getDraftMajorVersion(negotiatedVersion) < 18) {
    if (statusCode == kLegacyExpired) {
      return PublishDoneStatusCode::EXPIRED;
    }
    if (statusCode == kLegacyPublishDoneTooFarBehind) {
      return PublishDoneStatusCode::TOO_FAR_BEHIND;
    }
  }
  return static_cast<PublishDoneStatusCode>(statusCode);
}

uint32_t toWireResetStreamErrorCode(
    ResetStreamErrorCode errorCode,
    uint64_t negotiatedVersion) {
  if (getDraftMajorVersion(negotiatedVersion) >= 18) {
    return static_cast<uint32_t>(errorCode);
  }
  switch (errorCode) {
    case ResetStreamErrorCode::INTERNAL_ERROR:
      return 0;
    case ResetStreamErrorCode::CANCELLED:
      return kLegacyCancelled;
    case ResetStreamErrorCode::DELIVERY_TIMEOUT:
      return kLegacyDeliveryTimeout;
    case ResetStreamErrorCode::SESSION_CLOSED:
      return kLegacySessionClosed;
    case ResetStreamErrorCode::TOO_FAR_BEHIND:
      return kLegacyTooFarBehind;
    case ResetStreamErrorCode::UNKNOWN_OBJECT_STATUS:
      return kLegacyUnknownObjectStatus;
    case ResetStreamErrorCode::EXCESSIVE_LOAD:
      return kLegacyExcessiveLoad;
    case ResetStreamErrorCode::MALFORMED_TRACK:
      return kLegacyMalformedTrack;
    case ResetStreamErrorCode::GOING_AWAY:
    case ResetStreamErrorCode::EXPIRED_AUTH_TOKEN:
      return 0;
  }
  return static_cast<uint32_t>(errorCode);
}

ResetStreamErrorCode fromWireResetStreamErrorCode(
    uint32_t errorCode,
    uint64_t negotiatedVersion) {
  if (getDraftMajorVersion(negotiatedVersion) >= 18) {
    return static_cast<ResetStreamErrorCode>(errorCode);
  }
  switch (errorCode) {
    case 0:
      return ResetStreamErrorCode::INTERNAL_ERROR;
    case kLegacyDeliveryTimeout:
      return ResetStreamErrorCode::DELIVERY_TIMEOUT;
    case kLegacySessionClosed:
      return ResetStreamErrorCode::SESSION_CLOSED;
    case kLegacyCancelled:
      return ResetStreamErrorCode::CANCELLED;
    case kLegacyUnknownObjectStatus:
      return ResetStreamErrorCode::UNKNOWN_OBJECT_STATUS;
    case kLegacyTooFarBehind:
      return ResetStreamErrorCode::TOO_FAR_BEHIND;
    case kLegacyExcessiveLoad:
      return ResetStreamErrorCode::EXCESSIVE_LOAD;
    case kLegacyMalformedTrack:
      return ResetStreamErrorCode::MALFORMED_TRACK;
    default:
      return static_cast<ResetStreamErrorCode>(errorCode);
  }
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

SubscribeRange LocationFilter::resolve(
    std::optional<AbsoluteLocation> largest,
    bool isFetch) const {
  const auto nextObject =
      largest ? largest->next().value_or(kLocationMax) : kLocationMin;

  SubscribeRange result;
  if (fieldCount_ == 1) {
    const uint64_t nextGroup = largest ? saturatingAdd(largest->group, 1) : 0;
    const uint64_t groupsBack = start_.group;
    result.start = {groupsBack > nextGroup ? 0 : nextGroup - groupsBack, 0};
  } else if (isNextObjectForm()) {
    result.start = nextObject;
  } else {
    result.start = start_;
    // Below draft 20, a start behind Largest moves up to the Next Object.
    // TODO: skip this at draft 20 once the unfiltered default is settled.
    if (!isFetch && largest && result.start < *largest) {
      result.start = nextObject;
    }
  }

  if (fieldCount_ >= 3) {
    result.end = {end_.group, fieldCount_ == 3 ? kEightByteLimit : end_.object};
  } else {
    result.end = kLocationMax;
  }
  // A fetch never runs past Largest Object. Without one, an omitted end stays
  // open and the publisher bounds it.
  if (isFetch && largest) {
    result.end = std::min(result.end, *largest);
  }

  XLOG(DBG1) << "resolve " << describe() << " -> start=" << result.start
             << " end=" << result.end;
  return result;
}

std::pair<AbsoluteLocation, AbsoluteLocation> LocationFilter::fetchRange(
    std::optional<AbsoluteLocation> largest) const {
  auto range = resolve(largest, /*isFetch=*/true);
  // A StandaloneFetch holds a wire End Location: a range ending with its group
  // is written as object 0 of that group.
  if (range.end.object == kEightByteLimit) {
    return {range.start, AbsoluteLocation{range.end.group, 0}};
  }
  return {range.start, range.end.next().value_or(kLocationMax)};
}

folly::Expected<LocationFilter, ErrorCode> LocationFilter::fromFields(
    const std::array<uint64_t, kMaxFields>& fields,
    uint8_t count) {
  if (count > kMaxFields) {
    XLOG(ERR) << "LocationFilter: " << int(count) << " fields, max is "
              << int(kMaxFields);
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  AbsoluteLocation end;
  if (count >= 3) {
    // Only a sum past 2^64-1 is a violation. Both operands arrive as varints,
    // so a parsed filter cannot reach that sum, but a direct caller can.
    if (fields[2] > std::numeric_limits<uint64_t>::max() - fields[0]) {
      XLOG(ERR) << "LocationFilter: EndGroup overflows, startGroup="
                << fields[0] << " delta=" << fields[2];
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    // EndGroup is delta encoded; EndObject exists only in the four field form.
    // The sum saturates so that it stays encodable.
    end = AbsoluteLocation{
        saturatingAdd(fields[0], fields[2]),
        count == kMaxFields ? fields[3] : 0};
  }
  return LocationFilter(count, AbsoluteLocation{fields[0], fields[1]}, end);
}

std::string LocationFilter::describe() const {
  switch (fieldCount_) {
    case 0:
      return "{none}";
    case 1:
      return folly::to<std::string>("{relative ", start_.group, "}");
    case 2:
      return isNextObjectForm() ? "{nextObject}" : start_.describe();
    case 3:
      return folly::to<std::string>(start_.describe(), "-{", end_.group, ",*}");
    default:
      return folly::to<std::string>(start_.describe(), "-", end_.describe());
  }
}

LocationFilter toLocationFilter(const StandaloneFetch& range) {
  if (range.end == kLocationMax) {
    return LocationFilter::absoluteStart(range.start);
  }
  // A StandaloneFetch end is the wire End Location, where object 0 means all
  // of the end group and anything else is already one past the last object.
  if (range.end.object == 0) {
    return LocationFilter::range(range.start, range.end.group);
  }
  auto inclusiveEnd = range.end.prev();
  if (!inclusiveEnd) {
    return LocationFilter::absoluteStart(range.start);
  }
  return LocationFilter::range(range.start, *inclusiveEnd);
}

SubscriptionFilter toSubscriptionFilter(const LocationFilter& filter) {
  const auto fields = filter.fields();
  const auto start = AbsoluteLocation{fields[0], fields[1]};
  if (auto endGroup = filter.legacyEndGroup()) {
    return SubscriptionFilter(LocationType::AbsoluteRange, start, endGroup);
  }
  switch (filter.fieldCount()) {
    case 0:
      return SubscriptionFilter(
          LocationType::AbsoluteStart, kLocationMin, std::nullopt);
    case 1:
      if (start.group == 0) {
        return SubscriptionFilter(
            LocationType::NextGroupStart, std::nullopt, std::nullopt);
      }
      if (start.group > 1) {
        XLOG(ERR) << "No legacy encoding for a start " << start.group
                  << " groups back; clamping to the current group";
      }
      return SubscriptionFilter(
          LocationType::LargestGroup, std::nullopt, std::nullopt);
    default:
      if (!filter.absoluteStartLocation()) {
        return SubscriptionFilter(
            LocationType::LargestObject, std::nullopt, std::nullopt);
      }
      return SubscriptionFilter(
          LocationType::AbsoluteStart, start, std::nullopt);
  }
}

LocationFilter fromSubscriptionFilter(const SubscriptionFilter& filter) {
  switch (filter.filterType) {
    case LocationType::NextGroupStart:
      return LocationFilter::relativeStart(0);
    case LocationType::LargestGroup:
      return LocationFilter::relativeStart(1);
    case LocationType::LargestObject:
      return LocationFilter::nextObject();
    case LocationType::AbsoluteRange:
      return LocationFilter::range(
          filter.location.value_or(kLocationMin), filter.endGroup.value_or(0));
    case LocationType::AbsoluteStart:
      break;
  }
  return LocationFilter::absoluteStart(filter.location.value_or(kLocationMin));
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

std::ostream& operator<<(std::ostream& os, SessionId id) {
  os << id.value;
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

static bool isLegacyPublishOkSubscriptionParameter(TrackRequestParamKey key) {
  switch (key) {
    case TrackRequestParamKey::OBJECT_DELIVERY_TIMEOUT:
    case TrackRequestParamKey::SUBGROUP_DELIVERY_TIMEOUT:
    case TrackRequestParamKey::SUBSCRIBER_PRIORITY:
    case TrackRequestParamKey::SUBSCRIPTION_FILTER:
    case TrackRequestParamKey::GROUP_ORDER:
    case TrackRequestParamKey::FORWARD:
    case TrackRequestParamKey::NEW_GROUP_REQUEST:
      return true;
    default:
      return false;
  }
}

// v20+ carries the FETCH range in LOCATION_FILTER (0x21) and allows it on
// PUBLISH, but not on PUBLISH_OK.
static bool isLocationFilterAllowedV20(FrameType frameType) {
  switch (frameType) {
    case FrameType::SUBSCRIBE:
    case FrameType::REQUEST_UPDATE:
    case FrameType::PUBLISH:
    case FrameType::FETCH:
      return true;
    default:
      return false;
  }
}

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
        // v20+ FILL_PARAMETERS (0x23): MAY appear in SUBSCRIBE or
        // REQUEST_UPDATE.
        {TrackRequestParamKey::FILL_PARAMETERS,
         {FrameType::SUBSCRIBE, FrameType::REQUEST_UPDATE}},
        // v22+ INCLUDE_PROPERTIES (0x35): MAY appear in SUBSCRIBE,
        // TRACK_STATUS, FETCH, or SUBSCRIBE_TRACKS.
        {TrackRequestParamKey::INCLUDE_PROPERTIES,
         {FrameType::SUBSCRIBE,
          FrameType::TRACK_STATUS,
          FrameType::FETCH,
          FrameType::SUBSCRIBE_TRACKS}},
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

static bool isV20OnlyParamKey(TrackRequestParamKey key) {
  return key == TrackRequestParamKey::FILL_PARAMETERS;
}

static bool isV22OnlyParamKey(TrackRequestParamKey key) {
  return key == TrackRequestParamKey::INCLUDE_PROPERTIES;
}

bool Parameters::isKnownParamKey(uint64_t key, uint64_t majorVersion) {
  auto typedKey = static_cast<TrackRequestParamKey>(key);
  if (majorVersion < 18 && isV18OnlyParamKey(typedKey)) {
    return false;
  }
  if (majorVersion < 20 && isV20OnlyParamKey(typedKey)) {
    return false;
  }
  if (majorVersion < 22 && isV22OnlyParamKey(typedKey)) {
    return false;
  }
  return kParamAllowlist.find(typedKey) != kParamAllowlist.end();
}

bool Parameters::isParamAllowed(TrackRequestParamKey key) const {
  // Setup frame types allow all parameters
  if (kAllowAllParamsFrameTypes.contains(frameType_)) {
    return true;
  }

  const bool isDraft22OrLater =
      majorVersion_.has_value() && *majorVersion_ >= 22;
  const bool isPublishOkResponse = frameType_ == FrameType::PUBLISH_OK ||
      frameType_ == FrameType::REQUEST_OK;
  if (isDraft22OrLater && isPublishOkResponse &&
      isLegacyPublishOkSubscriptionParameter(key)) {
    return false;
  }
  if (key == TrackRequestParamKey::OBJECT_DELIVERY_TIMEOUT &&
      majorVersion_.has_value() && *majorVersion_ >= 18) {
    return kAllowedFramesForObjectDeliveryTimeoutV18.contains(frameType_);
  }

  // Version-specific restrictions for track property params
  if (majorVersion_.has_value()) {
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
        // Extensions-only, not allowed as a param.
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

  if (isV20OnlyParamKey(key) &&
      (!majorVersion_.has_value() || *majorVersion_ < 20)) {
    return false;
  }

  // INCLUDE_PROPERTIES did not exist before draft 22.
  if (isV22OnlyParamKey(key) &&
      (!majorVersion_.has_value() || *majorVersion_ < 22)) {
    return false;
  }

  if (key == TrackRequestParamKey::LOCATION_FILTER &&
      majorVersion_.has_value() && *majorVersion_ >= 20) {
    return isLocationFilterAllowedV20(frameType_);
  }

  auto it = kParamAllowlist.find(key);
  if (it == kParamAllowlist.end()) {
    // The wire parser rejects unknown keys (isKnownParamKey); local callers
    // may still carry application-defined ones.
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
    LocationFilter filter,
    const std::vector<Parameter>& inputParams) {
  SubscribeRequest req = SubscribeRequest{
      RequestID(), // Default constructed RequestID
      fullTrackName,
      priority,
      groupOrder,
      forward,
      filter,
      std::nullopt,
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

SubscribeRequest SubscribeRequest::make(
    const FullTrackName& fullTrackName,
    uint8_t priority,
    GroupOrder groupOrder,
    bool forward,
    LocationType locType,
    std::optional<AbsoluteLocation> start,
    uint64_t endGroup,
    const std::vector<Parameter>& inputParams) {
  return make(
      fullTrackName,
      priority,
      groupOrder,
      forward,
      fromSubscriptionFilter(SubscriptionFilter(
          locType,
          start,
          locType == LocationType::AbsoluteRange
              ? std::optional<uint64_t>(endGroup)
              : std::nullopt)),
      inputParams);
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
