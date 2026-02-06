/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQTypes.h>

#include <chrono>
#include <optional>

namespace moxygen {

/**
 * Track Property Accessors for Draft-16+
 *
 * Draft-16 moves several track properties from parameters to extensions.
 * These accessor functions abstract the param/extension details, storing
 * values in extensions (canonical for v16+) and allowing transparent access.
 *
 * Properties migrated:
 * - DELIVERY_TIMEOUT (0x02)
 * - MAX_CACHE_DURATION (0x04)
 * - PUBLISHER_PRIORITY (0x0E) - renamed DEFAULT_PUBLISHER_PRIORITY
 * - GROUP_ORDER (0x22) - renamed DEFAULT_PUBLISHER_GROUP_ORDER
 * - DYNAMIC_GROUPS (0x30)
 *
 * Supported message types: SubscribeOk, PublishRequest, FetchOk
 * (any type with an `extensions` member of type Extensions)
 */

namespace detail {

template <typename Msg>
void setIntExtension(Msg& msg, uint64_t type, uint64_t value, bool immutable) {
  if (immutable) {
    msg.extensions.insertImmutableExtension(Extension{type, value});
  } else {
    msg.extensions.insertMutableExtension(Extension{type, value});
  }
}

template <typename Msg>
std::optional<uint64_t> getIntExtension(const Msg& msg, uint64_t type) {
  return msg.extensions.getIntExtension(type);
}

} // namespace detail

// ============================================================================
// Setters - store in extensions (canonical for v16+)
// immutable defaults to false, set to true for immutable extensions
// ============================================================================

// DELIVERY_TIMEOUT setter
template <typename Msg>
void setPublisherDeliveryTimeout(
    Msg& msg,
    std::chrono::milliseconds timeout,
    bool immutable = false) {
  detail::setIntExtension(
      msg,
      kDeliveryTimeoutExtensionType,
      static_cast<uint64_t>(timeout.count()),
      immutable);
}

// MAX_CACHE_DURATION setter
template <typename Msg>
void setPublisherMaxCacheDuration(
    Msg& msg,
    std::chrono::milliseconds duration,
    bool immutable = false) {
  detail::setIntExtension(
      msg,
      kMaxCacheDurationExtensionType,
      static_cast<uint64_t>(duration.count()),
      immutable);
}

// PUBLISHER_PRIORITY setter
template <typename Msg>
void setPublisherPriority(Msg& msg, uint8_t priority, bool immutable = false) {
  detail::setIntExtension(
      msg, kPublisherPriorityExtensionType, priority, immutable);
}

// GROUP_ORDER setter
template <typename Msg>
void setPublisherGroupOrder(
    Msg& msg,
    GroupOrder order,
    bool immutable = false) {
  detail::setIntExtension(
      msg,
      kPublisherGroupOrderExtensionType,
      folly::to_underlying(order),
      immutable);
}

// DYNAMIC_GROUPS setter (0 = false, non-zero = true)
template <typename Msg>
void setPublisherDynamicGroups(Msg& msg, bool enabled, bool immutable = false) {
  detail::setIntExtension(
      msg, kDynamicGroupsExtensionType, enabled ? 1 : 0, immutable);
}

// ============================================================================
// Getters - use Extensions::getIntExtension() which searches both
// mutable/immutable
// ============================================================================

// DELIVERY_TIMEOUT getter
template <typename Msg>
std::optional<std::chrono::milliseconds> getPublisherDeliveryTimeout(
    const Msg& msg) {
  auto val = detail::getIntExtension(msg, kDeliveryTimeoutExtensionType);
  if (val) {
    return std::chrono::milliseconds(*val);
  }
  return std::nullopt;
}

// MAX_CACHE_DURATION getter
template <typename Msg>
std::optional<std::chrono::milliseconds> getPublisherMaxCacheDuration(
    const Msg& msg) {
  auto val = detail::getIntExtension(msg, kMaxCacheDurationExtensionType);
  if (val) {
    return std::chrono::milliseconds(*val);
  }
  return std::nullopt;
}

// PUBLISHER_PRIORITY getter
template <typename Msg>
std::optional<uint8_t> getPublisherPriority(const Msg& msg) {
  auto val = detail::getIntExtension(msg, kPublisherPriorityExtensionType);
  if (val && *val <= 255) {
    return static_cast<uint8_t>(*val);
  }
  return std::nullopt;
}

// GROUP_ORDER getter
template <typename Msg>
std::optional<GroupOrder> getPublisherGroupOrder(const Msg& msg) {
  auto val = detail::getIntExtension(msg, kPublisherGroupOrderExtensionType);
  if (val && *val <= folly::to_underlying(GroupOrder::NewestFirst)) {
    return static_cast<GroupOrder>(*val);
  }
  return std::nullopt;
}

// DYNAMIC_GROUPS getter
template <typename Msg>
std::optional<bool> getPublisherDynamicGroups(const Msg& msg) {
  auto val = detail::getIntExtension(msg, kDynamicGroupsExtensionType);
  if (val) {
    return *val != 0;
  }
  return std::nullopt;
}

} // namespace moxygen
