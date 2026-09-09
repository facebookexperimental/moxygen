/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/samples/media_server/sources/FilePrFaultState.h>

#include <algorithm>

namespace moxygen::media_server {

namespace {

std::string actionName(FilePrFaultAction action) {
  return action == FilePrFaultAction::Drop ? "drop" : "hold";
}

} // namespace

uint64_t FilePrFaultState::arm(
    const std::string& track,
    FilePrFaultAction action,
    std::chrono::milliseconds duration,
    bool affectIFrames) {
  std::lock_guard guard(mutex_);
  auto& state = tracks_[track];
  const auto now = Clock::now();
  expireCommand(state, now);
  if (state.command) {
    if (auto* operation = findOperation(state.command->id)) {
      operation->status = "replaced";
    }
  }
  const uint64_t id = nextCommandId_++;
  Command command{
      id,
      action,
      duration,
      affectIFrames,
      action == FilePrFaultAction::Drop,
      {}};
  if (action == FilePrFaultAction::Drop) {
    command.expiresAt = now + duration;
  }
  state.command = command;
  operations_.push_back(
      FilePrFaultOperation{
          .commandId = id,
          .track = track,
          .action = actionName(action),
          .status = action == FilePrFaultAction::Hold ? "armed" : "active",
          .durationMs = duration.count(),
          .affectIFrames = affectIFrames});
  state.lastEvent = actionName(action) + " armed for " +
      std::to_string(duration.count()) + " ms";
  return id;
}

uint64_t FilePrFaultState::clear(const std::string& track) {
  std::lock_guard guard(mutex_);
  auto& state = tracks_[track];
  if (state.command) {
    if (auto* operation = findOperation(state.command->id)) {
      operation->status = "cleared";
    }
  }
  state.command.reset();
  const uint64_t id = nextCommandId_++;
  operations_.push_back(
      FilePrFaultOperation{
          .commandId = id,
          .track = track,
          .action = "clear",
          .status = "completed"});
  state.lastEvent = "fault cleared";
  return id;
}

void FilePrFaultState::registerTrack(
    const std::string& track,
    const std::string& role) {
  std::lock_guard guard(mutex_);
  tracks_[track].role = role;
}

void FilePrFaultState::observeSubgroup(
    const std::string& track,
    uint64_t group,
    uint64_t subgroup,
    size_t objects) {
  std::lock_guard guard(mutex_);
  auto& state = tracks_[track];
  state.currentGroup = group;
  state.currentSubgroup = subgroup;
  state.groupObjects = objects;
  expireCommand(state, Clock::now());
}

std::optional<FilePrFaultDecision> FilePrFaultState::actionForSubgroup(
    const std::string& track,
    uint64_t group,
    uint64_t subgroup) {
  std::lock_guard guard(mutex_);
  auto it = tracks_.find(track);
  if (it == tracks_.end()) {
    return std::nullopt;
  }
  auto& state = it->second;
  const auto now = Clock::now();
  expireCommand(state, now);
  if (!state.command) {
    return std::nullopt;
  }

  auto& command = *state.command;
  if (command.action == FilePrFaultAction::Hold) {
    if (command.applied) {
      return std::nullopt;
    }
    command.applied = true;
    command.expiresAt = now + command.duration;
    if (auto* operation = findOperation(command.id)) {
      operation->status = "holding";
      operation->subgroupsAffected = 1;
      operation->lastGroup = group;
      operation->lastSubgroup = subgroup;
    }
    state.lastEvent = "holding group " + std::to_string(group) + " subgroup " +
        std::to_string(subgroup) + " for " +
        std::to_string(command.duration.count()) + " ms";
  }
  return FilePrFaultDecision{
      command.id, command.action, command.duration, command.affectIFrames};
}

bool FilePrFaultState::beginHoldRelease(
    const std::string& track,
    uint64_t commandId) {
  std::lock_guard guard(mutex_);
  auto it = tracks_.find(track);
  auto* operation = findOperation(commandId);
  if (it == tracks_.end() || !operation || operation->action != "hold") {
    return false;
  }
  operation->status = "releasing";
  it->second.lastEvent = "releasing held subgroup";
  return true;
}

void FilePrFaultState::recordDrop(
    const std::string& track,
    uint64_t commandId,
    uint64_t group,
    uint64_t subgroup,
    size_t dropped,
    size_t preservedIFrames) {
  std::lock_guard guard(mutex_);
  auto& state = tracks_[track];
  if (auto* operation = findOperation(commandId)) {
    ++operation->subgroupsAffected;
    operation->objectsDropped += dropped;
    operation->preservedIFrames += preservedIFrames;
    operation->lastGroup = group;
    operation->lastSubgroup = subgroup;
  }
  state.lastEvent = "group " + std::to_string(group) + " subgroup " +
      std::to_string(subgroup) + " dropped " + std::to_string(dropped) +
      " objects; preserved " + std::to_string(preservedIFrames) + " I-frames";
}

void FilePrFaultState::finishHold(
    const std::string& track,
    uint64_t commandId,
    uint64_t group,
    uint64_t subgroup,
    size_t objects,
    std::chrono::milliseconds duration) {
  std::lock_guard guard(mutex_);
  auto& state = tracks_[track];
  if (state.command && state.command->id == commandId) {
    state.command.reset();
  }
  if (auto* operation = findOperation(commandId)) {
    operation->status = "completed";
    operation->objectsReleased += objects;
    operation->lastGroup = group;
    operation->lastSubgroup = subgroup;
  }
  state.lastEvent = "group " + std::to_string(group) + " subgroup " +
      std::to_string(subgroup) + " held for " +
      std::to_string(duration.count()) + " ms";
}

std::vector<FilePrTrackStatus> FilePrFaultState::snapshot() {
  std::lock_guard guard(mutex_);
  const auto now = Clock::now();
  std::vector<FilePrTrackStatus> result;
  result.reserve(tracks_.size());
  for (auto& [track, state] : tracks_) {
    expireCommand(state, now);
    FilePrTrackStatus status;
    status.track = track;
    status.role = state.role;
    status.currentGroup = state.currentGroup;
    status.currentSubgroup = state.currentSubgroup;
    status.groupObjects = state.groupObjects;
    status.lastEvent = state.lastEvent;
    if (state.command) {
      const auto& command = *state.command;
      if (command.action == FilePrFaultAction::Hold) {
        if (!command.applied) {
          status.action = "hold-armed";
        } else if (const auto* operation = findOperation(command.id);
                   operation && operation->status == "releasing") {
          status.action = "releasing";
        } else {
          status.action = "holding";
        }
      } else {
        status.action = actionName(command.action);
      }
      status.durationMs = command.duration.count();
      status.affectIFrames = command.affectIFrames;
      status.remainingMs =
          command.action == FilePrFaultAction::Hold && !command.applied
          ? command.duration.count()
          : std::max<int64_t>(
                0,
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    command.expiresAt - now)
                    .count());
    }
    result.push_back(std::move(status));
  }
  std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
    return a.track < b.track;
  });
  return result;
}

std::vector<FilePrFaultOperation> FilePrFaultState::operationSnapshot() {
  std::lock_guard guard(mutex_);
  const auto now = Clock::now();
  for (auto& entry : tracks_) {
    expireCommand(entry.second, now);
  }
  return operations_;
}

FilePrFaultOperation* FilePrFaultState::findOperation(uint64_t commandId) {
  auto it = std::find_if(
      operations_.rbegin(), operations_.rend(), [commandId](const auto& item) {
        return item.commandId == commandId;
      });
  return it == operations_.rend() ? nullptr : &*it;
}

void FilePrFaultState::expireCommand(TrackState& state, Clock::time_point now) {
  if (!state.command) {
    return;
  }
  const auto& command = *state.command;
  if (command.action == FilePrFaultAction::Drop && now >= command.expiresAt) {
    if (auto* operation = findOperation(command.id)) {
      operation->status = "completed";
    }
    state.lastEvent = actionName(command.action) + " completed";
    state.command.reset();
  }
}

FilePrFaultState& filePrFaultState() {
  static FilePrFaultState state;
  return state;
}

} // namespace moxygen::media_server
