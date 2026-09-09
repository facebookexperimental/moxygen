/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace moxygen::media_server {

enum class FilePrFaultAction : uint8_t { Drop, Hold };

struct FilePrFaultDecision {
  uint64_t commandId{0};
  FilePrFaultAction action{FilePrFaultAction::Drop};
  std::chrono::milliseconds duration{0};
  bool affectIFrames{false};
};

struct FilePrTrackStatus {
  std::string track;
  std::string role;
  std::optional<uint64_t> currentGroup;
  std::optional<uint64_t> currentSubgroup;
  size_t groupObjects{0};
  std::string action;
  int64_t durationMs{0};
  int64_t remainingMs{0};
  bool affectIFrames{false};
  std::string lastEvent;
};

struct FilePrFaultOperation {
  uint64_t commandId{0};
  std::string track;
  std::string action;
  std::string status;
  int64_t durationMs{0};
  bool affectIFrames{false};
  uint64_t subgroupsAffected{0};
  uint64_t objectsDropped{0};
  uint64_t objectsReleased{0};
  uint64_t preservedIFrames{0};
  std::optional<uint64_t> lastGroup;
  std::optional<uint64_t> lastSubgroup;
};

// Process-local state for the disposable file_pr control fixture. The HTTP
// thread writes commands while file-backed media tracks consume them.
class FilePrFaultState {
 public:
  uint64_t arm(
      const std::string& track,
      FilePrFaultAction action,
      std::chrono::milliseconds duration,
      bool affectIFrames);

  uint64_t clear(const std::string& track);
  void registerTrack(const std::string& track, const std::string& role);
  void observeSubgroup(
      const std::string& track,
      uint64_t group,
      uint64_t subgroup,
      size_t objects);

  std::optional<FilePrFaultDecision> actionForSubgroup(
      const std::string& track,
      uint64_t group,
      uint64_t subgroup);

  bool beginHoldRelease(const std::string& track, uint64_t commandId);

  void recordDrop(
      const std::string& track,
      uint64_t commandId,
      uint64_t group,
      uint64_t subgroup,
      size_t dropped,
      size_t preservedIFrames);

  void finishHold(
      const std::string& track,
      uint64_t commandId,
      uint64_t group,
      uint64_t subgroup,
      size_t objects,
      std::chrono::milliseconds duration);

  std::vector<FilePrTrackStatus> snapshot();
  std::vector<FilePrFaultOperation> operationSnapshot();

 private:
  using Clock = std::chrono::steady_clock;

  struct Command {
    uint64_t id{0};
    FilePrFaultAction action{FilePrFaultAction::Drop};
    std::chrono::milliseconds duration{0};
    bool affectIFrames{false};
    bool applied{false};
    Clock::time_point expiresAt{};
  };

  struct TrackState {
    std::string role;
    std::optional<uint64_t> currentGroup;
    std::optional<uint64_t> currentSubgroup;
    size_t groupObjects{0};
    std::optional<Command> command;
    std::string lastEvent;
  };

  FilePrFaultOperation* findOperation(uint64_t commandId);
  void expireCommand(TrackState& state, Clock::time_point now);

  std::mutex mutex_;
  std::unordered_map<std::string, TrackState> tracks_;
  std::vector<FilePrFaultOperation> operations_;
  uint64_t nextCommandId_{1};
};

FilePrFaultState& filePrFaultState();

} // namespace moxygen::media_server
