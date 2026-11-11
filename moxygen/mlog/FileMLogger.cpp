/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/mlog/FileMLogger.h"
#include <folly/Random.h>
#include <folly/futures/Future.h>
#include <folly/json/json.h>
#include <folly/logging/xlog.h>
#include <chrono>
#include <filesystem>
#include <fstream>

namespace moxygen {

std::optional<std::string> FileMLogger::derivePath() const {
  if (dir_) {
    if (!dcid_) {
      XLOG(WARN)
          << "Skipping mlog output: dcid must be set when dir is configured";
      return std::nullopt;
    }
    const auto dcidHex = dcid_->hex();
    if (dcidHex.empty()) {
      XLOG(WARN)
          << "Skipping mlog output: dcid must have a non-empty hex representation when dir is configured";
      return std::nullopt;
    }
    return (std::filesystem::path(*dir_) / (dcidHex + ".mlog")).string();
  }
  return path_;
}

void FileMLogger::outputLogs() {
  auto path = derivePath();
  if (!path) {
    return;
  }

  // Pre-format logs on the calling thread (where formatLog() is safe to
  // call), then execute the write lambda either inline or via the executor.
  folly::dynamic events = folly::dynamic::array;
  for (const auto& log : logs_) {
    auto event = formatLog(log);
    event.erase("vantagePoint");
    const auto elapsed =
        std::chrono::duration<double, std::chrono::system_clock::period>(
            static_cast<double>(log.time_));
    event["time"] = std::chrono::duration<double, std::milli>(elapsed).count();
    events.push_back(std::move(event));
  }
  folly::dynamic trace = folly::dynamic::object("events", std::move(events));
  trace["event_schemas"] =
      folly::dynamic::array("urn:ietf:params:qlog:events:moqt-08");
  trace["vantage_point"] = folly::dynamic::object(
      "type", vantagePoint_ == VantagePoint::CLIENT ? "client" : "server");
  trace["common_fields"] =
      folly::dynamic::object("time_format", "relative_to_epoch")(
          "reference_time",
          folly::dynamic::object("clock_type", "system")("epoch", "unknown"));
  folly::dynamic document = folly::dynamic::object(
      "file_schema", "urn:ietf:params:qlog:file:contained")(
      "serialization_format", "application/qlog+json")(
      "traces", folly::dynamic::array(std::move(trace)));
  auto serialized = folly::toPrettyJson(document);

  auto write = [p = std::move(*path), contents = std::move(serialized)]() {
    std::ofstream fileObj(p);
    if (!fileObj.is_open()) {
      XLOG(WARN) << "Failed to open mlog output file: " << p;
      return;
    }
    fileObj << contents << '\n';
  };

  if (writeExecutor_) {
    pendingWrites_ =
        std::move(pendingWrites_)
            .deferValue([w = std::move(write)](folly::Unit) mutable { w(); })
            .via(writeExecutor_.get())
            .semi();
  } else {
    write();
  }
}

void FileMLogger::flush() {
  std::move(pendingWrites_).wait();
  pendingWrites_ = folly::makeSemiFuture();
}

} // namespace moxygen
