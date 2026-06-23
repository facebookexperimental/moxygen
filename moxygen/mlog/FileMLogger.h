/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Executor.h>
#include <folly/futures/Future.h>
#include <memory>
#include <optional>
#include <string>
#include "moxygen/mlog/MLogger.h"

namespace moxygen {

/**
 * FileMLogger is a concrete MLogger implementation that outputs logs to a file.
 *
 * Output location is fixed at construction time. Use the path-mode ctor to
 * write every session's logs to the same file, or the directory-mode ctor
 * (with the InDir tag) to derive a per-session filename as
 * {dir}/{dcid_hex}.mlog — the latter requires setDcid() to be called before
 * outputLogs().
 *
 * If a write executor is set via setWriteExecutor(), outputLogs() will
 * pre-format logs on the calling thread and schedule the file write
 * asynchronously on that executor.
 */
class FileMLogger : public MLogger {
 public:
  // Tag used to select the directory-mode constructor.
  struct DirTag {};
  static constexpr DirTag InDir{};

  // Path mode: writes every outputLogs() call to `path`.
  FileMLogger(VantagePoint vantagePoint, std::string path)
      : MLogger(vantagePoint), path_(std::move(path)) {}

  // Directory mode: writes to {dir}/{dcid_hex}.mlog. setDcid() must be set
  // before outputLogs(), otherwise the write is skipped with a warning.
  FileMLogger(VantagePoint vantagePoint, DirTag, std::string dir)
      : MLogger(vantagePoint), dir_(std::move(dir)) {}

  // Blocks until any outstanding async writes complete. If the configured
  // executor never drains (e.g. stuck thread pool), destruction will block
  // indefinitely — callers must ensure the executor is alive and progressing
  // through teardown.
  ~FileMLogger() override {
    flush();
  }

  FileMLogger(const FileMLogger&) = delete;
  FileMLogger& operator=(const FileMLogger&) = delete;
  FileMLogger(FileMLogger&&) = delete;
  FileMLogger& operator=(FileMLogger&&) = delete;

  /**
   * Set an executor for asynchronous file writes.
   */
  void setWriteExecutor(std::shared_ptr<folly::Executor> executor) {
    writeExecutor_ = std::move(executor);
  }

  /**
   * Outputs all accumulated logs to the configured destination.
   * Logs are formatted as pretty-printed JSON.
   */
  void outputLogs() override;

  /**
   * Blocks until all previously-scheduled async writes have completed.
   * Safe to call when no write executor is set (no-op). Must not race with
   * outputLogs() on the same logger.
   */
  void flush();

 private:
  /**
   * Derives the output file path. In directory mode, dcid with a non-empty
   * hex representation is required and the path is {dir}/{dcid_hex}.mlog.
   * In path mode, returns the explicit path. Returns nullopt when logging
   * should be skipped.
   */
  std::optional<std::string> derivePath() const;

  std::shared_ptr<folly::Executor> writeExecutor_;
  // Exactly one of path_ / dir_ is engaged, per the ctor invoked.
  std::optional<std::string> path_;
  std::optional<std::string> dir_;
  // Tail of the chain of scheduled async writes. Each outputLogs() call
  // appends its write to this future; flush() waits on it. Bounded by
  // outstanding (not yet executed) writes.
  folly::SemiFuture<folly::Unit> pendingWrites_{folly::makeSemiFuture()};
};

} // namespace moxygen
