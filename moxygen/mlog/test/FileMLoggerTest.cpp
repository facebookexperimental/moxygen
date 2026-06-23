/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/mlog/FileMLogger.h>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/ManualExecutor.h>
#include <folly/portability/GTest.h>
#include <quic/codec/QuicConnectionId.h>
#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;

namespace moxygen {

class FileMLoggerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Each test gets an isolated temp dir derived from testing::TempDir()
    dir_ = fs::path(testing::TempDir()) / "mlog_test";
    fs::create_directories(dir_);
  }

  void TearDown() override {
    fs::remove_all(dir_);
  }

  // Helper: create a ConnectionId from fixed bytes
  static quic::ConnectionId makeCid(std::vector<uint8_t> bytes) {
    return quic::ConnectionId::createAndMaybeCrash(std::move(bytes));
  }

  fs::path dir_;
};

// ---------------------------------------------------------------------------
// Sync write tests
// ---------------------------------------------------------------------------

TEST_F(FileMLoggerTest, SyncWrite_CreatesFile) {
  const auto kSyncOutputFile = "sync_out.mlog";
  auto path = (dir_ / kSyncOutputFile).string();
  FileMLogger logger(VantagePoint::SERVER, path);
  logger.outputLogs();

  EXPECT_TRUE(fs::exists(path));
}

TEST_F(FileMLoggerTest, SyncWrite_ErrorOnBadPath) {
  // Write to a path whose parent dir does not exist — should not throw
  FileMLogger logger(VantagePoint::SERVER, "/nonexistent_dir_xyz/out.mlog");
  EXPECT_NO_THROW(logger.outputLogs());
}

// ---------------------------------------------------------------------------
// Async write tests
// ---------------------------------------------------------------------------

TEST_F(FileMLoggerTest, AsyncWrite_FileNotCreatedBeforeDrain) {
  const auto kAsyncOutputFile = "async_out.mlog";
  auto executor = std::make_shared<folly::ManualExecutor>();
  auto path = (dir_ / kAsyncOutputFile).string();

  FileMLogger logger(VantagePoint::SERVER, path);
  logger.setWriteExecutor(executor);
  logger.outputLogs();

  // Task is enqueued but not yet run
  EXPECT_FALSE(fs::exists(path));

  executor->drain();
  EXPECT_TRUE(fs::exists(path));
}

TEST_F(FileMLoggerTest, AsyncWrite_DestructorFlushesPendingWrites) {
  const auto kFlushFile = "flush_out.mlog";
  auto path = (dir_ / kFlushFile).string();
  // Real thread-pool executor so writes run off the calling thread.
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(1);

  {
    FileMLogger logger(VantagePoint::SERVER, path);
    logger.setWriteExecutor(executor);
    logger.outputLogs();
    // logger goes out of scope here; destructor must wait for the
    // scheduled write to complete before returning.
  }

  // File should exist immediately after destruction — no manual drain.
  EXPECT_TRUE(fs::exists(path));
}

TEST_F(FileMLoggerTest, AsyncWrite_FlushWaitsForPendingWrites) {
  const auto kFlushFile = "flush_explicit.mlog";
  auto path = (dir_ / kFlushFile).string();
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(1);

  FileMLogger logger(VantagePoint::SERVER, path);
  logger.setWriteExecutor(executor);
  logger.outputLogs();
  logger.flush();

  EXPECT_TRUE(fs::exists(path));
}

TEST_F(FileMLoggerTest, AsyncWrite_ErrorOnBadPathDoesNotThrow) {
  auto executor = std::make_shared<folly::ManualExecutor>();
  FileMLogger logger(VantagePoint::SERVER, "/nonexistent_dir_xyz/out.mlog");
  logger.setWriteExecutor(executor);
  logger.outputLogs();
  EXPECT_NO_THROW(executor->drain());
}

// ---------------------------------------------------------------------------
// derivePath tests (verified through outputLogs file creation)
// ---------------------------------------------------------------------------

// Directory mode + dcid set: output is {dir}/{dcid_hex}.mlog
TEST_F(FileMLoggerTest, DerivePath_DirModeUsesDcid) {
  const auto kDcidFile = "12345678.mlog";
  const std::vector<uint8_t> kTestDcid = {0x12, 0x34, 0x56, 0x78};
  FileMLogger logger(VantagePoint::SERVER, FileMLogger::InDir, dir_.string());
  logger.setDcid(makeCid(kTestDcid));

  logger.outputLogs();

  EXPECT_TRUE(fs::exists(dir_ / kDcidFile));
}

TEST_F(FileMLoggerTest, DerivePath_EmptyDcidSkipsLogging) {
  FileMLogger logger(VantagePoint::SERVER, FileMLogger::InDir, dir_.string());
  logger.setDcid(quic::ConnectionId::createZeroLength());

  EXPECT_NO_THROW(logger.outputLogs());
  EXPECT_TRUE(fs::is_empty(dir_));
}

TEST_F(FileMLoggerTest, DerivePath_DirModeMissingDcidSkipsLogging) {
  FileMLogger logger(VantagePoint::SERVER, FileMLogger::InDir, dir_.string());

  EXPECT_NO_THROW(logger.outputLogs());
  EXPECT_TRUE(fs::is_empty(dir_));
}

// Path mode: output is the explicit path regardless of any cids
TEST_F(FileMLoggerTest, DerivePath_PathModeIgnoresDcid) {
  const auto kNoOpFile = "nodir_test.mlog";
  const std::vector<uint8_t> kTestCidSimple = {0x01, 0x02};
  auto path = (dir_ / kNoOpFile).string();
  FileMLogger logger(VantagePoint::SERVER, path);
  logger.setDcid(makeCid(kTestCidSimple)); // dcid present but path mode

  logger.outputLogs();

  EXPECT_TRUE(fs::exists(path));
  EXPECT_FALSE(fs::exists(dir_ / "dcid_derived.mlog"));
}

} // namespace moxygen
