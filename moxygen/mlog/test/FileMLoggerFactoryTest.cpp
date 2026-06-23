/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/mlog/FileMLogger.h>
#include <moxygen/mlog/FileMLoggerFactory.h>

#include <folly/executors/ManualExecutor.h>
#include <folly/portability/GTest.h>
#include <quic/codec/QuicConnectionId.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace moxygen {

class FileMLoggerFactoryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = fs::path(testing::TempDir()) / "mlog_factory_test";
    fs::create_directories(dir_);
  }

  void TearDown() override {
    fs::remove_all(dir_);
  }

  fs::path dir_;
};

// ---------------------------------------------------------------------------
// Constructor tests
// ---------------------------------------------------------------------------

TEST_F(FileMLoggerFactoryTest, PathCtor_CreatesLoggerThatWritesToGivenPath) {
  const auto kFactoryOutputFile = "factory_out.mlog";
  auto path = (dir_ / kFactoryOutputFile).string();
  FileMLoggerFactory factory(path);

  auto logger = factory.createMLogger();
  ASSERT_NE(logger, nullptr);

  logger->outputLogs();
  EXPECT_TRUE(fs::exists(path));
}

// ---------------------------------------------------------------------------
// Dir ctor propagation
// ---------------------------------------------------------------------------

// Factory propagates dir to loggers: outputLogs() should use dcid-derived
// filename
TEST_F(FileMLoggerFactoryTest, DirCtor_PropagatesDirToLogger) {
  const auto kFile = "11223344.mlog";
  const std::vector<uint8_t> kCid = {0x11, 0x22, 0x33, 0x44};
  DirMLoggerFactory factory(dir_.string());

  auto logger = factory.createMLogger();
  ASSERT_NE(logger, nullptr);

  // Give the logger a known dcid so derivePath produces a predictable filename
  logger->setDcid(quic::ConnectionId::createAndMaybeCrash(kCid));
  logger->outputLogs();

  EXPECT_TRUE(fs::exists(dir_ / kFile));
}

// Multiple loggers from the same factory each get their own dir
TEST_F(FileMLoggerFactoryTest, DirCtor_EachLoggerGetsOwnFile) {
  const auto kLoggerAFile = "55667788.mlog";
  const auto kLoggerBFile = "99aabbcc.mlog";
  const std::vector<uint8_t> kLoggerACid = {0x55, 0x66, 0x77, 0x88};
  const std::vector<uint8_t> kLoggerBCid = {0x99, 0xAA, 0xBB, 0xCC};
  DirMLoggerFactory factory(dir_.string());

  auto loggerA = factory.createMLogger();
  auto loggerB = factory.createMLogger();
  ASSERT_NE(loggerA, nullptr);
  ASSERT_NE(loggerB, nullptr);

  loggerA->setDcid(quic::ConnectionId::createAndMaybeCrash(kLoggerACid));
  loggerB->setDcid(quic::ConnectionId::createAndMaybeCrash(kLoggerBCid));
  loggerA->outputLogs();
  loggerB->outputLogs();

  EXPECT_TRUE(fs::exists(dir_ / kLoggerAFile));
  EXPECT_TRUE(fs::exists(dir_ / kLoggerBFile));
}

// ---------------------------------------------------------------------------
// setWriteExecutor propagation
// ---------------------------------------------------------------------------

TEST_F(FileMLoggerFactoryTest, SetWriteExecutor_PropagatesAsyncBehavior) {
  const auto kAsyncFactoryFile = "async_factory.mlog";
  auto executor = std::make_shared<folly::ManualExecutor>();
  auto path = (dir_ / kAsyncFactoryFile).string();

  FileMLoggerFactory factory(path);
  factory.setWriteExecutor(executor);

  auto logger = factory.createMLogger();
  ASSERT_NE(logger, nullptr);

  logger->outputLogs();
  // Not yet written — executor not drained
  EXPECT_FALSE(fs::exists(path));

  executor->drain();
  EXPECT_TRUE(fs::exists(path));
}

// Each logger created by the factory shares the same executor
TEST_F(FileMLoggerFactoryTest, SetWriteExecutor_SharedAcrossLoggers) {
  const auto kAsyncLoggerAFile = "ddeeff00.mlog";
  const auto kAsyncLoggerBFile = "aabbccdd.mlog";
  const std::vector<uint8_t> kAsyncLoggerACid = {0xDD, 0xEE, 0xFF, 0x00};
  const std::vector<uint8_t> kAsyncLoggerBCid = {0xAA, 0xBB, 0xCC, 0xDD};
  auto executor = std::make_shared<folly::ManualExecutor>();
  DirMLoggerFactory factory(dir_.string());
  factory.setWriteExecutor(executor);

  auto loggerA = factory.createMLogger();
  auto loggerB = factory.createMLogger();

  loggerA->setDcid(quic::ConnectionId::createAndMaybeCrash(kAsyncLoggerACid));
  loggerB->setDcid(quic::ConnectionId::createAndMaybeCrash(kAsyncLoggerBCid));

  loggerA->outputLogs();
  loggerB->outputLogs();

  EXPECT_FALSE(fs::exists(dir_ / kAsyncLoggerAFile));
  EXPECT_FALSE(fs::exists(dir_ / kAsyncLoggerBFile));

  executor->drain();

  EXPECT_TRUE(fs::exists(dir_ / kAsyncLoggerAFile));
  EXPECT_TRUE(fs::exists(dir_ / kAsyncLoggerBFile));
}

} // namespace moxygen
