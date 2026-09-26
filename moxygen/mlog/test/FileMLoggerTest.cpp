/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/mlog/FileMLogger.h>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/ManualExecutor.h>
#include <folly/json/json.h>
#include <folly/portability/GTest.h>
#include <quic/codec/QuicConnectionId.h>
#include <stdlib.h>
#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace moxygen {
namespace {

enum class ObjectEvent {
  DATAGRAM_CREATED,
  DATAGRAM_PARSED,
  SUBGROUP_CREATED,
  SUBGROUP_PARSED,
  FETCH_CREATED,
  FETCH_PARSED,
};

struct ObjectEventCase {
  ObjectEvent event;
  const char* name;
  const char* payloadField;
  bool datagram;
  bool retainsBackingStorage;
};

constexpr std::array<ObjectEventCase, 6> kObjectEvents{{
    {ObjectEvent::DATAGRAM_CREATED,
     "moqt:object_datagram_created",
     "object_payload",
     true,
     true},
    {ObjectEvent::DATAGRAM_PARSED,
     "moqt:object_datagram_parsed",
     "object_payload",
     true,
     false},
    {ObjectEvent::SUBGROUP_CREATED,
     "moqt:subgroup_object_created",
     "objectPayload",
     false,
     true},
    {ObjectEvent::SUBGROUP_PARSED,
     "moqt:subgroup_object_parsed",
     "objectPayload",
     false,
     true},
    {ObjectEvent::FETCH_CREATED,
     "moqt:fetch_object_created",
     "objectPayload",
     false,
     true},
    {ObjectEvent::FETCH_PARSED,
     "moqt:fetch_object_parsed",
     "objectPayload",
     false,
     true},
}};

Payload makePayload(std::string_view head, std::string_view tail = {}) {
  auto payload = folly::IOBuf::copyBuffer(head.data(), head.size());
  if (!tail.empty()) {
    payload->appendChain(folly::IOBuf::copyBuffer(tail.data(), tail.size()));
  }
  return payload;
}

void logObjectEvent(MLogger& logger, ObjectEvent event, Payload payload) {
  ObjectHeader header(7, 8, 9, 10);
  switch (event) {
    case ObjectEvent::DATAGRAM_CREATED:
      logger.logObjectDatagramCreated(TrackAlias(11), header, payload);
      return;
    case ObjectEvent::DATAGRAM_PARSED:
      logger.logObjectDatagramParsed(TrackAlias(11), header, payload);
      return;
    case ObjectEvent::SUBGROUP_CREATED:
      logger.logSubgroupObjectCreated(
          17, TrackAlias(11), header, std::move(payload));
      return;
    case ObjectEvent::SUBGROUP_PARSED:
      logger.logSubgroupObjectParsed(
          17, TrackAlias(11), header, std::move(payload));
      return;
    case ObjectEvent::FETCH_CREATED:
      logger.logFetchObjectCreated(17, header, std::move(payload));
      return;
    case ObjectEvent::FETCH_PARSED:
      logger.logFetchObjectParsed(17, header, std::move(payload));
      return;
  }
}

folly::dynamic parseSingleEvent(const std::string& path) {
  std::ifstream input(path);
  return folly::parseJson(
      std::string(std::istreambuf_iterator<char>(input), {}));
}

void expectEventControls(
    const folly::dynamic& event,
    const ObjectEventCase& testCase) {
  ASSERT_TRUE(event.isObject());
  EXPECT_EQ(event["name"].asString(), testCase.name);
  EXPECT_EQ(event["vantagePoint"].asString(), "server");
  const auto* data = event.get_ptr("data");
  ASSERT_NE(data, nullptr);
  const auto* objectId =
      data->get_ptr(testCase.datagram ? "object_id" : "objectId");
  ASSERT_NE(objectId, nullptr);
  if (testCase.datagram) {
    EXPECT_EQ(objectId->asInt(), 9);
  } else {
    EXPECT_EQ(objectId->asString(), "9");
  }
}

// Datagram events keep a qlog RawInfo holding only the length; the other
// events drop the payload field outright.
bool payloadBytesExported(
    const folly::dynamic& data,
    const ObjectEventCase& testCase) {
  const auto* payload = data.get_ptr(testCase.payloadField);
  if (!payload) {
    return false;
  }
  return !testCase.datagram || payload->get_ptr("data") != nullptr;
}

const folly::dynamic* payloadLength(
    const folly::dynamic& data,
    const ObjectEventCase& testCase) {
  if (!testCase.datagram) {
    return data.get_ptr("objectPayloadLength");
  }
  const auto* payload = data.get_ptr(testCase.payloadField);
  return payload ? payload->get_ptr("length") : nullptr;
}

struct ReleaseCounter {
  size_t count{0};
};

void releaseOwnedBuffer(void* buffer, void* userData) {
  ++static_cast<ReleaseCounter*>(userData)->count;
  delete[] static_cast<uint8_t*>(buffer);
}

Payload makeOwnedPayload(ReleaseCounter& counter) {
  auto makeSegment = [&counter](std::string_view contents) {
    auto* data = new uint8_t[contents.size()];
    std::memcpy(data, contents.data(), contents.size());
    return folly::IOBuf::takeOwnership(
        data, contents.size(), contents.size(), releaseOwnedBuffer, &counter);
  };
  auto payload = makeSegment("owned-head");
  payload->appendChain(makeSegment("owned-tail"));
  return payload;
}

void appendViolation(
    std::string& violations,
    const ObjectEventCase& testCase,
    std::string_view detail) {
  if (!violations.empty()) {
    violations += "; ";
  }
  violations += testCase.name;
  violations += ":";
  violations += detail;
}

} // namespace

class FileMLoggerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // mkdtemp, not a fixed name: test runners execute test cases in
    // concurrent processes that share TMPDIR, so a fixed name is shared state.
    std::string tmpl =
        (fs::path(testing::TempDir()) / "mlog_test_XXXXXX").string();
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    ASSERT_NE(::mkdtemp(buf.data()), nullptr) << "mkdtemp failed for " << tmpl;
    dir_ = fs::path(buf.data());
  }

  void TearDown() override {
    std::error_code ec;
    fs::remove_all(dir_, ec);
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

TEST_F(FileMLoggerTest, ObjectBodiesAbsentFromFileOutput) {
  std::string violations;
  for (size_t i = 0; i < kObjectEvents.size(); ++i) {
    const auto& testCase = kObjectEvents[i];
    SCOPED_TRACE(testCase.name);
    const auto canary = "file-object-body-" + std::to_string(i);
    const auto path = (dir_ / (std::to_string(i) + ".mlog")).string();
    FileMLogger logger(VantagePoint::SERVER, path);
    logObjectEvent(logger, testCase.event, makePayload(canary));
    logger.outputLogs();

    const auto event = parseSingleEvent(path);
    expectEventControls(event, testCase);
    const auto& data = event["data"];
    if (payloadBytesExported(data, testCase)) {
      appendViolation(violations, testCase, "payload field exported");
    }
    if (folly::toJson(event).find(canary) != std::string::npos) {
      appendViolation(violations, testCase, "canary exported");
    }
  }
  EXPECT_TRUE(violations.empty()) << violations;
}

TEST_F(FileMLoggerTest, ObjectPayloadLengthUsesEntireSuppliedChain) {
  constexpr std::string_view kHead = "length-head";
  constexpr std::string_view kTail = "length-tail";
  constexpr auto kExpectedLength = kHead.size() + kTail.size();
  std::string violations;
  for (size_t i = 0; i < kObjectEvents.size(); ++i) {
    const auto& testCase = kObjectEvents[i];
    SCOPED_TRACE(testCase.name);
    const auto path =
        (dir_ / ("length-" + std::to_string(i) + ".mlog")).string();
    FileMLogger logger(VantagePoint::SERVER, path);
    logObjectEvent(logger, testCase.event, makePayload(kHead, kTail));
    logger.outputLogs();

    const auto event = parseSingleEvent(path);
    expectEventControls(event, testCase);
    const auto& data = event["data"];
    const auto* length = payloadLength(data, testCase);
    if (!length) {
      appendViolation(violations, testCase, "payload length missing");
    } else if (testCase.datagram) {
      if (length->asInt() != kExpectedLength) {
        appendViolation(violations, testCase, "payload length mismatch");
      }
    } else if (length->asString() != std::to_string(kExpectedLength)) {
      appendViolation(violations, testCase, "payload length mismatch");
    }
  }
  EXPECT_TRUE(violations.empty()) << violations;
}

TEST_F(FileMLoggerTest, LoggerDoesNotRetainObjectBackingStorage) {
  std::string violations;
  for (size_t i = 0; i < kObjectEvents.size(); ++i) {
    const auto& testCase = kObjectEvents[i];
    if (!testCase.retainsBackingStorage) {
      continue;
    }
    SCOPED_TRACE(testCase.name);
    ReleaseCounter counter;
    {
      FileMLogger logger(
          VantagePoint::SERVER,
          (dir_ / ("retention-" + std::to_string(i) + ".mlog")).string());
      logObjectEvent(logger, testCase.event, makeOwnedPayload(counter));
      if (counter.count != 2) {
        appendViolation(violations, testCase, "backing storage retained");
      }
    }
    if (counter.count != 2) {
      appendViolation(violations, testCase, "teardown did not release storage");
    }
  }
  EXPECT_TRUE(violations.empty()) << violations;
}

} // namespace moxygen
