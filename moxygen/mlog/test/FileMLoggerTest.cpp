/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/mlog/FileMLogger.h>
#include <moxygen/mlog/MLogTypes.h>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/ManualExecutor.h>
#include <folly/json/json.h>
#include <folly/portability/GTest.h>
#include <quic/codec/QuicConnectionId.h>
#include <stdlib.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
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
     "object_payload",
     false,
     true},
    {ObjectEvent::SUBGROUP_PARSED,
     "moqt:subgroup_object_parsed",
     "object_payload",
     false,
     true},
    {ObjectEvent::FETCH_CREATED,
     "moqt:fetch_object_created",
     "object_payload",
     false,
     true},
    {ObjectEvent::FETCH_PARSED,
     "moqt:fetch_object_parsed",
     "object_payload",
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

std::string readLogFile(const std::string& path) {
  std::ifstream input(path);
  return std::string(std::istreambuf_iterator<char>(input), {});
}

folly::dynamic parseLogFile(const std::string& path) {
  return folly::parseJson(readLogFile(path));
}

folly::dynamic parseSingleEvent(const std::string& path) {
  return parseLogFile(path)["traces"][0]["events"][0];
}

void expectLowercaseKeys(const folly::dynamic& value) {
  if (value.isObject()) {
    for (const auto& [key, child] : value.items()) {
      EXPECT_EQ(
          key.asString().find_first_of("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
          std::string::npos)
          << key.asString();
      expectLowercaseKeys(child);
    }
  } else if (value.isArray()) {
    for (const auto& child : value) {
      expectLowercaseKeys(child);
    }
  }
}

void expectEventControls(
    const folly::dynamic& event,
    const ObjectEventCase& testCase) {
  ASSERT_TRUE(event.isObject());
  EXPECT_EQ(event["name"].asString(), testCase.name);
  const auto* data = event.get_ptr("data");
  ASSERT_NE(data, nullptr);
  const auto* objectId =
      data->get_ptr(testCase.datagram ? "object_id" : "object_id_delta");
  ASSERT_NE(objectId, nullptr);
  EXPECT_EQ(objectId->asInt(), 9);
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
    return data.get_ptr("object_payload_length");
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

class InspectableMLogger final : public MLogger {
 public:
  InspectableMLogger() : MLogger(VantagePoint::SERVER) {}

  void outputLogs() override {}

  folly::dynamic event(size_t index) {
    return formatLog(logs_.at(index));
  }
};

class InspectableFileMLogger final : public FileMLogger {
 public:
  using FileMLogger::FileMLogger;

  void setEventTime(size_t index, std::chrono::system_clock::duration elapsed) {
    logs_.at(index).time_ = static_cast<uint64_t>(elapsed.count());
  }
};

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

TEST_F(FileMLoggerTest, EmptyLogIsAJsonDocument) {
  const auto path = (dir_ / "empty.mlog").string();
  FileMLogger logger(VantagePoint::SERVER, path);
  logger.outputLogs();

  const auto document = parseLogFile(path);
  ASSERT_EQ(document["traces"].size(), 1);
  EXPECT_TRUE(document["traces"][0]["events"].empty());
}

TEST_F(FileMLoggerTest, QlogSchemaMetadata) {
  const auto path = (dir_ / "metadata.mlog").string();
  FileMLogger logger(VantagePoint::SERVER, path);
  logger.logPaddingDatagramCreated(17);
  logger.outputLogs();

  const auto serialized = readLogFile(path);
  const auto document = folly::parseJson(serialized);
  EXPECT_EQ(
      document["file_schema"].asString(),
      "urn:ietf:params:qlog:file:contained");
  EXPECT_EQ(
      document["serialization_format"].asString(), "application/qlog+json");
  const auto& trace = document["traces"][0];
  EXPECT_EQ(
      trace["event_schemas"],
      folly::dynamic::array("urn:ietf:params:qlog:events:moqt-08"));
  const folly::dynamic expectedTimeContext =
      folly::dynamic::object("time_format", "relative_to_epoch")(
          "reference_time",
          folly::dynamic::object("clock_type", "system")("epoch", "unknown"));
  EXPECT_EQ(trace["common_fields"], expectedTimeContext);

  const auto header = serialized.substr(0, 256);
  for (const auto* field :
       {"file_schema",
        "urn:ietf:params:qlog:file:contained",
        "serialization_format",
        "application/qlog+json"}) {
    EXPECT_NE(header.find(field), std::string::npos) << field;
  }
}

TEST_F(FileMLoggerTest, VantagePointIsTraceMetadata) {
  for (const auto& [vantagePoint, type] :
       {std::pair{VantagePoint::CLIENT, "client"},
        std::pair{VantagePoint::SERVER, "server"}}) {
    SCOPED_TRACE(type);
    const auto path = (dir_ / (std::string(type) + ".mlog")).string();
    FileMLogger logger(vantagePoint, path);
    logger.logPaddingDatagramCreated(17);
    logger.logPaddingDatagramParsed(19);
    logger.outputLogs();

    const auto document = parseLogFile(path);
    const auto& trace = document["traces"][0];
    const folly::dynamic expectedPoint = folly::dynamic::object("type", type);
    EXPECT_EQ(trace["vantage_point"], expectedPoint);
    for (const auto& event : trace["events"]) {
      EXPECT_EQ(event.count("vantagePoint"), 0);
      EXPECT_EQ(event.count("vantage_point"), 0);
    }
    expectLowercaseKeys(document);
  }
}

TEST_F(FileMLoggerTest, LegacyUnsubscribeUsesLowercaseJsonKeys) {
  const auto path = (dir_ / "unsubscribe.mlog").string();
  FileMLogger logger(VantagePoint::CLIENT, path);
  MOQTControlMessageCreated created;
  created.streamId = 17;
  auto unsubscribe = std::make_unique<MOQTUnsubscribe>();
  unsubscribe->subscribeId = 23;
  created.message = std::move(unsubscribe);
  logger.addControlMessageCreatedLog(std::move(created));
  logger.outputLogs();

  const auto document = parseLogFile(path);
  const auto& message = document["traces"][0]["events"][0]["data"]["message"];
  EXPECT_EQ(message["subscribe_id"], "23");
  expectLowercaseKeys(document);
}

TEST_F(FileMLoggerTest, MultipleEventsAreAJsonDocument) {
  const auto path = (dir_ / "multiple.mlog").string();
  FileMLogger logger(VantagePoint::SERVER, path);
  logger.logPaddingDatagramCreated(17);
  logger.logPaddingDatagramParsed(19);
  logger.outputLogs();

  const auto document = parseLogFile(path);
  ASSERT_EQ(document["traces"].size(), 1);
  const auto& events = document["traces"][0]["events"];
  std::vector<std::pair<std::string, int64_t>> observed;
  for (const auto& event : events) {
    observed.emplace_back(
        event["name"].asString(), event["data"]["padding_length"].asInt());
  }
  const std::vector<std::pair<std::string, int64_t>> expected{
      {"moqt:padding_datagram_created", 17},
      {"moqt:padding_datagram_parsed", 19}};
  EXPECT_EQ(observed, expected);
}

TEST_F(FileMLoggerTest, EventTimeIsNumericMilliseconds) {
  const auto path = (dir_ / "time.mlog").string();
  InspectableFileMLogger logger(VantagePoint::SERVER, path);
  logger.logPaddingDatagramCreated(17);
  logger.setEventTime(
      0,
      std::chrono::duration_cast<std::chrono::system_clock::duration>(
          std::chrono::microseconds(12345)));
  logger.outputLogs();

  const auto event = parseSingleEvent(path);
  ASSERT_TRUE(event["time"].isDouble());
  EXPECT_DOUBLE_EQ(event["time"].asDouble(), 12.345);
}

TEST_F(FileMLoggerTest, OutputLogsWritesTheLatestSnapshot) {
  const auto path = (dir_ / "snapshot.mlog").string();
  FileMLogger logger(VantagePoint::SERVER, path);
  logger.logPaddingDatagramCreated(17);
  logger.outputLogs();
  ASSERT_EQ(parseLogFile(path)["traces"][0]["events"].size(), 1);

  logger.logPaddingDatagramParsed(19);
  logger.outputLogs();
  const auto events = parseLogFile(path)["traces"][0]["events"];
  std::vector<int64_t> observed;
  for (const auto& event : events) {
    observed.push_back(event["data"]["padding_length"].asInt());
  }
  const std::vector<int64_t> expected{17, 19};
  EXPECT_EQ(observed, expected);
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
    } else if (length->asInt() != kExpectedLength) {
      appendViolation(violations, testCase, "payload length mismatch");
    }
  }
  EXPECT_TRUE(violations.empty()) << violations;
}

TEST_F(FileMLoggerTest, ObjectsWithoutAPayloadLogZeroLength) {
  std::string violations;
  for (size_t i = 0; i < kObjectEvents.size(); ++i) {
    const auto& testCase = kObjectEvents[i];
    if (testCase.datagram) {
      continue;
    }
    SCOPED_TRACE(testCase.name);
    const auto path =
        (dir_ / ("empty-" + std::to_string(i) + ".mlog")).string();
    FileMLogger logger(VantagePoint::SERVER, path);
    logObjectEvent(logger, testCase.event, nullptr);
    logger.outputLogs();

    const auto event = parseSingleEvent(path);
    expectEventControls(event, testCase);
    const auto* length = payloadLength(event["data"], testCase);
    if (!length || length->asInt() != 0) {
      appendViolation(violations, testCase, "payload length not zero");
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

// From draft 16 on, PUBLISH_NAMESPACE_DONE and PUBLISH_NAMESPACE_CANCEL carry
// a request ID and an empty namespace.
TEST_F(FileMLoggerTest, PublishNamespaceDoneByRequestId) {
  const auto path = (dir_ / "pubns_done.mlog").string();
  FileMLogger logger(VantagePoint::SERVER, path);
  PublishNamespaceDone done;
  done.requestID = RequestID(7);
  logger.logPublishNamespaceDone(
      done, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  logger.outputLogs();

  const auto event = parseSingleEvent(path);
  const auto& message = event["data"]["message"];
  EXPECT_EQ(message["request_id"].asInt(), 7);
  EXPECT_EQ(message.count("trackNamespace"), 0);
}

TEST_F(FileMLoggerTest, PublishNamespaceCancelByRequestId) {
  const auto path = (dir_ / "pubns_cancel.mlog").string();
  FileMLogger logger(VantagePoint::SERVER, path);
  PublishNamespaceCancel cancel;
  cancel.requestID = RequestID(9);
  cancel.errorCode = RequestErrorCode::INTERNAL_ERROR;
  logger.logPublishNamespaceCancel(
      cancel, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  logger.outputLogs();

  const auto event = parseSingleEvent(path);
  const auto& message = event["data"]["message"];
  EXPECT_EQ(message["request_id"].asInt(), 9);
  EXPECT_EQ(message.count("track_namespace"), 0);
}

// These messages carry only a request ID, so it is the one field that
// identifies them in the log.
TEST(MLogTypesTest, NamespaceMessagesLogRequestId) {
  MOQTPublishNamespaceDone done;
  done.requestId = 3;
  EXPECT_EQ(done.toDynamic()["request_id"], 3);

  MOQTPublishNamespaceCancel cancel;
  cancel.requestId = 5;
  EXPECT_EQ(cancel.toDynamic()["request_id"], 5);

  MOQTUnsubscribeNamespace unsubscribe;
  unsubscribe.requestId = 7;
  auto obj = unsubscribe.toDynamic();
  EXPECT_EQ(obj["request_id"], 7);
  EXPECT_EQ(obj.count("requestID"), 0);
}

TEST(MLogTypesTest, LatestTransportEventFieldNames) {
  MOQTStreamTypeSet streamType;
  streamType.owner = Owner::REMOTE;
  streamType.streamId = 7;
  streamType.streamType = MOQTStreamType::PADDING;
  auto stream = streamType.toDynamic();
  EXPECT_EQ(stream["owner"], "remote");
  EXPECT_EQ(stream["stream_id"], 7);
  EXPECT_EQ(stream["stream_type"], "padding");
  EXPECT_EQ(stream.count("streamId"), 0);

  MOQTSubgroupHeaderCreated header;
  header.streamId = 11;
  header.trackAlias = 12;
  header.groupId = 13;
  header.subgroupIdMode = 2;
  header.containsEndOfGroup = true;
  header.propertiesPresent = true;
  header.firstObject = true;
  auto subgroup = header.toDynamic();
  EXPECT_EQ(subgroup["subgroup_id_mode"], 2);
  EXPECT_EQ(subgroup["properties_present"], true);
  EXPECT_EQ(subgroup["first_object"], true);
  EXPECT_EQ(subgroup.count("extensions_present"), 0);

  MOQTFetchObjectCreated fetchObject;
  fetchObject.streamId = 21;
  fetchObject.endOfTimedOutRange = true;
  fetchObject.objectPayloadLength = 34;
  auto fetch = fetchObject.toDynamic();
  EXPECT_EQ(fetch["datagram"], false);
  EXPECT_EQ(fetch["end_of_nonexistent_range"], false);
  EXPECT_EQ(fetch["end_of_unknown_range"], false);
  EXPECT_EQ(fetch["end_of_timed_out_range"], true);
  EXPECT_EQ(fetch["object_payload_length"], 34);
  EXPECT_EQ(fetch.count("objectStatus"), 0);

  EXPECT_EQ(MOQTPaddingDatagramParsed{55}.toDynamic()["padding_length"], 55);
}

TEST(MLogTypesTest, FetchGroupDeltaFollowsObservedDirection) {
  ObjectHeader first(10, 1, 4, 12);
  ObjectHeader second(7, 1, 5, 12);

  InspectableMLogger created;
  created.logFetchHeaderCreated(17, 19);
  created.logFetchObjectCreated(17, first, nullptr);
  created.logFetchObjectCreated(17, second, nullptr);
  EXPECT_EQ(created.event(1)["data"]["group_id_delta"], 10);
  EXPECT_EQ(created.event(2)["data"]["group_id_delta"], 2);

  InspectableMLogger parsed;
  parsed.logFetchHeaderParsed(17, 19);
  parsed.logFetchObjectParsed(17, first, nullptr);
  parsed.logFetchObjectParsed(17, second, nullptr);
  EXPECT_EQ(parsed.event(1)["data"]["group_id_delta"], 10);
  EXPECT_EQ(parsed.event(2)["data"]["group_id_delta"], 2);
}

TEST(MLogTypesTest, FetchRangeEndLoggerApis) {
  static constexpr std::array<MOQTFetchRangeEndType, 3> kRangeEndTypes = {
      MOQTFetchRangeEndType::NONEXISTENT,
      MOQTFetchRangeEndType::UNKNOWN,
      MOQTFetchRangeEndType::TIMED_OUT};
  for (size_t i = 0; i < kRangeEndTypes.size(); ++i) {
    const auto type = kRangeEndTypes[i];
    const auto streamId = 40 + i;
    const auto endGroup = 50 + i;
    const auto endObject = 60 + i;
    auto verify = [&](const folly::dynamic& event) {
      const auto& data = event["data"];
      EXPECT_EQ(data["group_id_delta"], endGroup);
      EXPECT_EQ(data["object_id_delta"], endObject);
      EXPECT_EQ(data["object_payload_length"], 0);
      EXPECT_EQ(
          data["end_of_nonexistent_range"],
          type == MOQTFetchRangeEndType::NONEXISTENT);
      EXPECT_EQ(
          data["end_of_unknown_range"], type == MOQTFetchRangeEndType::UNKNOWN);
      EXPECT_EQ(
          data["end_of_timed_out_range"],
          type == MOQTFetchRangeEndType::TIMED_OUT);
    };

    InspectableMLogger created;
    created.logFetchObjectCreated(streamId, endGroup, endObject, type);
    verify(created.event(0));
    created.logFetchObjectCreated(
        streamId, ObjectHeader(endGroup, 0, endObject + 1), nullptr);
    EXPECT_EQ(created.event(1)["data"].count("group_id_delta"), 0);
    EXPECT_EQ(created.event(1)["data"].count("object_id_delta"), 0);

    InspectableMLogger parsed;
    parsed.logFetchObjectParsed(streamId, endGroup, endObject, type);
    verify(parsed.event(0));
  }
}

TEST_F(FileMLoggerTest, LatestSetupAndFillParameterSchemas) {
  const auto setupPath = (dir_ / "setup.mlog").string();
  FileMLogger setupLogger(VantagePoint::CLIENT, setupPath);
  struct Setup setup;
  ASSERT_TRUE(setup.params
                  .insertParam(Parameter(
                      folly::to_underlying(SetupKey::PATH), "/meeting"))
                  .hasValue());
  setupLogger.logClientSetup(setup, 20, ControlMessageType::CREATED);
  setupLogger.outputLogs();

  const auto setupEvent = parseSingleEvent(setupPath);
  const auto& setupMessage = setupEvent["data"]["message"];
  EXPECT_EQ(setupMessage["type"], "setup");
  EXPECT_EQ(setupMessage.count("version"), 0);
  EXPECT_EQ(setupMessage.count("number_of_parameters"), 0);
  ASSERT_EQ(setupMessage["setup_options"].size(), 1);
  EXPECT_EQ(setupMessage["setup_options"][0]["name"], "path");
  EXPECT_EQ(setupMessage["setup_options"][0]["value"], "/meeting");

  const auto subscribePath = (dir_ / "fill.mlog").string();
  FileMLogger subscribeLogger(VantagePoint::CLIENT, subscribePath);
  auto subscribe = SubscribeRequest::make(
      FullTrackName{TrackNamespace({"room"}), "audio"},
      42,
      GroupOrder::NewestFirst,
      false,
      LocationFilter::range({7, 8}, {9, 10}));
  subscribe.requestID = RequestID(11);
  subscribe.includeProperties = false;
  subscribe.fill = FillParameters{
      .locationFilter = LocationFilter::relativeStart(3),
      .subscriberPriority = 12,
      .groupOrder = GroupOrder::OldestFirst,
      .fillTimeout = std::chrono::milliseconds(13)};
  ASSERT_TRUE(subscribe.params
                  .insertParam(Parameter(
                      folly::to_underlying(
                          TrackRequestParamKey::OBJECT_DELIVERY_TIMEOUT),
                      14))
                  .hasValue());
  subscribeLogger.logSubscribe(subscribe);
  subscribeLogger.outputLogs();

  const auto subscribeEvent = parseSingleEvent(subscribePath);
  const auto& message = subscribeEvent["data"]["message"];
  EXPECT_EQ(message["track_namespace"][0]["value"], "room");
  EXPECT_EQ(message["track_name"]["value"], "audio");
  EXPECT_EQ(message.count("subscriber_priority"), 0);
  EXPECT_EQ(message.count("group_order"), 0);
  EXPECT_EQ(message.count("filter_type"), 0);
  const auto& parameters = message["parameters"];
  EXPECT_EQ(message["number_of_parameters"], parameters.size());
  std::vector<std::string> names;
  for (const auto& parameter : parameters) {
    names.push_back(parameter["name"].asString());
  }
  EXPECT_NE(
      std::find(names.begin(), names.end(), "object_delivery_timeout"),
      names.end());
  EXPECT_NE(
      std::find(names.begin(), names.end(), "location_filter"), names.end());
  EXPECT_NE(
      std::find(names.begin(), names.end(), "fill_parameters"), names.end());
  EXPECT_NE(
      std::find(names.begin(), names.end(), "subscriber_priority"),
      names.end());
  EXPECT_NE(std::find(names.begin(), names.end(), "group_order"), names.end());
  EXPECT_NE(std::find(names.begin(), names.end(), "forward"), names.end());
  EXPECT_NE(
      std::find(names.begin(), names.end(), "include_properties"), names.end());
}

TEST_F(FileMLoggerTest, LatestRangeFilterParameterSchema) {
  const auto path = (dir_ / "range_filter.mlog").string();
  FileMLogger logger(VantagePoint::CLIENT, path);
  RequestOk ok;
  ok.requestSpecificParams.emplace_back(
      0x29,
      std::string(
          "\x07\x02\x03\x02\x05\x05",
          6)); // set 7, property 2, ranges 3-5 and 10-15
  logger.logRequestOk(ok);
  logger.outputLogs();

  const auto event = parseSingleEvent(path);
  const auto& parameter = event["data"]["message"]["parameters"][0];
  EXPECT_EQ(parameter["name"], "track_property_filter");
  EXPECT_EQ(parameter["set_id"], 7);
  EXPECT_EQ(parameter["property_type"], 2);
  ASSERT_EQ(parameter["ranges"].size(), 2);
  EXPECT_EQ(parameter["ranges"][0]["start"], 3);
  EXPECT_EQ(parameter["ranges"][0]["end"], 5);
  EXPECT_EQ(parameter["ranges"][1]["start"], 10);
  EXPECT_EQ(parameter["ranges"][1]["end"], 15);
}

TEST_F(FileMLoggerTest, LatestFillParametersPreserveRangeFilters) {
  const auto path = (dir_ / "fill_range_filters.mlog").string();
  FileMLogger logger(VantagePoint::CLIENT, path);
  auto subscribe =
      SubscribeRequest::make(FullTrackName{TrackNamespace({"room"}), "audio"});
  subscribe.fill = FillParameters{.groupOrder = GroupOrder::OldestFirst};
  std::vector<Parameter> fillRangeFilters = {
      Parameter(0x25, std::string("\x01\x03\x02", 3)),
      Parameter(0x25, std::string("\x02\x07", 2)),
      Parameter(0x26, std::string("\x03\x09\x01", 3)),
      Parameter(0x27, std::string()),
      Parameter(0x28, std::string("\x04\x02\x05\x03", 4)),
  };
  logger.logSubscribe(subscribe, fillRangeFilters);
  logger.outputLogs();

  const auto event = parseSingleEvent(path);
  const auto& parameters = event["data"]["message"]["parameters"];
  const folly::dynamic* fill = nullptr;
  for (const auto& parameter : parameters) {
    if (parameter["name"] == "fill_parameters") {
      fill = &parameter;
      break;
    }
  }
  ASSERT_NE(fill, nullptr);
  const auto& values = (*fill)["value"];
  ASSERT_EQ(values.size(), 6);
  EXPECT_EQ(values[0]["name"], "group_order");
  EXPECT_EQ(values[1]["name"], "subgroup_filter");
  EXPECT_EQ(values[1]["set_id"], 1);
  EXPECT_EQ(values[1]["ranges"][0]["start"], 3);
  EXPECT_EQ(values[1]["ranges"][0]["end"], 5);
  EXPECT_EQ(values[2]["name"], "subgroup_filter");
  EXPECT_EQ(values[2]["set_id"], 2);
  EXPECT_EQ(values[2]["ranges"][0]["start"], 7);
  EXPECT_EQ(values[2]["ranges"][0].count("end"), 0);
  EXPECT_EQ(values[3]["name"], "objectid_filter");
  EXPECT_EQ(values[4]["name"], "priority_filter");
  EXPECT_EQ(values[4].count("set_id"), 0);
  EXPECT_EQ(values[5]["name"], "object_property_filter");
  EXPECT_EQ(values[5]["property_type"], 2);

  InspectableMLogger updateLogger;
  SubscribeUpdate update;
  updateLogger.logSubscribeUpdate(
      update, {Parameter(0x26, std::string("\x05\x0b", 2))});
  const auto updateEvent = updateLogger.event(0);
  EXPECT_EQ(
      updateEvent["data"]["message"]["parameters"][0]["name"],
      "fill_parameters");
}

TEST_F(FileMLoggerTest, LatestResponseAndNamespaceControlMessages) {
  const auto errorPath = (dir_ / "request_error.mlog").string();
  FileMLogger errorLogger(VantagePoint::SERVER, errorPath);
  RequestError error;
  error.requestID = RequestID(17);
  error.errorCode = RequestErrorCode::REDIRECT;
  error.reasonPhrase = "moved";
  error.retryInterval = std::chrono::milliseconds(18);
  error.redirect = Redirect{
      "https://new.example/moq",
      FullTrackName{TrackNamespace({"replacement"}), "video"}};
  errorLogger.logSubscribeError(error, ControlMessageType::PARSED);
  errorLogger.outputLogs();

  const auto errorEvent = parseSingleEvent(errorPath);
  const auto& errorMessage = errorEvent["data"]["message"];
  EXPECT_EQ(errorMessage["type"], "request_error");
  EXPECT_EQ(errorMessage["error_code"], static_cast<uint64_t>(error.errorCode));
  EXPECT_EQ(errorMessage["retry_interval"], 18);
  EXPECT_EQ(errorMessage["reason_phrase"], "moved");
  EXPECT_EQ(errorMessage.count("request_id"), 0);
  EXPECT_EQ(errorMessage["redirect"]["connect_uri"]["length"], 23);
  EXPECT_EQ(
      errorMessage["redirect"]["track_namespace"][0]["value"], "replacement");

  const auto okPath = (dir_ / "request_ok.mlog").string();
  FileMLogger okLogger(VantagePoint::SERVER, okPath);
  RequestOk ok;
  ok.requestID = RequestID(19);
  ok.trackProperties.insertMutableExtension(Extension(2, 20));
  okLogger.logPublishNamespaceOk(ok, ControlMessageType::PARSED);
  okLogger.outputLogs();

  const auto okEvent = parseSingleEvent(okPath);
  const auto& okMessage = okEvent["data"]["message"];
  EXPECT_EQ(okMessage["type"], "request_ok");
  EXPECT_EQ(okMessage["number_of_parameters"], 0);
  EXPECT_EQ(okMessage.count("request_id"), 0);
  EXPECT_EQ(okMessage["track_properties"][0]["type"], 2);
  EXPECT_EQ(okMessage["track_properties"][0]["value"], 20);

  MOQTSubscribeTracks subscribeTracks;
  subscribeTracks.requestId = 21;
  subscribeTracks.trackNamespacePrefix.emplace_back();
  subscribeTracks.trackNamespacePrefix[0].type =
      MOQTByteStringType::VALUE_BYTES;
  subscribeTracks.trackNamespacePrefix[0].valueBytes =
      folly::IOBuf::copyBuffer("ab", 2);
  auto subscribeTracksJson = subscribeTracks.toDynamic();
  EXPECT_EQ(subscribeTracksJson["type"], "subscribe_tracks");
  EXPECT_EQ(
      subscribeTracksJson["track_namespace_prefix"][0]["value_bytes"], "6162");

  MOQTPublishSkipped skipped;
  skipped.trackName.type = MOQTByteStringType::STRING_VALUE;
  skipped.trackName.value = "late";
  EXPECT_EQ(skipped.toDynamic()["type"], "publish_skipped");
}

} // namespace moxygen
