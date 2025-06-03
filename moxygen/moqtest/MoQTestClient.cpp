// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "moxygen/moqtest/MoQTestClient.h"
#include "moxygen/MoQClient.h"
#include "moxygen/moqtest/Utils.h"

namespace moxygen {

DEFINE_int32(connect_timeout, 1000, "connect timeout in ms");
DEFINE_int32(transaction_timeout, 1000, "transaction timeout in ms");
const int kDefaultRequestId = 0;
const std::string kDefaultTrackName = "test";
const GroupOrder kDefaultGroupOrder = GroupOrder::OldestFirst;
const LocationType kDefaultLocationType = LocationType::NextGroupStart;
const uint64_t kDefaultEndGroup = 10;
const TrackAlias kDefaultTrackAlias = TrackAlias(0);

MoQTestClient::MoQTestClient(folly::EventBase* evb, proxygen::URL url)
    : moqClient_(std::make_unique<MoQClient>(evb, std::move(url))) {}

folly::coro::Task<void> MoQTestClient::connect(folly::EventBase* evb) {
  co_await moqClient_->setupMoQSession(
      std::chrono::milliseconds(FLAGS_connect_timeout),
      std::chrono::seconds(FLAGS_transaction_timeout),
      nullptr,
      shared_from_this());
  co_return;
}

void MoQTestClient::initialize() {
  // Create a receiver for the client
  subReceiver_ = std::make_shared<ObjectReceiver>(
      ObjectReceiver::SUBSCRIBE,
      std::shared_ptr<MoQTestClient>(shared_from_this()));
  fetchReceiver_ = std::make_shared<ObjectReceiver>(
      ObjectReceiver::FETCH,
      std::shared_ptr<MoQTestClient>(shared_from_this()));
}

folly::coro::Task<moxygen::TrackNamespace> MoQTestClient::subscribe(
    MoQTestParameters params) {
  auto trackNamespace = convertMoqTestParamToTrackNamespace(&params);

  // Create a SubRequest with the created TrackNamespace as its fullTrackName
  SubscribeRequest sub;
  sub.requestID = kDefaultRequestId;

  FullTrackName ftn;
  ftn.trackNamespace = trackNamespace.value();
  ftn.trackName = kDefaultTrackName;

  sub.fullTrackName = ftn;
  sub.trackAlias = kDefaultTrackAlias;
  sub.groupOrder = kDefaultGroupOrder;
  sub.locType = kDefaultLocationType;
  sub.endGroup = kDefaultEndGroup;

  // Set Current Request
  receivingType_ = ReceivingType::SUBSCRIBE;
  initializeExpecteds(params);

  // Subscribe to the reciever
  auto res = co_await moqClient_->moqSession_->subscribe(sub, subReceiver_);

  if (!res.hasError()) {
    subHandle_ = res.value();
  } else {
    XLOG(ERR) << "SUBSCRIBE ERROR";
  }

  co_return trackNamespace.value();
}

folly::coro::Task<moxygen::TrackNamespace> MoQTestClient::fetch(
    MoQTestParameters params) {
  auto trackNamespace = convertMoqTestParamToTrackNamespace(&params);

  // Create a Fetch with the created TrackNamespace as its fullTrackName
  Fetch fetch;
  fetch.requestID = kDefaultRequestId;

  FullTrackName ftn;
  ftn.trackNamespace = trackNamespace.value();
  ftn.trackName = kDefaultTrackName;
  fetch.fullTrackName = ftn;
  fetch.groupOrder = kDefaultGroupOrder;

  // Set Current Request
  receivingType_ = ReceivingType::FETCH;
  initializeExpecteds(params);

  // Fetch to the reciever
  auto res = co_await moqClient_->moqSession_->fetch(fetch, fetchReceiver_);
  if (!res.hasError()) {
    fetchHandle_ = res.value();
  } else {
    XLOG(ERR) << "FETCH ERROR";
  }

  co_return trackNamespace.value();
}

ObjectReceiverCallback::FlowControlState MoQTestClient::onObject(
    const ObjectHeader& objHeader,
    Payload payload) {
  XLOG(DBG1) << "Calling onObject" << std::endl;

  ObjectHeader header = objHeader;
  // Validate the received data
  if (!validateSubscribedData(header, payload->toString())) {
    XLOG(ERR) << "onObject: Data Validation Failed" << std::endl;
    if (receivingType_ == ReceivingType::SUBSCRIBE) {
      subHandle_->unsubscribe();
    } else if (receivingType_ == ReceivingType::FETCH) {
      fetchHandle_->fetchCancel();
    }
    moqClient_->moqSession_->close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return ObjectReceiverCallback::FlowControlState::BLOCKED;
  }

  // Adjust the expected data
  switch (params_.forwardingPreference) {
    case (ForwardingPreference::ONE_SUBGROUP_PER_GROUP): {
      if (!adjustExpectedForOneSubgroupPerGroup(params_)) {
        XLOG(DBG1) << "onObject: No more data to be expected" << std::endl;
        return ObjectReceiverCallback::FlowControlState::BLOCKED;
      }
      break;
    }
    default: {
      break;
    }
  }

  return ObjectReceiverCallback::FlowControlState::UNBLOCKED;
}

void MoQTestClient::onObjectStatus(const ObjectHeader& objHeader) {
  XLOG(DBG1) << "onObjectStatus" << std::endl;

  ObjectHeader header = objHeader;
  // Validate the received data
  if (header.status != ObjectStatus::END_OF_GROUP) {
    XLOG(ERR) << "Unknown status received: " << header.status << std::endl;
    return;
  }

  if (!params_.sendEndOfGroupMarkers) {
    XLOG(ERR) << "End of Group Marker Recieved When Not Expected" << std::endl;
    return;
  }

  if (header.id != params_.lastObjectInTrack) {
    XLOG(ERR) << "Object Id Mismatch For End of Group Marker: Actual="
              << header.id << "  Expected=" << params_.lastObjectInTrack
              << std::endl;
    return;
  }

  // Adjust the expected data
  switch (params_.forwardingPreference) {
    case (ForwardingPreference::ONE_SUBGROUP_PER_GROUP): {
      if (!adjustExpectedForOneSubgroupPerGroup(params_)) {
        XLOG(DBG1) << "onObject: No more data to be expected" << std::endl;
        return;
      }
      break;
    }
    default: {
      break;
    }
  }
}

void MoQTestClient::onEndOfStream() {
  XLOG(DBG1) << "onEndOfStream" << std::endl;
}

void MoQTestClient::onError(ResetStreamErrorCode) {
  XLOG(DBG1) << "onError" << std::endl;
}
void MoQTestClient::onSubscribeDone(SubscribeDone done) {
  XLOG(DBG1) << "onSubscribeDone" << std::endl;

  switch (params_.forwardingPreference) {
    case (ForwardingPreference::ONE_SUBGROUP_PER_GROUP): {
      // If we are expecting more objects -> returns true
      if (adjustExpectedForOneSubgroupPerGroup(params_)) {
        subHandle_->unsubscribe();
        XLOG(ERR) << "SubscribeDone recieved while objects are still expected";
      }
      break;
    }
    default: {
      break;
    }
  }
}

bool MoQTestClient::validateSubscribedData(
    const ObjectHeader& header,
    const std::string& payload) {
  // Validate Group, Object Id, SubGroup (and End of Group Markers if
  // applicable)
  if (header.group != expectedGroup_) {
    XLOG(ERR) << "Group Mismatch: Actual=" << header.group
              << "  Expected=" << expectedGroup_ << std::endl;
    return false;
  }

  if (header.subgroup != expectedSubgroup_) {
    XLOG(ERR) << "SubGroup Mismatch: Actual=" << header.subgroup
              << "  Expected=" << expectedSubgroup_ << std::endl;
    return false;
  }

  if (header.id != expectedObjectId_) {
    XLOG(ERR) << "Object Id Mismatch: Actual=" << header.id
              << "  Expected=" << expectedObjectId_ << std::endl;
    return false;
  }

  // Validate End of Group
  if (header.id == params_.lastObjectInTrack && expectEndOfGroup_) {
    if (header.status != ObjectStatus::END_OF_GROUP) {
      XLOG(ERR) << "End of Group Mismatch: Actual=" << header.status
                << "  Expected=" << ObjectStatus::END_OF_GROUP << std::endl;
      return false;
    }
  }

  // Validate Extensions have been made
  auto result = validateExtensions(header.extensions, &params_);
  if (result.hasError()) {
    XLOG(ERR) << "Extension Error=" << std::to_string(result.error().code)
              << " Reason=" << result.error().reason << std::endl;
    return false;
  }

  // Validate Payload
  int objectSize = moxygen::getObjectSize(expectedObjectId_, &params_);
  if (!validatePayload(objectSize, payload)) {
    XLOG(ERR) << "Payload Mismatch: Actual=" << payload
              << "  Expected=" << std::string(objectSize, 't') << std::endl;
    return false;
  }

  return true;
}

bool MoQTestClient::adjustExpectedForOneSubgroupPerGroup(
    MoQTestParameters& params) {
  // Adjust Expected Group and ObjectId
  if (expectedGroup_ < params.lastGroupInTrack &&
      expectedObjectId_ == params.lastObjectInTrack) {
    expectedGroup_ += params.groupIncrement;
    expectedObjectId_ = params.startObject;
  } else if (expectedObjectId_ < params.lastObjectInTrack) {
    expectedObjectId_ += params.objectIncrement;
  } else {
    return false;
  }
  return true;
}

folly::Expected<folly::Unit, ExtensionError> MoQTestClient::validateExtensions(
    std::vector<Extension> extensions,
    MoQTestParameters* params) {
  // validate extension size
  if (!validateExtensionSize(extensions, params)) {
    int expectedAmount = (int)(params->testIntegerExtension >= 0) +
        (int)(params->testVariableExtension >= 0);
    ExtensionError error{
        ExtensionErrorCode::INVALID_EXTENSION_AMOUNT,
        folly::to<std::string>(
            "Invalid Extensions Amount-> Expected size: ",
            expectedAmount,
            " Actual size: ",
            extensions.size())};
    return folly::makeUnexpected(error);
  }

  // Get Extensions
  Extension intExt;
  Extension varExt;
  for (Extension ext : extensions) {
    if (ext.type % 2 == 0) {
      intExt = ext;
    } else {
      varExt = ext;
    }
  }

  // validate integer extensions
  if (params->testIntegerExtension >= 0 &&
      !validateIntExtensions(intExt, params)) {
    ExtensionError error{
        ExtensionErrorCode::INVALID_INT_EXTENSION,
        folly::to<std::string>(
            "Invalid Integer Extension-> Expected id: ",
            (2 * params->testIntegerExtension),
            " Actual id: ",
            intExt.type)};
    return folly::makeUnexpected(error);
  }

  // validate variable extensions
  if (params->testVariableExtension >= 0 &&
      (!validateVarExtensions(varExt, params))) {
    ExtensionError error{
        ExtensionErrorCode::INVALID_VAR_EXTENSION,
        folly::to<std::string>(
            "Invalid Variable Extension-> Expected id: ",
            2 * params->testVariableExtension + 1,
            " Actual id: ",
            varExt.type)};
    return folly::makeUnexpected(error);
  }

  // Return Validated
  return folly::Unit({});
}

void MoQTestClient::initializeExpecteds(MoQTestParameters& params) {
  params_ = params;
  expectedGroup_ = params.startGroup;
  expectedObjectId_ = params.startObject;
  expectedSubgroup_ = 0;
  expectEndOfGroup_ = params.sendEndOfGroupMarkers;
}

} // namespace moxygen
