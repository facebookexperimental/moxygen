/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include "moxygen/moqtest/MoQTestClient.h"

#include <utility>
#include "moxygen/MoQClient.h"
#include "moxygen/MoQWebTransportClient.h"
#include "moxygen/moqtest/Utils.h"
#include "moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h"

namespace moxygen {

DEFINE_int32(connect_timeout, 1000, "connect timeout in ms");
DEFINE_int32(transaction_timeout, 1000, "transaction timeout in ms");
const int kDefaultRequestId = 0;
const std::string kDefaultTrackName = "test";
const GroupOrder kDefaultGroupOrder = GroupOrder::OldestFirst;
const LocationType kDefaultLocationType = LocationType::NextGroupStart;
const uint64_t kDefaultEndGroup = 10;
const TrackAlias kDefaultTrackAlias = TrackAlias(0);

MoQTestClient::MoQTestClient(
    folly::EventBase* evb,
    proxygen::URL url,
    bool useQuicTransport)
    : moqExecutor_(std::make_shared<MoQFollyExecutorImpl>(evb)),
      moqClient_(
          useQuicTransport
              ? std::make_unique<MoQClient>(
                    moqExecutor_,
                    std::move(url),
                    std::make_shared<
                        test::InsecureVerifierDangerousDoNotUseInProduction>())
              : std::make_unique<MoQWebTransportClient>(
                    moqExecutor_,
                    std::move(url),
                    std::make_shared<
                        test::
                            InsecureVerifierDangerousDoNotUseInProduction>())),

      subReceiver_(
          std::make_shared<ObjectReceiver>(
              ObjectReceiver::SUBSCRIBE,
              std::shared_ptr<ObjectReceiverCallback>(
                  std::shared_ptr<void>(),
                  &objectReceiverCallback_))),
      fetchReceiver_(
          std::make_shared<ObjectReceiver>(
              ObjectReceiver::FETCH,
              std::shared_ptr<ObjectReceiverCallback>(
                  std::shared_ptr<void>(),
                  &objectReceiverCallback_))) {}

void MoQTestClient::setLogger(const std::shared_ptr<MLogger>& logger) {
  moqClient_->setLogger(logger);
}

folly::coro::Task<void> MoQTestClient::doSubscribeUpdate(
    std::shared_ptr<Publisher::SubscriptionHandle> handle,
    SubscribeUpdate update) {
  auto result = co_await handle->subscribeUpdate(std::move(update));
  if (result.hasError()) {
    XLOG(ERR) << "subscribeUpdate failed: error code="
              << static_cast<uint64_t>(result.error().errorCode)
              << ", reason=" << result.error().reasonPhrase;
  } else {
    XLOG(INFO) << "subscribeUpdate succeeded: requestID="
               << result.value().requestID.value;
  }
}

void MoQTestClient::subscribeUpdate(SubscribeUpdate update) {
  XLOG(DBG1) << "MoQTest DEBUGGING: calling subscribeUpdate";
  if (receivingType_ == ReceivingType::SUBSCRIBE && subHandle_) {
    folly::coro::co_withExecutor(
        moqExecutor_->getBackingEventBase(),
        doSubscribeUpdate(subHandle_, std::move(update)))
        .start();
  }
}

folly::coro::Task<void> MoQTestClient::connect(folly::EventBase* evb) {
  // Test client always uses experimental protocols for testing
  std::vector<std::string> alpns = getDefaultMoqtProtocols(true);

  co_await moqClient_->setupMoQSession(
      std::chrono::milliseconds(FLAGS_connect_timeout),
      std::chrono::seconds(FLAGS_transaction_timeout),
      nullptr,
      nullptr,
      [] {
        quic::TransportSettings ts;
        ts.orderedReadCallbacks = true;
        return ts;
      }(),
      alpns);

  co_return;
}

folly::coro::Task<moxygen::TrackNamespace> MoQTestClient::subscribe(
    MoQTestParameters params) {
  auto trackNamespace = convertMoqTestParamToTrackNamespace(params);
  if (trackNamespace.hasError()) {
    XLOG(ERR)
        << "MoQTest verification result: "
        << "FAILURE! Reason: Error Converting Parameters to TrackNamespace: "
        << trackNamespace.error().what();
    moqClient_->moqSession_->drain();
    co_yield folly::coro::co_error(trackNamespace.error());
  }

  // Create a SubRequest with the created TrackNamespace as its fullTrackName
  SubscribeRequest sub;
  sub.requestID = kDefaultRequestId;
  requestID_ = kDefaultRequestId;

  FullTrackName ftn;
  ftn.trackNamespace = trackNamespace.value();
  ftn.trackName = kDefaultTrackName;

  sub.fullTrackName = ftn;
  sub.trackAlias = kDefaultTrackAlias;
  sub.groupOrder = kDefaultGroupOrder;
  sub.locType = kDefaultLocationType;
  sub.endGroup = kDefaultEndGroup;

  // Add delivery timeout parameter if configured
  if (params.deliveryTimeout > 0) {
    sub.params.insertParam(
        {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
         params.deliveryTimeout});
  }

  // Set Current Request
  receivingType_ = ReceivingType::SUBSCRIBE;
  initializeExpecteds(params);

  // Subscribe to the reciever
  auto res = co_await moqClient_->moqSession_->subscribe(sub, subReceiver_);
  moqClient_->moqSession_->drain();

  if (!res.hasError()) {
    subHandle_ = res.value();
  } else {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! Reason: Error Subscribing to receiver. "
        << "Error code: " << static_cast<uint64_t>(res.error().errorCode)
        << ", Reason: " << res.error().reasonPhrase;
    co_yield folly::coro::co_error(
        std::runtime_error(
            folly::to<std::string>(
                "Error code: ",
                static_cast<uint64_t>(res.error().errorCode),
                ", Reason: ",
                res.error().reasonPhrase)));
  }

  co_return trackNamespace.value();
}

folly::coro::Task<moxygen::TrackNamespace> MoQTestClient::fetch(
    MoQTestParameters params) {
  auto trackNamespace = convertMoqTestParamToTrackNamespace(params);
  if (trackNamespace.hasError()) {
    XLOG(ERR)
        << "MoQTest verification result: "
        << "FAILURE! Reason: Error Converting Parameters to TrackNamespace: "
        << trackNamespace.error().what();
    moqClient_->moqSession_->drain();
    co_yield folly::coro::co_error(trackNamespace.error());
  }

  // Create a Fetch with the created TrackNamespace as its fullTrackName
  Fetch fetch;
  fetch.requestID = kDefaultRequestId;
  requestID_ = kDefaultRequestId;

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
  moqClient_->moqSession_->drain();
  if (!res.hasError()) {
    fetchHandle_ = res.value();
  } else {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! Reason: Error Fetching to receiver. "
        << "Error code: " << static_cast<uint64_t>(res.error().errorCode)
        << ", Reason: " << res.error().reasonPhrase;
    co_yield folly::coro::co_error(
        std::runtime_error(
            folly::to<std::string>(
                "Error code: ",
                static_cast<uint64_t>(res.error().errorCode),
                ", Reason: ",
                res.error().reasonPhrase)));
  }

  co_return trackNamespace.value();
}

ObjectReceiverCallback::FlowControlState MoQTestClient::onObject(
    const folly::Optional<TrackAlias>& /* trackAlias */,
    const ObjectHeader& objHeader,
    Payload payload) {
  XLOG(DBG1) << "MoQTest DEBUGGING: Calling onObject";

  // Validate the received data
  if (!validateSubscribedData(objHeader, payload->toString())) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Data Validation Failed";
    if (receivingType_ == ReceivingType::SUBSCRIBE) {
      subHandle_->unsubscribe();
    } else if (receivingType_ == ReceivingType::FETCH) {
      fetchHandle_->fetchCancel();
    }
    moqClient_->moqSession_->close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return ObjectReceiverCallback::FlowControlState::BLOCKED;
  }

  // Adjust the expected data (If Still recieving data, leave unblocked)
  auto result = adjustExpected(params_, &objHeader);
  if (result == AdjustedExpectedResult::STILL_RECEIVING_DATA) {
    return ObjectReceiverCallback::FlowControlState::UNBLOCKED;
  } else {
    return ObjectReceiverCallback::FlowControlState::BLOCKED;
  }
}

void MoQTestClient::onObjectStatus(
    const folly::Optional<TrackAlias>& /* trackAlias */,
    const ObjectHeader& objHeader) {
  XLOG(DBG1) << "MoQTest DEBUGGING: calling onObjectStatus";

  ObjectHeader header = objHeader;
  // Validate the received data
  if (header.status != ObjectStatus::END_OF_GROUP) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Unknown object status received: "
        << header.status;
    return;
  }

  if (!params_.sendEndOfGroupMarkers) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: End of Group Marker Recieved When Not Expected";
    return;
  }

  if (header.id != params_.lastObjectInTrack) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Object Id Mismatch For End of Group Marker: Actual="
        << header.id << "  Expected=" << params_.lastObjectInTrack;
    return;
  }

  // Adjust the expected data
  if (adjustExpected(params_, &objHeader) ==
      AdjustedExpectedResult::RECEIVED_ALL_DATA) {
    XLOG(DBG1)
        << "MoQTest DEBUGGING: onObjectStatus: No more data to be expected";
  }
}

void MoQTestClient::onEndOfStream() {
  XLOG(DBG1) << "MoQTest DEBUGGING: calling onEndOfStream";
}

void MoQTestClient::onError(ResetStreamErrorCode) {
  XLOG(DBG1) << "MoQTest DEBUGGING: calling onError";
}
void MoQTestClient::onAllDataReceived() {
  XLOG(DBG1) << "MoQTest DEBUGGING: onAllDataReceived";
  // Ensure subHandle_ is reset at the end of this function, even if an early
  // return occurs
  auto subHandleResetGuard = folly::makeGuard([this] { subHandle_.reset(); });

  if (params_.forwardingPreference == ForwardingPreference::DATAGRAM) {
    if (datagramObjects_ == 0) {
      XLOG(ERR)
          << "MoQTest verification result: FAILURE! reason: Datagram Failed - 0 Objects Recieved";
      subHandle_->unsubscribe();
      return;
    } else {
      XLOG(INFO) << "MoQTest verification result: SUCCESS! Datagram Recieved "
                 << datagramObjects_ << " objects";
      return;
    }
  }
  if (params_.forwardingPreference != ForwardingPreference::DATAGRAM &&
      adjustExpected(params_, nullptr) ==
          AdjustedExpectedResult::STILL_RECEIVING_DATA) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: SubscribeDone recieved while objects are still expected";
    subHandle_->unsubscribe();
    return;
  }

  XLOG(INFO) << "MoQTest verification result: SUCCESS! All Data Recieved";
}

bool MoQTestClient::validateSubscribedData(
    const ObjectHeader& header,
    const std::string& payload) {
  // Validate Group, Object Id, SubGroup (and End of Group Markers if
  // applicable)
  XLOG(DBG1) << "MoQTest DEBUGGING: Expected Group=" << expectedGroup_
             << " Expected ObjectId="
             << subgroupToExpectedObjId_[header.subgroup];
  XLOG(DBG1) << "MoQTest DEBUGGING: Object Group=" << header.group
             << " end of group markers=" << params_.sendEndOfGroupMarkers
             << " expected end of group markers=" << expectEndOfGroup_;
  if (params_.forwardingPreference != ForwardingPreference::DATAGRAM &&
      header.group != expectedGroup_) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Group Mismatch: Actual="
        << header.group << "  Expected=" << expectedGroup_;
    return false;
  }

  if (params_.forwardingPreference ==
          ForwardingPreference::ONE_SUBGROUP_PER_GROUP &&
      header.subgroup != expectedSubgroup_) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: SubGroup Mismatch: Actual="
        << header.subgroup << "  Expected=" << expectedSubgroup_;
    return false;
  }

  // Validate function for Datagram Objects
  if (params_.forwardingPreference == ForwardingPreference::DATAGRAM) {
    if (!validateDatagramObjects(header)) {
      return false;
    }
  }

  // Validate subgroup ID according to forwarding preference
  if ((params_.forwardingPreference ==
           ForwardingPreference::ONE_SUBGROUP_PER_GROUP &&
       header.subgroup != 0) ||
      (params_.forwardingPreference ==
           ForwardingPreference::TWO_SUBGROUPS_PER_GROUP &&
       header.subgroup > 1)) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: SubGroup Mismatch: Actual="
        << header.subgroup << "  Expected="
        << (params_.forwardingPreference ==
                    ForwardingPreference::ONE_SUBGROUP_PER_GROUP
                ? "0"
                : (params_.forwardingPreference ==
                           ForwardingPreference::TWO_SUBGROUPS_PER_GROUP
                       ? "0 or 1"
                       : "N/A"));
    return false;
  }

  if (params_.forwardingPreference != ForwardingPreference::DATAGRAM &&
      params_.forwardingPreference !=
          ForwardingPreference::ONE_SUBGROUP_PER_OBJECT &&
      header.id != subgroupToExpectedObjId_[header.subgroup]) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Object Id Mismatch: Actual="
        << header.id
        << "  Expected=" << subgroupToExpectedObjId_[header.subgroup]
        << " (Subgroup=" << header.subgroup << ")";
    return false;
  }

  // Validate End of Group
  if (header.id == params_.lastObjectInTrack && expectEndOfGroup_) {
    if (header.status != ObjectStatus::END_OF_GROUP) {
      XLOG(ERR)
          << "MoQTest verification result: FAILURE! reason: End of Group Mismatch: Actual="
          << header.status << "  Expected=" << ObjectStatus::END_OF_GROUP;
      return false;
    }
  }

  // Validate Extensions have been made
  auto result =
      validateExtensions(header.extensions.getMutableExtensions(), &params_);
  if (result.hasError()) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Extension Error="
        << std::to_string(result.error().code)
        << " Reason=" << result.error().reason;
    return false;
  }

  // Validate Payload
  int objectSize = moxygen::getObjectSize(header.id, &params_);
  if (!validatePayload(objectSize, payload)) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Payload Mismatch: Actual="
        << payload << "  Expected=" << std::string(objectSize, 't');
    return false;
  }

  return true;
}

AdjustedExpectedResult MoQTestClient::adjustExpectedForOneSubgroupPerGroup(
    MoQTestParameters& params) {
  // Adjust Expected Group and ObjectId
  if (expectedGroup_ < params.lastGroupInTrack &&
      subgroupToExpectedObjId_[0] == params.lastObjectInTrack) {
    expectedGroup_ += params.groupIncrement;
    subgroupToExpectedObjId_[0] = params.startObject;
  } else if (subgroupToExpectedObjId_[0] < params.lastObjectInTrack) {
    subgroupToExpectedObjId_[0] += params.objectIncrement;
  } else {
    return AdjustedExpectedResult::RECEIVED_ALL_DATA;
  }
  return AdjustedExpectedResult::STILL_RECEIVING_DATA;
}

AdjustedExpectedResult MoQTestClient::adjustExpectedForOneSubgroupPerObject(
    MoQTestParameters& params) {
  // Adjust Expected Group, ObjectId and Subgroup
  if (expectedGroup_ < params.lastGroupInTrack &&
      subgroupToExpectedObjId_[0] == params.lastObjectInTrack) {
    // Increment Group, Reset ObjectId and Subgroup
    expectedGroup_ += params.groupIncrement;
    subgroupToExpectedObjId_[0] = params.startObject;
    expectedSubgroup_ = 0;
  } else if (subgroupToExpectedObjId_[0] < params.lastObjectInTrack) {
    // Increment ObjectId and Subgroup
    subgroupToExpectedObjId_[0] += params.objectIncrement;
    expectedSubgroup_ += params.objectIncrement;
  } else {
    return AdjustedExpectedResult::RECEIVED_ALL_DATA;
  }
  return AdjustedExpectedResult::STILL_RECEIVING_DATA;
}

AdjustedExpectedResult MoQTestClient::adjustExpectedForTwoSubgroupsPerGroup(
    const ObjectHeader* header,
    MoQTestParameters& params) {
  auto subgroup =
      header ? header->subgroup : ((params.lastObjectInTrack & 1) ? 1 : 0);
  // Adjust Expected Group, ObjectId and Subgroup
  if (expectedGroup_ < params.lastGroupInTrack &&
      subgroupToExpectedObjId_[subgroup] >= params.lastObjectInTrack) {
    // Increment Group, Reset ObjectId and Subgroup
    expectedGroup_ += params.groupIncrement;
    subgroupToExpectedObjId_[params.startObject & 1] = params.startObject;
    subgroupToExpectedObjId_[!(params.startObject & 1)] =
        params.startObject + params.objectIncrement;
  } else if (subgroupToExpectedObjId_[subgroup] < params.lastObjectInTrack) {
    // Increment ObjectId for this subgroup.  If increment is odd, increment
    // twice
    subgroupToExpectedObjId_[subgroup] += params.objectIncrement;
    if (params.objectIncrement % 2 == 1) {
      subgroupToExpectedObjId_[subgroup] += params.objectIncrement;
    }
  } else {
    return AdjustedExpectedResult::RECEIVED_ALL_DATA;
  }
  return AdjustedExpectedResult::STILL_RECEIVING_DATA;
}

AdjustedExpectedResult MoQTestClient::adjustExpectedForDatagram(
    MoQTestParameters& params) {
  // Adjust Object Count
  datagramObjects_++;
  // Only Complete if expectedGroup_ and subgroupToExpectedObjId_ are at the end
  if (expectedGroup_ == params_.lastGroupInTrack &&
      subgroupToExpectedObjId_[0] == params_.lastObjectInTrack) {
    return AdjustedExpectedResult::RECEIVED_ALL_DATA;
  }
  return AdjustedExpectedResult::STILL_RECEIVING_DATA;
}

folly::Expected<folly::Unit, ExtensionError> MoQTestClient::validateExtensions(
    const std::vector<Extension>& extensions,
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
  for (const Extension& ext : extensions) {
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
  if (params.forwardingPreference ==
      ForwardingPreference::TWO_SUBGROUPS_PER_GROUP) {
    subgroupToExpectedObjId_[params.startObject & 1] = params.startObject;
    subgroupToExpectedObjId_[!(params.startObject & 1)] =
        params.startObject + params.objectIncrement;
  } else {
    subgroupToExpectedObjId_[0] = params.startObject;
  }
  expectedSubgroup_ = 0;
  expectEndOfGroup_ = params.sendEndOfGroupMarkers;

  // Only relevant for Datagram Forwarding Preference
  datagramObjects_ = 0;
}

AdjustedExpectedResult MoQTestClient::adjustExpected(
    MoQTestParameters& params,
    const ObjectHeader* header) {
  switch (params_.forwardingPreference) {
    case (ForwardingPreference::ONE_SUBGROUP_PER_GROUP): {
      return adjustExpectedForOneSubgroupPerGroup(params);
      break;
    }
    case (ForwardingPreference::ONE_SUBGROUP_PER_OBJECT): {
      return adjustExpectedForOneSubgroupPerObject(params);
      break;
    }
    case (ForwardingPreference::TWO_SUBGROUPS_PER_GROUP): {
      return adjustExpectedForTwoSubgroupsPerGroup(header, params);
      break;
    }
    case (ForwardingPreference::DATAGRAM): {
      if (receivingType_ == ReceivingType::FETCH) {
        XLOG(ERR)
            << "MoQTest verification result: FAILURE! reason: Datagram Forwarding Preference Not Supported For Fetch";
        return AdjustedExpectedResult::ERROR_RECEIVING_DATA;
      }
      return adjustExpectedForDatagram(params);
      break;
    }
    default: {
      break;
    }
  }

  return AdjustedExpectedResult::ERROR_RECEIVING_DATA;
}

bool MoQTestClient::validateDatagramObjects(const ObjectHeader& header) {
  // Validate Datagram Group and ObjectId

  // Group Must be Properly incremented
  if (header.group % params_.groupIncrement != 0) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Datagram Group Mismatch: Actual="
        << header.group << "Expected Increment of " << params_.groupIncrement;
    return false;
  }

  // Group Must be before last group in track
  if (header.group > params_.lastGroupInTrack) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Datagram Group Mismatch: Actual="
        << header.group << "Can't be greater than last group "
        << params_.lastGroupInTrack;
    return false;
  }

  // Object Id Must be Properly incremented
  if (header.id % params_.objectIncrement != 0) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Datagram Object Id Mismatch: Actual="
        << header.id << "Expected Increment of " << params_.objectIncrement;
    return false;
  }

  // Object Id Must be before last object in track
  if (header.id > params_.lastObjectInTrack) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Datagram Object Id Mismatch: Actual="
        << header.id << "Can't be greater than last object "
        << params_.lastObjectInTrack;
    return false;
  }

  return true;
}

folly::coro::Task<void> MoQTestClient::trackStatus(TrackStatus req) {
  co_await moqClient_->moqSession_->trackStatus(req);
}

} // namespace moxygen
