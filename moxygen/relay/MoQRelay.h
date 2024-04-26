#pragma once

#include "moxygen/MoQSession.h"
#include "moxygen/relay/MoQForwarder.h"

#include <folly/container/F14Set.h>
#include <list>

namespace moxygen {

class MoQRelay {
 public:
  void setAllowedNamespacePrefix(std::string allowed) {
    allowedNamespacePrefix_ = std::move(allowed);
  }

  void onAnnounce(Announce&& ann, std::shared_ptr<MoQSession> session);
  folly::coro::Task<void> onSubscribe(
      SubscribeRequest subReq,
      std::shared_ptr<MoQSession> session);

  void removeSession(const std::shared_ptr<MoQSession>& session);

 private:
  struct RelaySubscription {
    std::shared_ptr<MoQForwarder> forwarder;
    std::shared_ptr<MoQSession> upstream;
    uint64_t subscribeID;
    folly::CancellationSource cancellationSource;
  };
  folly::coro::Task<void> forwardTrack(
      std::shared_ptr<MoQSession::TrackHandle> track,
      std::shared_ptr<MoQForwarder> forwarder);

  std::string allowedNamespacePrefix_;
  folly::F14FastMap<std::string, std::shared_ptr<MoQSession>> announces_;
  folly::F14FastMap<FullTrackName, RelaySubscription, FullTrackName::hash>
      subscriptions_;
};

} // namespace moxygen
