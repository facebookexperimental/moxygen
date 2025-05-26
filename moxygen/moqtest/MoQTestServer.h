// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "moxygen/MoQServer.h"
#include "moxygen/Publisher.h"

namespace moxygen {

class MoQTestServer : public moxygen::Publisher,
                      public moxygen::MoQServer,
                      public std::enable_shared_from_this<MoQTestServer> {
 public:
  MoQTestServer(uint16_t port);

  // Override onNewSession to set publisher handler to be this object
  virtual void onNewSession(
      std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(shared_from_this());
  }

 private:
};

} // namespace moxygen
