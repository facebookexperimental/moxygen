# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the Apache 2.0 license found in the
# LICENSE file in the root directory of this source tree.

moxygen_add_library(moxygen_samples_util_qmux_relay_utils
  SRCS
    Utils.cpp
  DEPS
    moxygen_moq_qmux_client
    moxygen_moq_relay_session
    moxygen_moq_webtransport_client
    moxygen_moqclient
  EXPORTED_DEPS
    moxygen_moqclient_base
    moxygen_events_moq_folly_executor_impl
    proxygen::proxygen_utils_parse_url
    fizz::fizz
)
