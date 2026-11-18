# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the Apache 2.0 license found in the
# LICENSE file in the root directory of this source tree.

moxygen_add_library(moxygen_samples_lane_manager_scripted_policy
  SRCS
    ScriptedPolicy.cpp
  EXPORTED_DEPS
    moxygen_lane_manager_switching_policy
)
