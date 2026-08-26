# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the Apache 2.0 license found in the
# LICENSE file in the root directory of this source tree.

# util/ is added by the root CMakeLists.txt: the moqtest binaries need it even
# when BUILD_SAMPLES is off.

add_subdirectory(chat)
add_subdirectory(date)
add_subdirectory(flv_receiver_client)
add_subdirectory(flv_streamer_client)
add_subdirectory(media_server)
add_subdirectory(text-client)
