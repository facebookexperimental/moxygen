# For installed picoquic (e.g. getdeps); FetchContent builds define the target
# directly without going through find_package.
find_package(picoquic QUIET)
if(picoquic_FOUND OR TARGET picoquic::picoquic-core)
  message(STATUS "picoquic found, building pico transport")
  add_subdirectory(pico)
else()
  message(STATUS "picoquic not found, skipping pico transport")
endif()
