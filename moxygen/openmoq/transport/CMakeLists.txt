# For installed picoquic (e.g. getdeps); FetchContent builds define the target
# directly without going through find_package.
find_package(picoquic QUIET)
# Check for the imported target directly. picoquic_FOUND alone is not
# sufficient: upstream picoquic's cmake config (picoquic-config.cmake) sets
# picoquic_FOUND=TRUE but does not define the picoquic::picoquic-core target,
# so target_link_libraries below would fail at generate time.
if(TARGET picoquic::picoquic-core)
  message(STATUS "picoquic found, building pico transport")
  add_subdirectory(pico)
else()
  message(STATUS "picoquic not found, skipping pico transport")
endif()
