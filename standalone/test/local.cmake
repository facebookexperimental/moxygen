# Tests for transports that exist only in this tree. The pico sample binaries
# are built under BUILD_PICOQUIC.
if(TARGET pico_evb_relay_server AND TARGET pico_evb_text_client)
  add_test(
    NAME xlog_category_pico
    COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/xlog_category_pico_smoke.sh
            $<TARGET_FILE:pico_evb_relay_server> $<TARGET_FILE:pico_evb_text_client>
  )
  set_tests_properties(xlog_category_pico PROPERTIES
    LABELS "logging;smoke" TIMEOUT 180)
endif()
