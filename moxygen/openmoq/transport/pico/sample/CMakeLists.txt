add_executable(pico_relay_server PicoRelayServer.cpp)
target_link_libraries(pico_relay_server
  openmoq_pico_server_transport
  moxygen_relay_moq_relay
  moxygen_moq_relay_session
  Folly::folly_init_init
)

add_executable(pico_evb_relay_server PicoEventBaseRelayServer.cpp)
target_link_libraries(pico_evb_relay_server
  openmoq_pico_evb_server_transport
  moxygen_relay_moq_relay
  moxygen_moq_relay_session
  Folly::folly_init_init
)

add_executable(pico_evb_text_client PicoTextClient.cpp)
target_link_libraries(pico_evb_text_client
  openmoq_pico_evb_client_transport
  moxygen_moq
  moxygen_moq_versions
  proxygen::proxygen_utils_parse_url
  Folly::folly_init_init
)
