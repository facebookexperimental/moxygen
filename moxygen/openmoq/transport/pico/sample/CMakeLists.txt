add_executable(pico_relay_server PicoRelayServer.cpp)
target_link_libraries(pico_relay_server
  openmoq_pico_transport
  moxygen_relay_moq_relay
  moxygen_moq_relay_session
  Folly::folly_init_init
)
