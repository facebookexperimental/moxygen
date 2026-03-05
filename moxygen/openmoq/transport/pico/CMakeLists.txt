# Lowest-level adapter: WebTransport over a picoquic connection.
# Used by all variants (native server, evb server, evb client).
moxygen_add_library(openmoq_pico_base
  SRCS
    PicoQuicWebTransport.cpp
  EXPORTED_DEPS
    proxygen::proxygen_http_webtransport
    proxygen::proxygen_http_webtransport_wt_stream_manager
    mvfst::mvfst_priority_http_priority_queue
    Folly::folly_logging_logging
    Folly::folly_network_address
    Folly::folly_string
    picoquic::picoquic-core
)

# Shared server base: ALPN selection, picoCallback, per-connection context,
# onNewConnectionImpl. Used by both the thread-based and EventBase servers.
moxygen_add_library(openmoq_pico_server_base
  SRCS
    MoQPicoServerBase.cpp
  EXPORTED_DEPS
    openmoq_pico_base
    moxygen_moq_server_base
    moxygen_events_moq_executor
    Folly::folly_function
)

# Thread-based server: picoquic runs its own network thread via
# picoquic_start_network_thread; bridged to moxygen via PicoQuicExecutor.
moxygen_add_library(openmoq_pico_server_transport
  SRCS
    PicoQuicExecutor.cpp
    MoQPicoQuicServer.cpp
  EXPORTED_DEPS
    openmoq_pico_server_base
)

if(BUILD_SAMPLES)
  add_subdirectory(sample)
endif()
