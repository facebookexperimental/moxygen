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

# EventBase-driven UDP I/O engine shared by both evb server and evb client.
moxygen_add_library(openmoq_pico_evb_base
  SRCS
    PicoQuicSocketHandler.cpp
  EXPORTED_DEPS
    openmoq_pico_base
    Folly::folly_io_async_async_udp_socket
)
if(APPLE)
  # RFC 3542 advanced IPv6 API (IPV6_PKTINFO, in6_pktinfo) must be unlocked
  # before any system headers are processed; do it via compile definition so
  # it precedes all transitive includes (e.g. from picoquic.h).
  target_compile_definitions(openmoq_pico_evb_base PRIVATE __APPLE_USE_RFC_3542)
endif()

# EventBase server: picoquic I/O driven by a caller-supplied folly::EventBase.
moxygen_add_library(openmoq_pico_evb_server_transport
  SRCS
    MoQPicoQuicEventBaseServer.cpp
  EXPORTED_DEPS
    openmoq_pico_evb_base
    openmoq_pico_server_base
    moxygen_events_moq_folly_executor_impl
)

if(BUILD_SAMPLES)
  add_subdirectory(sample)
endif()
