# Lowest-level adapter: WebTransport over a picoquic connection.
# Used by all variants (native server, evb server, evb client).
moxygen_add_library(openmoq_pico_base
  SRCS
    PicoWebTransportBase.cpp
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
# PicoConnectionContext.h includes PicoH3WebTransport.h which needs h3zero.
# Propagate picohttp include path to all consumers via BUILD_INTERFACE
# (avoids CMake policy error for build-dir paths in INTERFACE_INCLUDE_DIRECTORIES).
if(TARGET picohttp-core)
  target_include_directories(openmoq_pico_base PUBLIC
    $<BUILD_INTERFACE:${picoquic_SOURCE_DIR}/picohttp>)
  target_link_libraries(openmoq_pico_base PUBLIC
    $<TARGET_FILE:picohttp-core>)
endif()

# Shared server base: ALPN selection, picoCallback, per-connection context,
# onNewConnectionImpl. Used by both the thread-based and EventBase servers.
moxygen_add_library(openmoq_pico_server_base
  SRCS
    MoQPicoServerBase.cpp
  EXPORTED_DEPS
    openmoq_pico_base
    openmoq_pico_h3_webtransport
    moxygen_moq_server_base
    moxygen_events_moq_executor
    Folly::folly_function
)

# HTTP/3 WebTransport adapter for picoquic (h3 ALPN, browser clients).
# Depends on picohttp/h3zero; linked separately to avoid exporting picohttp
# as a public dependency of openmoq_pico_server_base.
moxygen_add_library(openmoq_pico_h3_webtransport
  SRCS
    PicoH3WebTransport.cpp
  EXPORTED_DEPS
    openmoq_pico_base
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

# EventBase client: outgoing QUIC connection + MoQ session on a folly::EventBase.
moxygen_add_library(openmoq_pico_evb_client_transport
  SRCS
    MoQPicoQuicEventBaseClient.cpp
  EXPORTED_DEPS
    openmoq_pico_evb_base
    moxygen_moq
    moxygen_events_moq_folly_executor_impl
)

if(BUILD_SAMPLES)
  add_subdirectory(sample)
endif()

