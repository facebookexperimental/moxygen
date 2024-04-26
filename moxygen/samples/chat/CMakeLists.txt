include(CMakePrintHelpers)

# MoQChatClient
add_executable(MoQChatClient MoQChatClient.cpp)
target_include_directories(MoQChatClient PRIVATE ${FOLLY_INCLUDE_DIR})
target_link_libraries(MoQChatClient PRIVATE
  ${FOLLY_LIBRARIES}
  ${FIZZ_LIBRARIES}
  ${Boost_LIBRARIES}
  ${OPENSSL_LIBRARIES}
  ${GLOG_LIBRARIES}
  ${GFLAGS_LIBRARIES}
  ${LIBEVENT_LIB}
  ${DOUBLE_CONVERSION_LIBRARY}
  ${CMAKE_DL_LIBS}
  ${LIBRT_LIBRARIES}
  ${mvfst_LIBRARIES}
  ${DEPS_LIBRARIES_DIR}/libwangle.a #TODO: fix wangle_LIBRARIES empty this by wangle exporting it
  ${DEPS_LIBRARIES_DIR}/libproxygen.a #TODO: fix PROXYGEN_LIBRARIES empty this by proxygen exporting it
  ${DEPS_LIBRARIES_DIR}/libproxygencurl.a
  ${DEPS_LIBRARIES_DIR}/libproxygendeviousbaton.a
  ${DEPS_LIBRARIES_DIR}/libproxygenhqloggerhelper.a
  ${DEPS_LIBRARIES_DIR}/libproxygenhttpserver.a
  Threads::Threads
  MoqClient
  MoqSession
  MoqFramer
  MoqCodec)

# MoQChatServer
add_executable(MoQChatServer MoQChatServer.cpp)
target_include_directories(MoQChatServer PRIVATE ${FOLLY_INCLUDE_DIR})
target_include_directories(MoQChatServer PRIVATE ${PROXYGEN_INCLUDES})
target_link_libraries(MoQChatServer PRIVATE
  ${FOLLY_LIBRARIES}
  ${FIZZ_LIBRARIES}
  ${Boost_LIBRARIES}
  ${OPENSSL_LIBRARIES}
  ${GLOG_LIBRARIES}
  ${GFLAGS_LIBRARIES}
  ${LIBEVENT_LIB}
  ${DOUBLE_CONVERSION_LIBRARY}
  ${CMAKE_DL_LIBS}
  ${LIBRT_LIBRARIES}
  ${mvfst_LIBRARIES}
  ${DEPS_LIBRARIES_DIR}/libwangle.a #TODO: fix wangle_LIBRARIES empty this by wangle exporting it
  ${DEPS_LIBRARIES_DIR}/libproxygen.a #TODO: fix PROXYGEN_LIBRARIES empty this by proxygen exporting it
  ${DEPS_LIBRARIES_DIR}/libproxygencurl.a
  ${DEPS_LIBRARIES_DIR}/libproxygendeviousbaton.a
  ${DEPS_LIBRARIES_DIR}/libproxygenhqloggerhelper.a
  ${DEPS_LIBRARIES_DIR}/libproxygenhttpserver.a
  ${DEPS_LIBRARIES_DIR}/libproxygenhqserver.a
  Threads::Threads
  MoqServer
  MoqSession
  MoqFramer
  MoqCodec
  MoqRelay)
