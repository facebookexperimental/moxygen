# Regression guard for the per-layer XLOG category rooting in ../CMakeLists.txt:
# drives a short mvfst session and asserts each layer's --logging selector scopes
# to that layer.
add_test(
  NAME xlog_category_roots
  COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/xlog_category_smoke.sh
          $<TARGET_FILE:moqdateserver> $<TARGET_FILE:moqtextclient>
)
set_tests_properties(xlog_category_roots PROPERTIES
  LABELS "logging;smoke" TIMEOUT 180)

# Extension point for tests that exist only in a downstream tree.
include(${CMAKE_CURRENT_SOURCE_DIR}/local.cmake OPTIONAL)
