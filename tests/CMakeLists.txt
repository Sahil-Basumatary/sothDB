file(GLOB_RECURSE TEST_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/storage/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/recovery/*.cpp"
)

add_executable(sothdb_tests ${TEST_SOURCES})
target_link_libraries(sothdb_tests PRIVATE sothdb_engine GTest::gtest_main)
include(GoogleTest)
gtest_discover_tests(sothdb_tests)
