file(GLOB_RECURSE SOTHDB_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/storage/*.cpp"
)

add_library(sothdb_engine STATIC ${SOTHDB_SOURCES})
target_include_directories(sothdb_engine PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
