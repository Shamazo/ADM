# If the gtest target already exists, do not attempt to build it from source
# This can happen if proteus is included as a dependency in another project that builds gtest
if (NOT TARGET gtest)
  MESSAGE(STATUS "no 'gtest' target found. Building from source.")
  include(external/CMakeLists.txt.gtest.in)
  set(GTEST gtest)
  set(GTEST_MAIN gtest_main)
  export(TARGETS ${GTEST} ${GTEST_MAIN} FILE GtestConfig.cmake)
else ()
  get_target_property(existing_gtest_source_dir gtest SOURCE_DIR)
  MESSAGE(Status "Found existing 'gtest' target. 'gtest' SOURCE_DIR ${existing_gtest_source_dir}")
endif ()


