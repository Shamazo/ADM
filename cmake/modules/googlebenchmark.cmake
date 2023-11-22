# If the the target already exists, do not attempt to build it from source
if (NOT TARGET benchmark)
  MESSAGE(STATUS "no 'benchmark' target found. Building from source.")
  include(llvm-virtual)

  set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_USE_LIBCXX ${USE_LIBCXX} CACHE BOOL "" FORCE)

  include(external/CMakeLists.txt.googlebenchmark.in)

  export(TARGETS benchmark FILE GoogleBenchmarkConfig.cmake)
else ()
  get_target_property(existing_benchmark_source_dir benchmark SOURCE_DIR)
  MESSAGE(Status "Found existing 'benchmark' target. 'benchmark' SOURCE_DIR ${existing_benchmark_source_dir}")
endif ()



