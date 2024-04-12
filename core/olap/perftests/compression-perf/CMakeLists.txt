get_filename_component(TS_NAME ${CMAKE_CURRENT_SOURCE_DIR} NAME)

add_proteus_perftest(perftest-${TS_NAME} proteus::olap cpu-benchmarks.cpp compression-perf.cpp gpu-benchmarks.cpp)

# not 100% sure we need to link lz4/nvcomp again since we are not directly calling/including them
set(COMPRESSION_PERF_LIBS jemalloc lz4_static proteus::olap proteus::olap-flags)

find_package(nvcomp)
if (nvcomp_FOUND)
  list(APPEND COMPRESSION_PERF_LIBS nvcomp::nvcomp)
  target_compile_definitions(perftest-${TS_NAME} PRIVATE HAVE_NVCOMP)
endif ()

target_link_libraries(perftest-${TS_NAME}
  PUBLIC
  ${COMPRESSION_PERF_LIBS}
  )


