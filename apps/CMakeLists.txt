add_subdirectory(standalones)

# Benchmarks
if (NOT PROTEUS_PLATFORM_ONLY)
  add_subdirectory(benchmarks/olap)
  add_subdirectory(benchmarks/oltp)
  add_subdirectory(benchmarks/htap)
endif()