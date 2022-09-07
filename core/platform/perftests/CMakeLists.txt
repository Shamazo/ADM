function(add_proteus_platform_perftest target)
  add_proteus_perftest(${target} ${ARGN})
  target_link_libraries(${target} PUBLIC proteus::platform-perf-main)
endfunction()

add_subdirectory(platform-perf-main)

add_subdirectory(queue-perf)