# Set the name of the app from the folder name
get_filename_component(APP_NAME ${CMAKE_CURRENT_SOURCE_DIR} NAME)


project(proteus${APP_NAME} VERSION 0.1 LANGUAGES CXX)
add_executable(${PROJECT_NAME}
  main.cpp
  prepared_queries/prepared-scans.cpp
  prepared_queries/q1_1_pushdown.cpp
  prepared_queries/q1_2_pushdown.cpp
  prepared_queries/q1_3_pushdown.cpp
  util.cpp
  prepared_queries/prepared-scan-sum-micro.cpp
  )

# Depending on the olap libraries
target_link_libraries(${PROJECT_NAME}
  jemalloc
  codegen::olap_flags
  proteus::olap
  proteus::storage
  proteus::platform
  queries::ssb
  magic_enum::magic_enum
  ${CMAKE_DL_LIBS}
  )

include(proteus-experiment)
