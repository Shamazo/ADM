include(proteus-experiment)
# Set the name of the app from the folder name
get_filename_component(APP_NAME ${CMAKE_CURRENT_SOURCE_DIR} NAME)


project(proteus${APP_NAME} VERSION 0.1 LANGUAGES CXX)
add_executable(${PROJECT_NAME}
  main.cpp
  prepared_queries/prepared-scans.cpp
  prepared_queries/q1_1_pushdown.cpp
  prepared_queries/q1_2_pushdown.cpp
  prepared_queries/q1_3_pushdown.cpp
  prepared_queries/q1_1_adaptive.cpp
  prepared_queries/q1_2_adaptive.cpp
  prepared_queries/q1_3_adaptive.cpp
  prepared_queries/q2_1_adaptive.cpp
  prepared_queries/q2_2_adaptive.cpp
  prepared_queries/q2_3_adaptive.cpp
  prepared_queries/q3_1_adaptive.cpp
  prepared_queries/q3_2_adaptive.cpp
  prepared_queries/q3_3_adaptive.cpp
  prepared_queries/q3_4_adaptive.cpp
  prepared_queries/q4_1_adaptive.cpp
  prepared_queries/q4_2_adaptive.cpp
  prepared_queries/q4_3_adaptive.cpp
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

configure_proteus_experiment(${PROJECT_NAME})

add_executable(selectivity-micros
  selectivity-micros-main.cpp
  prepared_queries/prepared-scans.cpp
  prepared_queries/q1_1_pushdown.cpp
  prepared_queries/q1_2_pushdown.cpp
  prepared_queries/q1_3_pushdown.cpp
  prepared_queries/q1_1_adaptive.cpp
  util.cpp
  prepared_queries/prepared-scan-sum-micro.cpp
)

# Depending on the olap libraries
target_link_libraries(selectivity-micros
  jemalloc
  codegen::olap_flags
  proteus::olap
  proteus::storage
  proteus::platform
  queries::ssb
  magic_enum::magic_enum
  ${CMAKE_DL_LIBS}
)

configure_proteus_experiment(selectivity-micros)

add_executable(adaptivity-micros
  adaptivity-micros-main.cpp
  prepared_queries/prepared-scans.cpp
  util.cpp
  prepared_queries/prepared-scan-sum-micro.cpp
  prepared_queries/q1_1_adaptive.cpp
  prepared_queries/q1_2_adaptive.cpp
  prepared_queries/q1_3_adaptive.cpp
  prepared_queries/q2_1_adaptive.cpp
  prepared_queries/q2_2_adaptive.cpp
  prepared_queries/q2_3_adaptive.cpp
  prepared_queries/q3_1_adaptive.cpp
  prepared_queries/q3_2_adaptive.cpp
  prepared_queries/q3_3_adaptive.cpp
  prepared_queries/q3_4_adaptive.cpp
  prepared_queries/q4_1_adaptive.cpp
  prepared_queries/q4_2_adaptive.cpp
  prepared_queries/q4_3_adaptive.cpp
)

# Depending on the olap libraries
target_link_libraries(adaptivity-micros
  jemalloc
  codegen::olap_flags
  proteus::olap
  proteus::storage
  proteus::platform
  queries::ssb
  magic_enum::magic_enum
  ${CMAKE_DL_LIBS}
)

configure_proteus_experiment(adaptivity-micros)

add_executable(taxi-adm
  taxi-main.cpp
  prepared_queries/taxi-q1.cpp
  prepared_queries/taxi-q2.cpp
  util.cpp
  prepared_queries/prepared-scans.cpp
  prepared_queries/prepared-scan-sum-micro.cpp
  prepared_queries/q1_1_adaptive.cpp
  prepared_queries/q1_2_adaptive.cpp
  prepared_queries/q1_3_adaptive.cpp
  prepared_queries/q2_1_adaptive.cpp
  prepared_queries/q2_2_adaptive.cpp
  prepared_queries/q2_3_adaptive.cpp
  prepared_queries/q3_1_adaptive.cpp
  prepared_queries/q3_2_adaptive.cpp
  prepared_queries/q3_3_adaptive.cpp
  prepared_queries/q3_4_adaptive.cpp
  prepared_queries/q4_1_adaptive.cpp
  prepared_queries/q4_2_adaptive.cpp
  prepared_queries/q4_3_adaptive.cpp
)

# Depending on the olap libraries
target_link_libraries(taxi-adm
  jemalloc
  codegen::olap_flags
  proteus::olap
  proteus::storage
  proteus::platform
  queries::taxi
  magic_enum::magic_enum
  ${CMAKE_DL_LIBS}
)

configure_proteus_experiment(taxi-adm)