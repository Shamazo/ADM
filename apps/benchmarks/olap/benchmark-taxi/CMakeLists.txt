include(proteus-experiment)
get_filename_component(APP_NAME ${CMAKE_CURRENT_SOURCE_DIR} NAME)

project(proteus-${APP_NAME} VERSION 0.1 LANGUAGES CXX)

add_executable(${PROJECT_NAME}
  benchmark-taxi.cpp
  )

# Depending on the olap libraries
target_link_libraries(${PROJECT_NAME}
  proteus::olap
  proteus::olap-flags
  proteus::olap-perf-util
  proteus::platform
  queries::taxi
  )

configure_proteus_experiment(${PROJECT_NAME})