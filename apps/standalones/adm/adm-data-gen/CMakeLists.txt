include(proteus-experiment)
# Set the name of the app from the folder name
get_filename_component(APP_NAME ${CMAKE_CURRENT_SOURCE_DIR} NAME)


project(proteus${APP_NAME} VERSION 0.1 LANGUAGES CXX)
add_executable(${PROJECT_NAME}
  main.cpp
  )

# Depending on the olap libraries
target_link_libraries(${PROJECT_NAME}
  jemalloc
  glog::glog
  cli-flags::cli-flags
  )

configure_proteus_experiment(${PROJECT_NAME})
