project(codegen-test-environment VERSION 0.1 LANGUAGES CXX)

add_library(${PROJECT_NAME} SHARED
  lib/pipeline-environment.cpp
  )

target_link_libraries(${PROJECT_NAME}
  PUBLIC
  proteus::codegen
  )

# Add the gtest libraries, as this is a "test" library
proteus_target_link_test_libraries(${PROJECT_NAME})

include(proteus-lib)
add_library(proteus::${PROJECT_NAME} ALIAS ${PROJECT_NAME})
