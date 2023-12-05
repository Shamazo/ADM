# Set the name of the app from the folder name
get_filename_component(TS_NAME ${CMAKE_CURRENT_SOURCE_DIR} NAME)

add_proteus_unit_test(unit-${TS_NAME} proteus::codegen tests-expressions.cpp)

target_link_libraries(unit-${TS_NAME}
  PUBLIC
  proteus::codegen-test-environment
  proteus::codegen-test-main
  proteus::codegen-test-utils
  )
