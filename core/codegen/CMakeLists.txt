project(codegen VERSION 0.1 LANGUAGES CXX)

# Pure, regular C++ files
set(codegen_cxx
  lib/context/context.cpp
  lib/expressions/expressionTypes.cpp
  lib/expressions/indexed-seq.cpp
  lib/jit/control-flow/if-statement.cpp
  )

add_library(${PROJECT_NAME} SHARED ${codegen_cxx})

# If we have compiler requirements for this library, list them
# here
target_compile_features(${PROJECT_NAME}
  PUBLIC
  cxx_std_20
  )

# Depend on a library that we defined in the top-level file
target_link_libraries(${PROJECT_NAME}
  PUBLIC
  LLVM::LLVM
  proteus::platform
  )

include(proteus-lib)
add_library(proteus::${PROJECT_NAME} ALIAS ${PROJECT_NAME})

if (UNIT_TESTS)
  add_subdirectory(tests)
endif ()
