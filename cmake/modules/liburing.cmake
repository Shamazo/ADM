include(external/CMakeLists.txt.liburing.in)


# This is the easiest way to add a configure && make install dependency. Inspired by https://stackoverflow.com/questions/15175318/cmake-how-to-build-external-projects-and-include-their-targets
# ExternalProject_Add wont work because we need to compile this at configure time to have the location of the header file
# FetchContent_* and friends don't seem to let you specify build and configure commands, though it may work for header only libs, e.g https://stackoverflow.com/questions/65586352/is-it-possible-to-use-fetchcontent-or-an-equivalent-to-add-a-library-that-has-no
execute_process(COMMAND ./configure --prefix=${liburing-download_BINARY_DIR} --cc=${CMAKE_C_COMPILER} --cxx=${CMAKE_CXX_COMPILER}
  WORKING_DIRECTORY ${liburing-download_SOURCE_DIR})
execute_process(COMMAND make install
  WORKING_DIRECTORY ${liburing-download_SOURCE_DIR})


add_library(liburing STATIC IMPORTED)
add_dependencies(liburing liburing-download)


SET(LIBURING_INCLUDE_DIR ${liburing-download_BINARY_DIR}/include)
SET(LIBURING_LIB_DIR ${liburing-download_BINARY_DIR}/lib)

set_target_properties(liburing PROPERTIES IMPORTED_LOCATION ${liburing-download_BINARY_DIR}/liburing.a)
set_target_properties(liburing PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${LIBURING_INCLUDE_DIR})

