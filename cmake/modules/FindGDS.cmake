find_package(PkgConfig)
pkg_check_modules(PC_GDS QUIET gds)

# TODO hardcoded
# Proteus currently only works with cuda 11
# cuda 12 currently fails because it adds a static assert:
# '#error "libc++ is not supported on x86 system"'
# and we use libc++ (the LLVM version) instead of libstdc++ (the gcc version) because our LLVM links to it
# until we change to libc++ we are stuck on cuda 11
# however we want GDS >= v1.7 as it adds support for cuda streams
# cuda 12.2.1 has v1.7.1 https://docs.nvidia.com/cuda/archive/12.2.1/cuda-toolkit-release-notes/index.html
# cuda 12.3.1 has v1.8.1
# cuda 11.7.1 has v1.3.1
# cufile.so does not link to any other nvidia toolkit libraries and I think just depends on the kernel driver / nvidia-fs modules
# it does link to libstdc++.so.6, but this is fine because none of std:: is used in the header/interface
#find_path(GDS_INCLUDE_DIRS
#    NAMES cufile.h
#    HINTS /scratch/cufile
#    )
#
#find_library(GDS_LIBRARIES
#    NAMES cufile.so.1.8.1
#    HINTS /usr/local/cuda-12.3/lib64
#    )

set(GDS_INCLUDE_DIRS  /scratch/cufile)

if(EXISTS "/usr/local/cuda-12.3/lib64/libcufile.so.1.8.1")
    set(GDS_LIBRARIES  /usr/local/cuda-12.3/lib64/libcufile.so.1.8.1)
elseif(EXISTS "/usr/local/cuda-12.2/lib64/libcufile.so.1.7.1")
    set(GDS_LIBRARIES  /usr/local/cuda-12.2/lib64/libcufile.so.1.7.1)
endif()


set(GDS_VERSION ${PC_GDS_VERSION})

if(GDS_LIBRARIES AND GDS_INCLUDE_DIRS AND NOT TARGET NVIDIA::GDS)
    set(GDS_FOUND TRUE)
    add_library(NVIDIA::GDS UNKNOWN IMPORTED)
    set_target_properties(NVIDIA::GDS
        PROPERTIES
            IMPORTED_LOCATION ${GDS_LIBRARIES}
            INTERFACE_INCLUDE_DIRECTORIES ${GDS_INCLUDE_DIRS}
        )
    message(STATUS "NVIDIA GDS found: ${GDS_INCLUDE_DIRS}, ${GDS_LIBRARIES}")
else()
    set(GDS_FOUND FALSE)
endif()

mark_as_advanced(GDS_FOUND GDS_INCLUDE_DIRS GDS_LIBRARIES GDS_VERSION)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GDS
    DEFAULT_MSG GDS_INCLUDE_DIRS GDS_LIBRARIES
    )
