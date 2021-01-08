find_package(PkgConfig)
pkg_check_modules(PC_GDS QUIET gds)

find_path(GDS_INCLUDE_DIRS
    NAMES cufile.h
    HINTS ${CUDA_TOOLKIT_ROOT_DIR}/lib64
    )

find_library(GDS_LIBRARIES
    NAMES cufile
    HINTS ${CUDA_TOOLKIT_ROOT_DIR}/lib64
    )

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
