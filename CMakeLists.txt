cmake_minimum_required(VERSION 3.16)
project(OLAPAnalysisCpp)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
find_package(PkgConfig REQUIRED)

# Find Arrow
find_package(Arrow QUIET)
if(NOT Arrow_FOUND)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(ARROW arrow)
    if(ARROW_FOUND)
        set(Arrow_FOUND TRUE)
        message(STATUS "Found Arrow via pkg-config")
    endif()
endif()

find_package(Parquet QUIET)
if(NOT Parquet_FOUND)
    pkg_check_modules(PARQUET parquet)
    if(PARQUET_FOUND)
        set(Parquet_FOUND TRUE)
        message(STATUS "Found Parquet via pkg-config")
    endif()
endif()

# Find DuckDB
pkg_check_modules(DUCKDB duckdb)
if(NOT DUCKDB_FOUND)
    # Fallback for manual installation
    find_library(DUCKDB_LIBRARY duckdb PATHS /usr/local/lib)
    find_path(DUCKDB_INCLUDE_DIR duckdb.h PATHS /usr/local/include)
    if(DUCKDB_LIBRARY AND DUCKDB_INCLUDE_DIR)
        set(DUCKDB_FOUND TRUE)
        set(DUCKDB_LIBRARIES ${DUCKDB_LIBRARY})
        set(DUCKDB_INCLUDE_DIRS ${DUCKDB_INCLUDE_DIR})
        message(STATUS "Found DuckDB manually: ${DUCKDB_LIBRARY}")
    endif()
endif()

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Create executables (only if dependencies are found)
if(Arrow_FOUND AND Parquet_FOUND)
    add_executable(arrow_olap_analysis
        src/main_arrow.cpp
        src/arrow_analyzer.cpp
    )
    
    # Link libraries for Arrow version
    if(TARGET Arrow::arrow_shared)
        target_link_libraries(arrow_olap_analysis
            Arrow::arrow_shared
            Parquet::parquet_shared
        )
    else()
        # Fallback for pkg-config
        target_link_libraries(arrow_olap_analysis
            ${ARROW_LIBRARIES}
            ${PARQUET_LIBRARIES}
        )
        target_include_directories(arrow_olap_analysis PRIVATE 
            ${ARROW_INCLUDE_DIRS} 
            ${PARQUET_INCLUDE_DIRS}
        )
    endif()
    
    message(STATUS "Arrow OLAP analysis will be built")
else()
    message(WARNING "Arrow or Parquet not found - skipping arrow_olap_analysis")
endif()

if(DUCKDB_FOUND)
    add_executable(duckdb_olap_analysis
        src/main_duckdb.cpp
        src/duckdb_analyzer.cpp
    )
    
    # Link libraries for DuckDB version
    target_link_libraries(duckdb_olap_analysis
        ${DUCKDB_LIBRARIES}
    )
    
    target_include_directories(duckdb_olap_analysis PRIVATE ${DUCKDB_INCLUDE_DIRS})
    if(DUCKDB_CFLAGS_OTHER)
        target_compile_options(duckdb_olap_analysis PRIVATE ${DUCKDB_CFLAGS_OTHER})
    endif()
    
    message(STATUS "DuckDB OLAP analysis will be built")
else()
    message(WARNING "DuckDB not found - skipping duckdb_olap_analysis")
endif()

# Set output directory (only for built targets)
set(BUILT_TARGETS "")
if(TARGET arrow_olap_analysis)
    list(APPEND BUILT_TARGETS arrow_olap_analysis)
endif()
if(TARGET duckdb_olap_analysis)
    list(APPEND BUILT_TARGETS duckdb_olap_analysis)
endif()

if(BUILT_TARGETS)
    set_target_properties(${BUILT_TARGETS}
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )
endif()

# Print build information
message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "Arrow found: ${Arrow_FOUND}")
message(STATUS "Parquet found: ${Parquet_FOUND}")
message(STATUS "DuckDB found: ${DUCKDB_FOUND}")
