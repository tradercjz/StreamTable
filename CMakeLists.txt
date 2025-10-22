cmake_minimum_required(VERSION 3.16)
project(StreamingCompute VERSION 1.0.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)

# Find Arrow
find_package(Arrow REQUIRED)

# Include directories
include_directories(${CMAKE_SOURCE_DIR}/include)

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")

# Source files
set(SOURCES
    src/stream_table.cpp
    src/publisher.cpp
    src/subscriber.cpp
    src/stream_engine.cpp
    src/time_series_engine.cpp
    src/reactive_state_engine.cpp
    src/cross_sectional_engine.cpp
    src/cep_engine.cpp
    src/persistence_manager.cpp
    src/monitoring.cpp
    src/streaming_system.cpp
    src/utils.cpp
)

# Create library
add_library(streaming_compute STATIC ${SOURCES})

# Link libraries
target_link_libraries(streaming_compute
    Arrow::arrow_shared
    Threads::Threads
)

# Examples
add_subdirectory(examples)

# Tests
enable_testing()
add_subdirectory(tests)

# Install targets
install(TARGETS streaming_compute
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

install(DIRECTORY include/
    DESTINATION include
    FILES_MATCHING PATTERN "*.h"
)