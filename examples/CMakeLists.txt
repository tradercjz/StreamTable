cmake_minimum_required(VERSION 3.16)

# Basic streaming example
add_executable(basic_streaming basic_streaming.cpp)
target_link_libraries(basic_streaming streaming_compute Arrow::arrow_shared Threads::Threads)

# Time series engine example
add_executable(time_series_example time_series_example.cpp)
target_link_libraries(time_series_example streaming_compute Arrow::arrow_shared Threads::Threads)

# Reactive state engine example
add_executable(reactive_state_example reactive_state_example.cpp)
target_link_libraries(reactive_state_example streaming_compute Arrow::arrow_shared Threads::Threads)

# Cross-sectional engine example
add_executable(cross_sectional_example cross_sectional_example.cpp)
target_link_libraries(cross_sectional_example streaming_compute Arrow::arrow_shared Threads::Threads)

# Complete system example
add_executable(complete_system_example complete_system_example.cpp)
target_link_libraries(complete_system_example streaming_compute Arrow::arrow_shared Threads::Threads)

# Performance benchmark
add_executable(performance_benchmark performance_benchmark.cpp)
target_link_libraries(performance_benchmark streaming_compute Arrow::arrow_shared Threads::Threads)

# CEP engine example
add_executable(cep_example cep_example.cpp)
target_link_libraries(cep_example streaming_compute Arrow::arrow_shared Threads::Threads)