cmake_minimum_required(VERSION 3.16)

# Find GTest
find_package(GTest QUIET)

if(GTest_FOUND)
    # Basic functionality tests
    add_executable(test_stream_table test_stream_table.cpp)
    target_link_libraries(test_stream_table streaming_compute Arrow::arrow_shared Threads::Threads GTest::gtest GTest::gtest_main)
    
    add_executable(test_engines test_engines.cpp)
    target_link_libraries(test_engines streaming_compute Arrow::arrow_shared Threads::Threads GTest::gtest GTest::gtest_main)
    
    # Add tests to CTest
    add_test(NAME StreamTableTest COMMAND test_stream_table)
    add_test(NAME EnginesTest COMMAND test_engines)
else()
    message(WARNING "GTest not found, skipping tests")
endif()