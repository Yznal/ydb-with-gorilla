cmake_minimum_required(VERSION 3.27)
project(custom_gorilla)

set(CMAKE_CXX_STANDARD 17)

add_executable(
        bit_writer_test
        bit_writer_test.cpp
        bit_writer.h
)

add_executable(
        bit_wr_test
        bit_wr_test.cpp
        bit_writer.h
        bit_reader.h
)

add_executable(
        gorilla_test
        gorilla_test.cpp
        bit_writer.h
        compressor.h
        bit_reader.h
        decompressor.h
)

option(ARROW_LINK_SHARED "Link to the Arrow shared library" ON)
find_package(Arrow REQUIRED)

if(NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 17)
endif()

# We require a C++17 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT DEFINED CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

message(STATUS "Arrow version: ${ARROW_VERSION}")
message(STATUS "Arrow SO version: ${ARROW_FULL_SO_VERSION}")

add_executable(
        arrow_gorilla_test
        arrow_gorilla_test.cpp
)

if(ARROW_LINK_SHARED)
    target_link_libraries(arrow_gorilla_test PRIVATE Arrow::arrow_shared)
else()
    target_link_libraries(arrow_gorilla_test PRIVATE Arrow::arrow_static)
endif()
