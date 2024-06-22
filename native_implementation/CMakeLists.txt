cmake_minimum_required(VERSION 3.27)
project(custom_gorilla)

set(CMAKE_CXX_STANDARD 20)

add_executable(
        bit_writer_test
        bit_writer_test.cpp
        bit_writer.h
)

add_executable(
        bit_wr_test
        test_bit_reader_writer.cpp
        bit_writer.h
        bit_reader.h
)

add_executable(
        gorilla_test
        test_gorilla.cpp
        bit_writer.h
        compressor.h
        bit_reader.h
        decompressor.h
)

option(ARROW_LINK_SHARED "Link to the Arrow shared library" ON)
find_package(Arrow REQUIRED)

message(STATUS "Arrow version: ${ARROW_VERSION}")
message(STATUS "Arrow SO version: ${ARROW_FULL_SO_VERSION}")

add_executable(
        arrow_gorilla_test
        test_gorilla_arrow.cpp
)

if(ARROW_LINK_SHARED)
    target_link_libraries(arrow_gorilla_test PRIVATE Arrow::arrow_shared)
else()
    target_link_libraries(arrow_gorilla_test PRIVATE Arrow::arrow_static)
endif()
