cmake_minimum_required(VERSION 3.27)
project(custom_gorilla)

set(CMAKE_CXX_STANDARD 20)

add_executable(
        bit_writer_test
        bit_writer_test.cpp
        bit_writer.h
)

add_executable(
        bit_reader_test
        bit_reader_test.cpp
        bit_reader.h
)

add_executable(
        custom_gorilla
        main.cpp
        bit_writer.h
        compressor.h
        bit_reader.h
        decompressor.h
)

