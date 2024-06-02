// For benchmarks see:
// * https://github.com/andybbruno/TSXor/tree/master/benchmark
// * https://github.com/burmanm/gorilla-tsc/blob/master/src/main/java/fi/iki/yak/ts/compression/gorilla/benchmark/EncodingBenchmark.java
//
// Online float -> binary converter:
// https://www.h-schmidt.net/FloatConverter/IEEE754.html
//
// C++ version of https://github.com/keisku/gorilla.
//
// For ready to use C++ implementation see
// https://github.com/andybbruno/TSXor/tree/master/benchmark/algo/core

#include <iostream>
#include <ctime>
#include <sstream>
#include <algorithm>
#include "compressor.h"

const char *OUTPUT_FILE_NAME = "main_output.bin";

std::time_t get_date_timestamp(int year, int month, int day, int hour, int min, int sec) {
    struct tm tm{};
    tm.tm_year = year - 1900;
    tm.tm_mon = month - 1;
    tm.tm_mday = day;
    tm.tm_hour = hour;
    tm.tm_min = min;
    tm.tm_sec = sec;
    std::time_t ts = std::mktime(&tm);
    return ts;
}

// Use `hexdump -C cmake-build-debug/main_output.bin`
// to investigate binary representation of the written bits.

// For binary: `xxd -b cmake-build-debug/main_output.bin`
// For hex (alternative): `xxd cmake-build-debug/main_output.bin`
void write_to_file() {
    std::ofstream outFile(OUTPUT_FILE_NAME, std::ios::binary);
    if (!outFile.is_open()) {
        std::cerr << "Failed to open output file." << std::endl;
        exit(1);
    }

    // 1427151600.
    // 4 bytes of header.
    //
    // 00000000 00000000 00000000 00000000 01010101 00010000 10011010 11110000
    uint64_t header = get_date_timestamp(2015, 3, 24, 2, 0, 0);
    Compressor compressor(outFile, header);

    // 1427151662.
    // (first) diff = 1427151662 - 1427151600 = 62 [2 + 4 + 8 + 16 + 32].
    // First 14 bits of diff.
    //
    // 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00111110
    //                             first 14 bits:              00000001 111100
    auto first_pair_time = get_date_timestamp(2015, 3, 24, 2, 1, 2);
    std::cout << "First diff is:" << std::endl;
    print_binary_representation(first_pair_time - header);
    double first_pair_value = 12;
    // 0100000000101000000000000000000000000000000000000000000000000000
    std::cout << "First value is:" << std::endl;
    print_binary_representation(first_pair_value);
    compressor.compress(first_pair_time, first_pair_value);
    return;

    auto second_pair_time = get_date_timestamp(2015, 3, 24, 2, 2, 2);
    // All 64 bits.
    // 0000000000000000000000000000000000000000000000000000000000001100
    double second_pair_value = 12;
    compressor.compress(second_pair_time, second_pair_value);

    auto third_pair_time = get_date_timestamp(2015, 3, 24, 2, 3, 2);
    double third_pair_value = 24;
    compressor.compress(third_pair_time, third_pair_value);
    compressor.finish();

    outFile.close();
    std::cout << "Wrote bits to the file" << std::endl;
}

void write_to_cout() {
    Compressor compressor(std::cout, 0x12345678);
    compressor.compress(0x87654321, 3.14159);
    compressor.finish();
}

int main() {
    write_to_file();
    return 0;
}
