// C++ version of https://github.com/keisku/gorilla.
//
// For benchmarks see:
// * https://github.com/andybbruno/TSXor/tree/master/benchmark
// * https://github.com/burmanm/gorilla-tsc/blob/master/src/main/java/fi/iki/yak/ts/compression/gorilla/benchmark/EncodingBenchmark.java
//
// Online float -> binary converter:
// https://www.h-schmidt.net/FloatConverter/IEEE754.html
//
// For ready to use C++ implementation see
// https://github.com/andybbruno/TSXor/tree/master/benchmark/algo/core

#include <iostream>
#include <ctime>
#include <bitset>
#include <sstream>
#include <algorithm>
#include "compressor.h"
#include "decompressor.h"

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

void test_article_example() {
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
    uint64_t first_pair_value = 12;
    // 0100000000101000000000000000000000000000000000000000000000000000
    std::cout << "First value is:" << std::endl;
    compressor.compress(first_pair_time, first_pair_value);
    return;

    auto second_pair_time = get_date_timestamp(2015, 3, 24, 2, 2, 2);
    // All 64 bits.
    // 0000000000000000000000000000000000000000000000000000000000001100
    uint64_t second_pair_value = 12;
    compressor.compress(second_pair_time, second_pair_value);

    auto third_pair_time = get_date_timestamp(2015, 3, 24, 2, 3, 2);
    uint64_t third_pair_value = 24;
    compressor.compress(third_pair_time, third_pair_value);
    compressor.finish();

    // TODO: finish example.

    outFile.close();
    std::cout << "Wrote bits to the file" << std::endl;
}

const std::string INTEGRATION_READ_WRITE_FILE_NAME = "integration.bin";

struct data {
    uint64_t time;
    uint64_t value;

    bool operator==(const data& other) const {
        return time == other.time && value == other.value;
    }
};

std::ostream& operator<<(std::ostream& out, const data& d)
{
    return out << "([time: " << d.time << "]" << " [value: " << d.value << "]";
}

void test_compress_decompress() {
    // Generate data for compression.
    uint64_t header = get_date_timestamp(2024, 0, 0, 0, 0, 0);
    // 00000000 00000000 00000000 00000000 01100101 01100111 10100110
    // 01010000
    std::bitset<64> binary_header(header);
    size_t data_len = 10;
    auto expected_data_vec = std::vector<data>(data_len);
    uint64_t current_timestamp = header;
    for (int i = 0; i < data_len; i++) {
        if (0 < i && i % 10 == 0) {
            // TODO: Change to random.
            current_timestamp -= 100;
        } else {
            // TODO: Change to random.
            current_timestamp += 200;
        }

        // TODO: Change to random.
        uint64_t value = 300;
        expected_data_vec[i] = data { current_timestamp, value };

        if (i == 0) {
            uint64_t first_delta = current_timestamp - header;
            // 00000011 001000
            // Total:
            // 00000000 00000000 00000000 00000000 01100101 01100111
            // 10100110 01010000 00000011 001000
            std::bitset<14> binary_first_delta(first_delta);

            // 00 00000000 00000000 00000000 00000000 00000000 00000000 00000100 101100
            // Total:
            // 00000000 00000000 00000000 00000000 01100101 01100111
            // 10100110 01010000 00000011 00100000 00000000 00000000
            // 00000000 00000000 00000000 00000000 00000100 101100
            std::bitset<64> binary_first_value(value);
        }
    }

    // Compress data.
    std::ofstream buffer_out(INTEGRATION_READ_WRITE_FILE_NAME, std::ios::binary);
    if (!buffer_out.is_open()) {
        std::cerr << "Failed to open integration file as output buffer." << std::endl;
        return;
    }
    Compressor c(buffer_out, header);
    for (auto data : expected_data_vec) {
        c.compress(data.time, data.value);
    }
    // Total:
    // 00000000 00000000 00000000 00000000 01100101 01100111
    // 10100110 01010000 00000011 00100000 00000000 00000000
    // 00000000 00000000 00000000 00000000 00000100 10110011
    // 11111111 11111111 11111111 11111111 11000000
    c.finish();
    buffer_out.close();

    // Decompress data.
    std::ifstream buffer_in(INTEGRATION_READ_WRITE_FILE_NAME, std::ios::binary);
    if (!buffer_in.is_open()) {
        std::cerr << "Failed to open integration file as input buffer." << std::endl;
        return;
    }
    auto actual_data_vec = std::vector<data>();
    Decompressor d(buffer_in);
    auto d_header = d.get_header();
    if (d_header != header) {
        std::cerr << "Headers differ. Expected: " << header << ". Actual: " << d_header << "." <<  std::endl;
        return;
    }

    for (int i = 0; i < data_len; i++) {
        std::pair<uint64_t, uint64_t> current_pair = d.next();
        actual_data_vec.push_back(data { current_pair.first, current_pair.second });
    }

    if (expected_data_vec.size() != actual_data_vec.size()) {
        std::cerr << "Data vector sizes differ. Expected: " << expected_data_vec.size() << ". Actual: " << actual_data_vec.size() << "." <<  std::endl;
        return;
    }

    for (int i = 0; i < data_len; i++) {
        auto expected_data = expected_data_vec[i];
        auto actual_data = actual_data_vec[i];
        if (!(expected_data == actual_data)) {
            std::cerr << "Data differ on index: " << i << ". " << expected_data << " != " << actual_data << std::endl;
            return;
        }
    }
    buffer_in.close();
}

// To run execute:
// `cmake . && make gorilla_test && ./gorilla_test`
//
// Commands to investigate binary representation of the testing file:
// * Binary: `xxd -b integration.bin`
//
// Note:
// Header -- 2 hours aligned datetime.
int main() {
    test_compress_decompress();
    return 0;
}
