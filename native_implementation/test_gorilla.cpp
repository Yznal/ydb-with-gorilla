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
#include <algorithm>
#include "compressor.h"
#include "decompressor.h"

const std::string INTEGRATION_READ_WRITE_FILE_NAME = "integration.bin";

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
int main() {
    test_compress_decompress();
    return 0;
}
