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
#include "test_common.h"

const std::string INTEGRATION_READ_WRITE_FILE_NAME = "integration.bin";

void test_compress_decompress() {
    // Compress data.
    std::ofstream buffer_out(INTEGRATION_READ_WRITE_FILE_NAME, std::ios::binary);
    if (!buffer_out.is_open()) {
        std::cerr << "Failed to open integration file as output buffer." << std::endl;
        return;
    }
    auto test_data = get_test_data_vec();
    auto header = test_data.first;
    auto data_vec = test_data.second;
    std::cout << "Actual header is: " << header << std::endl;
    Compressor c(buffer_out, header);
    for (auto data : data_vec) {
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

    for (int i = 0; i < DEFAULT_TEST_DATA_LEN; i++) {
        std::optional<std::pair<uint64_t, uint64_t>> current_pair = d.next();
        actual_data_vec.push_back(data { (*current_pair).first, (*current_pair).second });
    }

    if (data_vec.size() != actual_data_vec.size()) {
        std::cerr << "Data vector sizes differ. Expected: " << data_vec.size() << ". Actual: " << actual_data_vec.size() << "." <<  std::endl;
        return;
    }

    for (int i = 0; i < DEFAULT_TEST_DATA_LEN; i++) {
        auto expected_data = data_vec[i];
        auto actual_data = actual_data_vec[i];
        std::cout << "Actual data. Time: " << actual_data.time << ". Value: " << actual_data.value << std::endl;
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
// * Binary: `xxd -b integration.bin` (`xxd -b cmake-build-debug/integration.bin`)
int main() {
    test_compress_decompress();
    return 0;
}
