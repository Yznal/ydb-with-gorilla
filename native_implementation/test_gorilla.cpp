#include <iostream>
#include <ctime>
#include <bitset>
#include <algorithm>
#include "gorilla.h"
#include "test_common.h"

const std::string INTEGRATION_READ_WRITE_FILE_NAME = "integration.bin";

void testCompressDecompressPairs() {
    // Compress data.
    std::ofstream buffer_out(INTEGRATION_READ_WRITE_FILE_NAME, std::ios::binary);
    if (!buffer_out.is_open()) {
        std::cerr << "Failed to open integration file as output buffer." << std::endl;
        return;
    }
    auto test_data = getTestDataVec<uint64_t>();
    auto data_vec = test_data.second;
    std::cout << "Actual header is: " << header << std::endl;
    auto bw = BitWriter(buffer_out);
    PairsCompressor c(bw);
    for (auto data : data_vec) {
        c.compress(std::make_pair(data.time, data.value));
    }
    c.finish();
    buffer_out.close();

    // Decompress data.
    std::ifstream buffer_in(INTEGRATION_READ_WRITE_FILE_NAME, std::ios::binary);
    if (!buffer_in.is_open()) {
        std::cerr << "Failed to open integration file as input buffer." << std::endl;
        return;
    }
    auto actual_data_vec = std::vector<data<uint64_t>>();
    auto br = BitReader(buffer_in);
    PairsDecompressor d(br);
    auto d_header = d.getHeader();
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
    testCompressDecompressPairs();
    return 0;
}
