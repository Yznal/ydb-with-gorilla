#include <vector>
#include <sstream>
#include "bit_writer.h"

using namespace std;

void write_bit_test() {
    vector<string> binaries = {"00000001", "00001000", "01110001", "11111111"};
    uint8_t hexValues[] = {0x1, 0x8, 0x71, 0xFF};

    for (size_t i = 0; i < binaries.size(); ++i) {
        stringstream ss;
        BitWriter bw(ss);
        for (char bit : binaries[i]) {
            bw.writeBit(bit == '1');
        }

        auto left = hexValues[i];
        auto right = static_cast<uint8_t>(ss.str()[0]);
        bool res = left == right;
        if (!res) {
            std::cout << "write_bit_test: Not equal on i = " << i << ". Left: " << left << ". Right: " << right << "." << std::endl;
        }
    }
}

// Use `hexdump -C cmake-build-debug/write_output.bin`
// to investigate binary representation of the written bits.
int write_to_file() {
    std::ofstream outFile("write_output.bin", std::ios::binary);
    if (!outFile.is_open()) {
        std::cerr << "Failed to open output file." << std::endl;
        return 1;
    }

    BitWriter bw(outFile);
    bw.writeBits(0x123456789ABCDEF0ULL, 32);
    bw.flush(true);

    outFile.close();
    std::cout << "Wrote bits to the file" << std::endl;
    return 0;
}

int main() {
//    custom_write();
    write_bit_test();
}