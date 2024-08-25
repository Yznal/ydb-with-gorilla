#include <vector>
#include <sstream>
#include "gorilla.h"

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

        auto expected = hexValues[i];
        auto actual = static_cast<uint8_t>(ss.str()[0]);
        if (expected != actual) {
            std::cerr << "write_bit_test: Not equal on i = " << i << ". Left: " << expected << ". Right: " << actual << "." << std::endl;
            return;
        }
    }
}

int main() {
    write_bit_test();
}