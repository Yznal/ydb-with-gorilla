#include <sstream>
#include "bit_writer.h"
#include "bit_reader.h"

const std::string CUSTOM_READ_WRITE_FILE_NAME = "write_read.bin";

using namespace std;

void write() {
    std::ofstream outFile(CUSTOM_READ_WRITE_FILE_NAME, std::ios::binary);
    if (!outFile.is_open()) {
        std::cerr << "Failed to open output file." << std::endl;
        exit(1);
    }
    BitWriter bw(outFile);

    // 110101
    bw.writeBit(true);
    bw.writeBit(true);
    bw.writeBit(false);
    bw.writeBit(true);
    bw.writeBit(false);
    bw.writeBit(true);
    // 11010100 <-- fill
    bw.flush(false);

    // 11010100 11100010
    bw.writeByte(0b11100010);

    // 11010100 11100010 10
    bw.writeBit(true);
    bw.writeBit(false);
    // 11010100 11100010 10111111 <-- fill
    bw.flush(true);

    // 11010100 11100010 10111111 11101
    bw.writeBits(1, 1);
    bw.writeBits(1, 1);
    bw.writeBits(1, 1);
    bw.writeBits(0, 1);
    bw.writeBits(1, 1);
    // 11010100 11100010 10111111 11101000 <-- fill
    bw.flush(false);

    // Note on bytes usage below:
    // 0x08 = 00001000
    // 0x1B = 00011011
    // 0xC  = 1100
    // 0xFA = 11111010

    // 11010100 11100010 10111111 11101000
    // 01100
    bw.writeBits(0xC, 5);
    // 11010100 11100010 10111111 11101000
    // 01100111 <-- fill
    bw.flush(true);
    // 11010100 11100010 10111111 11101000
    // 01100111 01000
    bw.writeBits(0x08, 5);
    // 11010100 11100010 10111111 11101000
    // 01100111 01000000 <-- fill
    bw.flush(false);
    // 11010100 11100010 10111111 11101000
    // 01100111 01000000 1111010
    bw.writeBits(0xFA, 7);
    // 11010100 11100010 10111111 11101000
    // 01100111 01000000 11110101 <-- fill
    bw.flush(true);

    outFile.close();
    std::cout << "Wrote bits to the file." << std::endl;
}

void read() {
    std::ifstream inFile(CUSTOM_READ_WRITE_FILE_NAME, std::ios::binary);
    if (!inFile.is_open()) {
        std::cerr << "Failed to open input file." << std::endl;
        exit(1);
    }
    BitReader br(inFile);

    // Expected 11010100.
    for (int i = 0; i < 8; i++) {
        bool bit = br.readBit();
        std::cout << bit << "";
    }
    std::cout << std::endl;

    // Expected 11100010.
    uint8_t byte_1 = br.readByte();
    std::bitset<8> byte_1_bits(byte_1);
    std::cout << byte_1_bits << std::endl;

    // Expected 10111111.
    uint8_t byte_2 = br.readByte();
    std::bitset<8> byte_2_bits(byte_2);
    std::cout << byte_2_bits << std::endl;

    // Expected 11101.
    // Read     11101000.
    uint64_t bits_1_1 = br.readBits(5);
    std::bitset<5> bits_1_1_bits(bits_1_1);
    std::cout << bits_1_1_bits;
    // Expected 000.
    uint64_t bits_1_2 = br.readBits(3);
    std::bitset<3> bits_1_2_bits(bits_1_2);
    std::cout << bits_1_2_bits << std::endl;

    // Expected 01100111 010.
    // Read     01100111 01000000.
    uint64_t bits_2_1 = br.readBits(11);
    std::bitset<11> bits_2_1_bits(bits_2_1);
    std::cout << bits_2_1_bits;
    // Expected 00000.
    uint64_t bits_2_2 = br.readBits(5);
    std::bitset<5> bits_2_2_bits(bits_2_2);
    std::cout << bits_2_2_bits << std::endl;

    // Expected 11110101.
    uint8_t last_byte = br.readByte();
    std::bitset<8> last_byte_bits(last_byte);
    std::cout << last_byte_bits << std::endl;

    inFile.close();
    std::cout << "Read bits from the file." << std::endl;
}

// To run execute:
// `cmake . && make bit_wr_test && ./bit_wr_test`
//
// Hex to binary converter: https://www.rapidtables.com/convert/number/hex-to-binary.html
//
// Commands to investigate binary representation of the testing file:
// * Binary: `xxd -b write_read.bin`
// * Hex   : `xxd write_read.bin`
//
// Note: possibly would have to add `cmake-build-debug/` prefix.
int main() {
    write();
    read();
}