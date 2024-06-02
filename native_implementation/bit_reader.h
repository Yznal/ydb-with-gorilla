#pragma once

#include <iostream>
#include <fstream>
#include <cstdint>
#include <stdexcept>
#include <vector>

class BitReader {
public:
    explicit BitReader(std::istream& is) : in(is), buffer_(0), count_(8) {}

    // Return next bit from the stream.
    // Read a new byte if needed.
    bool readBit() {
        if (count_ == 0) {
            in >> buffer_;
        }
        --count_;
        return Bit((in.peek() & 0x80) != 0);
    }

    // TODO: fix
    uint64_t readBits(int nbits) {
        uint64_t result = 0;
        while (nbits >= 8) {
            if (!readByte()) {
                throw std::runtime_error("Failed to read a byte");
            }
            result = (result << 8) | static_cast<uint64_t>(in.get());
            nbits -= 8;
        }

        while (nbits > 0) {
            Bit bit = readBit();
            if (bit == Bit::Zero) {
                throw std::runtime_error("Unexpected end of input");
            }
            result <<= 1;
            if (bit == Bit::One) {
                result |= 1;
            }
            --nbits;
        }

        return result;
    }

private:
    bool readByte() {
        char c;
        if (!(in >> c)) {
            return false;
        }
        return true;
    }

    std::istream& in;
    uint8_t buffer_;
    // How many right-most bits are available for reading in the current byte.
    // Note: reading is applied from left to right.
    uint8_t count_;
};