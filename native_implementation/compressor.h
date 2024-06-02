#pragma once

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <vector>
#include <bit>
#include "bit_writer.h"

constexpr int32_t FIRST_DELTA_BITS = 14;

uint8_t leading_zeros(uint64_t v) {
    uint64_t mask = 0x8000000000000000;
    uint8_t ret = 0;
    while (ret < 64 && (v & mask) == 0) {
        mask >>= 1;
        ret++;
    }
    return ret;
}

uint8_t trailing_zeros(uint64_t v) {
    uint64_t mask = 0x0000000000000001;
    uint8_t ret = 0;
    while (ret < 64 && (v & mask) == 0) {
        mask <<= 1;
        ret++;
    }
    return ret;
}

template<typename T>
void print_binary_representation(T value) {
    char* bits = reinterpret_cast<char*>(&value);
    std::stringstream ss;
    for (std::size_t n = 0; n < sizeof(value); n++) {
        ss << std::bitset<8>(bits[n]);
    }
    std::string res = ss.str();
    std::reverse(res.begin(), res.end());
    std::cout << res << std::endl;
}

template<>
void print_binary_representation(uint64_t value) {
    for (int i = 63; i >= 0; i--) {
        std::cout << ((value >> i) & 1);
    }
    std::cout << std::endl;
}

// See https://www.h-schmidt.net/FloatConverter/IEEE754.html
// for comparison (float or double?).
template<>
void print_binary_representation(double value) {
    uint64_t myUint64 = *reinterpret_cast<uint64_t*>(&value);

    for (int i = 63; i >= 0; i--) {
        std::cout << ((myUint64 >> i) & 1);
    }
    std::cout << std::endl;
}

template<>
void print_binary_representation(float value) {
    uint32_t myUint32 = *reinterpret_cast<uint32_t*>(&value);

    for (int i = 31; i >= 0; i--) {
        std::cout << ((myUint32 >> i) & 1);
    }
    std::cout << std::endl;
}

template<typename T>
void print_hex_representation(T value) {
    char result[sizeof(T)];
    memcpy(result, &value, sizeof(T));
    for (int i = sizeof(T); i >= 0; i--) {
        printf("%x", result[i]);
    }
    std::cout << std::endl;
}

class Compressor {
public:
    Compressor(std::ostream& os, uint64_t header) : bw(os), header_(header), leading_zeros_(UINT8_MAX) {
        bw.writeBits(header_, 64);
    }

    void compress(uint64_t t, double v) {
        if (t_ == 0) {
            int64_t delta = static_cast<int64_t>(t) - static_cast<int64_t>(header_);
            t_ = t;
            t_delta_ = delta;
            value_ = *reinterpret_cast<uint64_t*>(&v);

            bw.writeBits(delta, FIRST_DELTA_BITS);
            bw.writeBits(value_, 64);
        } else {
            compressTimestamp(t);
            compressValue(v);
        }
    }

    void finish() {
        if (t_ == 0) {
            bw.writeBits(1<< (FIRST_DELTA_BITS - 1), FIRST_DELTA_BITS);
            bw.writeBits(0, 64);
            bw.flush(false);
            return;
        }

        bw.writeBits(0x0F, 4);
        bw.writeBits(0xFFFFFFFF, 64);
        bw.writeBit(false);
        bw.flush(false);
    }

private:
    void compressTimestamp(uint64_t t) {
        auto delta = static_cast<int64_t>(t) - static_cast<int64_t>(t_);
        int64_t dod = static_cast<int64_t>(delta) - static_cast<int64_t>(t_delta_);

        t_ = t;
        t_delta_ = delta;

        if (dod == 0) {
            bw.writeBit(false);
        } else if (-63 <= dod && dod <= 64) {
            bw.writeBits(0x02, 2);
            writeInt64Bits(dod, 7);
        } else if (-255 <= dod && dod <= 256) {
            bw.writeBits(0x06, 3);
            writeInt64Bits(dod, 9);
        } else if (-2047 <= dod && dod <= 2048) {
            bw.writeBits(0x0E, 4);
            writeInt64Bits(dod, 12);
        } else {
            bw.writeBits(0x0F, 4);
            writeInt64Bits(dod, 64);
        }
    }

    void writeInt64Bits(int64_t i, int nbits) {
        uint64_t u;
        if (i >= 0 || nbits >= 64) {
            u = static_cast<uint64_t>(i);
        } else {
            u = static_cast<uint64_t>(1 << (nbits + i));
        }
        bw.writeBits(u, int(nbits));
    }

    void compressValue(double v) {
        uint64_t casted_value = *reinterpret_cast<uint64_t*>(&v);
        uint64_t xor_val = value_ ^ casted_value;
        value_ = casted_value;

        if (xor_val == 0) {
            bw.writeBit(false);
            return;
        }

        uint8_t leading_zeros_val = leading_zeros(xor_val);
        uint8_t trailing_zeros_val = trailing_zeros(xor_val);

        bw.writeBit(true);

        if (leading_zeros_val <= leading_zeros_ && trailing_zeros_val <= trailing_zeros_) {
            bw.writeBit(false);
            int significant_bits = 64 - leading_zeros_ - trailing_zeros_;
            bw.writeBits(xor_val >> trailing_zeros_, significant_bits);
            return;
        }

        leading_zeros_ = leading_zeros_val;
        trailing_zeros_ = trailing_zeros_val;

        bw.writeBit(true);
        bw.writeBits(leading_zeros_val, 5);
        int significant_bits = 64 - leading_zeros_ - trailing_zeros_;
        bw.writeBits(static_cast<uint64_t>(significant_bits), 6);
        bw.writeBits(xor_val >> trailing_zeros_val, significant_bits);
    }

    // Util to apply bit operations.
    BitWriter bw;
    // Header bits.
    uint64_t header_;
    // Last time passed for compression.
    uint64_t t_ = 0;
    // 1.) In case first (time, value) pair passed after header, find delta with header time.
    // 2.) Otherwise, last time delta with new passed time and `t_`.
    int64_t t_delta_ = 0;
    uint8_t leading_zeros_ = 0;
    uint8_t trailing_zeros_ = 0;
    // Last value passed for compression.
    uint64_t value_ = 0;
};