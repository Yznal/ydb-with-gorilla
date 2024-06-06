#pragma once

#include <iostream>
#include <fstream>
#include <cstdint>
#include <stdexcept>
#include <cmath>
#include "bit_reader.h"

class Decompressor {
public:
    explicit Decompressor(std::istream& is) : br(is) {
        header_ = br.readBits(64);
    }

    [[nodiscard]] uint64_t get_header() const {
        return header_;
    }

    std::pair<uint64_t, uint64_t> next() {
        if (t_ == 0) {
            return decompress_first();
        } else {
            return decompress();
        }
    }

private:
    [[nodiscard]] std::pair<uint64_t, uint64_t> decompress_first() {
        uint64_t delta_u64 = br.readBits(FIRST_DELTA_BITS);
        int64_t delta = *reinterpret_cast<int64_t*>(&delta_u64);

        if (delta == (1 << (FIRST_DELTA_BITS - 1))) {
            return std::make_pair(0, 0);
        }

        uint64_t value = br.readBits(64);
        t_delta_ = delta;
        t_ = header_ + t_delta_;
        value_ = value;

        return std::make_pair(t_, value_);
    }

    std::pair<uint64_t, uint64_t> decompress() {
        uint64_t t = decompress_timestamp();
        uint64_t v = decompress_value();

        return std::make_pair(t, v);
    }

    uint64_t decompress_timestamp() {
        uint8_t n = dod_timestamp_bits();

        if (n == 0) {
            t_ += t_delta_;
            return t_;
        }

        uint64_t bits = br.readBits(n);

        if (n == 32 && bits == 0xFFFFFFFF) {
            return 0;
        }

        int64_t bits_int64 = *reinterpret_cast<int64_t*>(bits);
        int64_t dod = bits_int64;
        if (n != 32 && (1 << (n - 1)) < bits_int64) {
            dod = bits_int64 - (1 << n);
        }

        t_delta_ += dod;
        t_ += t_delta_;
        return t_;
    }

    uint8_t dod_timestamp_bits() {
        uint8_t dod = 0;
        for (int i = 0; i < 4; i++) {
            dod <<= 1;
            bool bit = br.readBit();
            if (bit) {
                dod |= 1;
            } else {
                break;
            }
        }

        if (dod == 0x00) {
            // Case of dod == 0.
            return 0;
        } else if (dod == 0x02) {
            // Case of dod == 10.
            return 7;
        } else if (dod == 0x06) {
            return 9;
        } else if (dod == 0x0E) {
            return 12;
        } else if (dod == 0x0F) {
            return 32;
        } else {
            std::cerr << "invalid bit header for bit length to read" << std::endl;
            exit(1);
        }
    }

    uint64_t decompress_value() {
        uint8_t read = 0;
        for (int i = 0; i < 2; i++) {
            bool bit = br.readBit();
            if (bit) {
                read <<= 1;
                read++;
            } else {
                break;
            }
        }

        if (read == 0x1 || read == 0x3) {
            if (read == 0x3) {
                uint8_t leading_zeroes = br.readBits(5);
                uint8_t significant_bits = br.readBits(6);
                if (significant_bits == 0) {
                    significant_bits = 64;
                }
                leading_zeros_ = leading_zeroes;
                trailing_zeros_ = 64 - significant_bits - leading_zeroes;
            }

            uint64_t value_bits = br.readBits(64 - leading_zeros_ - trailing_zeros_);
            value_bits <<= trailing_zeros_;
            value_ ^= value_bits;
        }

        return value_;
    }

    BitReader br;
    uint64_t header_ = 0;
    uint64_t t_ = 0;
    int64_t t_delta_ = 0;
    uint8_t leading_zeros_ = 0;
    uint8_t trailing_zeros_ = 0;
    uint64_t value_ = 0;
};