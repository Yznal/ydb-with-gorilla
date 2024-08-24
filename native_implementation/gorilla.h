#pragma once

// File containing all the logic from previously many header files.

#include <iostream>
#include <fstream>
#include <bitset>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <utility>
#include <vector>
#include <bit>
#include <optional>

// ---------- COMPRESSION ------------------
class BitWriter {
public:
    explicit BitWriter(std::ostream &os) : out(os), buffer(0), count(8) {}

    // Write a single bit at the available right-most position of the `buffer`.
    void writeBit(bool bit) {
        if (bit) {
            // 1. mask = 1 << (count - 1)
            // Shift binary representation of 1 to the left by (count - 1) positions
            // (create a mask with a single bit set at position (count - 1)).
            //
            // 2. buffer |= mask
            // Apply bitwise OR assignment operator.
            buffer |= (1 << (count - 1));
        }
        count--;

        // If `buffer` is filled, write it out and reinitialize.
        if (count == 0) {
            writeBuf();
            buffer = 0;
            count = 8;
        }
    }

    // Write the `nbits` right-most bits of `u64` to the `buffer` in left-to-right order.
    //
    // E.g., given:
    // * `u64`   = ...0001010101010_000111
    // * `nbits` = 6,
    // it will write `000111` to the buffer.
    void writeBits(uint64_t u64, int nbits) {
        // Left-shit `u64` leaving only `nbits` of meaningful bits (on leading positions).
        // Was:    ...0001010101010_000111
        // Becase: 000111...00000000000000
        u64 <<= (64 - nbits);
        while (nbits >= 8) {
            auto byte = static_cast<uint8_t>(u64 >> 56);
            writeByte(byte);
            u64 <<= 8;
            nbits -= 8;
        }

        while (nbits > 0) {
            bool bit = (u64 >> 63) == 1;
            writeBit(bit);

            u64 <<= 1;
            nbits--;
        }
    }

    // Write a single byte to the stream, regardless of alignment.
    void writeByte(uint8_t byte) {
        //            writing ->
        // buffer:         [xxx*****]
        //  x -- non-empty (3)
        //  * -- empty     (5 = count)
        // 1. Shift `byte` on the number of already taken positions of `buffer`
        //    (3 in this example).
        // Was:    11001100
        // Became: 00011001
        // 2. Write the mask to the `buffer`.
        // 3. Write out the `buffer`.
        // 4. Write the remaining (right-remaining) part of the `byte` to the `buffer`
        //    (00000100 in this example)
        buffer |= (byte >> (8 - count));
        writeBuf();
        buffer = byte << count;
    }

    // Empty the currently in-process `buffer` by filling it with 'bit'
    // (all unused right-most bits will be filled with `bit`).
    void flush(bool bit) {
        // `count` will become 8 after `buffer` is written out?
        while (count != 8) {
            writeBit(bit);
        }
    }

private:
    void writeBuf() {
        auto casted_buffer = reinterpret_cast<const char *>(&buffer);
        out.write(casted_buffer, sizeof(buffer));
    }

    std::ostream &out;
    uint8_t buffer;
    // How many right-most bits are available for writing in the current byte (the last byte of the buffer).
    uint8_t count;
};

constexpr int32_t
        FIRST_DELTA_BITS = 14;

uint8_t leadingZeros(uint64_t v) {
    uint64_t mask = 0x8000000000000000;
    uint8_t ret = 0;
    while (ret < 64 && (v & mask) == 0) {
        mask >>= 1;
        ret++;
    }
    return ret;
}

uint8_t trailingZeros(uint64_t v) {
    uint64_t mask = 0x0000000000000001;
    uint8_t ret = 0;
    while (ret < 64 && (v & mask) == 0) {
        mask <<= 1;
        ret++;
    }
    return ret;
}

// Header is a first time aligned to 2 hours window.
//
// We need header, because it helps us to deal with a case when `finish`
// was called without `compress`
uint64_t getHeaderFromTimestamp(uint64_t first_time) {
    auto seconds_after_2_hour_window = first_time % (60 * 60 * 2);
    return first_time - seconds_after_2_hour_window;
}

template<typename T>
class CompressorBase {
public:
    explicit CompressorBase(std::shared_ptr<BitWriter> bw) : bw_(std::move(bw)), first_compressed_(false) {}

    virtual ~CompressorBase() = default;

    virtual void compressFirst(T) = 0;

    virtual void compressNonFirst(T) = 0;

    void compress(T entity) {
        if (first_compressed_) {
            compressNonFirst(entity);
        } else {
            compressFirst(entity);
        }
    }

    virtual void finish() = 0;

protected:
    std::shared_ptr<BitWriter> bw_;
    bool first_compressed_;
};

class TimestampsCompressor : public CompressorBase<uint64_t> {
public:
    explicit TimestampsCompressor(std::shared_ptr<BitWriter> bw) : CompressorBase(std::move(bw)), header_(0) {}

    void compressFirst(uint64_t t) override {
        header_ = getHeaderFromTimestamp(t);
        bw_->writeBits(header_, 64);
        if (t - header_ < 0) {
            std::cerr << "First time passed for compression is less than header." << std::endl;
            std::cerr << "Header: " << header_ << ". Time: " << t << "." << std::endl;
            exit(0);
        }
        int64_t delta = static_cast<int64_t>(t) - static_cast<int64_t>(header_);
        t_ = t;
        t_delta_ = delta;
        bw_->writeBits(delta, FIRST_DELTA_BITS);
        first_compressed_ = true;
    }

    void compressNonFirst(uint64_t t) override {
        auto delta = static_cast<int64_t>(t) - static_cast<int64_t>(t_);
        int64_t dod = delta - t_delta_;

        t_ = t;
        t_delta_ = delta;

        if (dod == 0) {
            bw_->writeBit(false);
        } else if (-63 <= dod && dod <= 64) {
            bw_->writeBits(0x02, 2);
            writeInt64Bits(dod, 7);
        } else if (-255 <= dod && dod <= 256) {
            bw_->writeBits(0x06, 3);
            writeInt64Bits(dod, 9);
        } else if (-2047 <= dod && dod <= 2048) {
            bw_->writeBits(0x0E, 4);
            writeInt64Bits(dod, 12);
        } else {
            bw_->writeBits(0x0F, 4);
            writeInt64Bits(dod, 64);
        }
    }

    void finish() override {
        if (!first_compressed_) {
            bw_->writeBits((1 << FIRST_DELTA_BITS) - 1, FIRST_DELTA_BITS);
            bw_->writeBits(0, 64);
            bw_->flush(false);
            return;
        }

        // 0x0F           = 00001111 -> 1111 (cutted).
        bw_->writeBits(0x0F, 4);
        // 0xFFFFFFFF     = 11111111 11111111 11111111 11111111
        bw_->writeBits(0xFFFFFFFFFFFFFFFF, 64);
        bw_->writeBit(false);
        bw_->flush(false);
    }

private:
    void writeInt64Bits(int64_t i, int nbits) {
        uint64_t u = 0;
        if (i >= 0 || nbits >= 64) {
            u = static_cast<uint64_t>(i);
        } else {
            u = static_cast<uint64_t>((1 << nbits) + i);
        }
        bw_->writeBits(u, int(nbits));
    }

    // Header bits.
    uint64_t header_;
    // Last time passed for compression.
    uint64_t t_ = 0;
    // 1.) In case first (time, value) pair passed after header, find delta with header time.
    // 2.) Otherwise, last time delta with new passed time and `t_`.
    int64_t t_delta_ = 0;
};

class ValuesCompressor : public CompressorBase<uint64_t> {
public:
    explicit ValuesCompressor(std::shared_ptr<BitWriter> bw) : CompressorBase(std::move(bw)), leading_zeros_(INT8_MAX) {}

    void compressFirst(uint64_t v) override {
        value_ = v;
        bw_->writeBits(value_, 64);
        first_compressed_ = true;
    }

    void compressNonFirst(uint64_t v) override {
        uint64_t xor_val = value_ ^ v;
        value_ = v;

        if (xor_val == 0) {
            bw_->writeBit(false);
            return;
        }

        uint8_t leading_zeros_val = leadingZeros(xor_val);
        uint8_t trailing_zeros_val = trailingZeros(xor_val);

        bw_->writeBit(true);

        if (leading_zeros_ <= leading_zeros_val && trailing_zeros_ <= trailing_zeros_val) {
            bw_->writeBit(false);
            int significant_bits = 64 - leading_zeros_ - trailing_zeros_;
            bw_->writeBits(xor_val >> trailing_zeros_, significant_bits);
            return;
        }

        leading_zeros_ = leading_zeros_val;
        trailing_zeros_ = trailing_zeros_val;

        bw_->writeBit(true);
        bw_->writeBits(leading_zeros_, 6);
        int significant_bits = 64 - leading_zeros_ - trailing_zeros_;
        bw_->writeBits(static_cast<uint64_t>(significant_bits), 6);
        bw_->writeBits(xor_val >> trailing_zeros_val, significant_bits);
    }

    void finish() override {
        if (!first_compressed_) {
            bw_->writeBits(0, 64);
            bw_->flush(false);
            return;
        }

        bw_->writeBit(true);
        bw_->writeBit(true);

        // 0x3F = 00111111 -> 111111 (cutted).
        bw_->writeBits(0x3F, 6);
        bw_->writeBits(0x3F, 6);
        bw_->flush(false);
    }

private:
    uint8_t leading_zeros_ = 0;
    uint8_t trailing_zeros_ = 0;
    // Last value passed for compression.
    uint64_t value_ = 0;
};

// Diff from initial article implementation:
// 1.) Leading zeroes are encoded and decoded as 6 bits and not as 5 (as it's done in the article).
// 2.) Max DOD encoded as 64 bits and not as 32.
// 3.) Unable to decompress 0xFFFFFFFFFFFFFFFF as value as currently it's reserved as a flag of series end.
class PairsCompressor : public CompressorBase<std::pair<uint64_t, uint64_t>> {
public:
    explicit PairsCompressor(const std::shared_ptr<BitWriter>& bw) : CompressorBase(bw), compressor_ts_(bw), compressor_value_(bw) {}

    void compressFirst(std::pair<uint64_t, uint64_t> entity) override {
        auto [t, v] = entity;
        compressor_ts_.compressFirst(t);
        compressor_value_.compressFirst(v);
        first_compressed_ = true;
    }

    void compressNonFirst(std::pair<uint64_t, uint64_t> entity) override {
        auto [t, v] = entity;
        compressor_ts_.compressNonFirst(t);
        compressor_value_.compressNonFirst(v);
    }

    void finish() override {
        compressor_ts_.finish();
    }

private:
    TimestampsCompressor compressor_ts_;
    ValuesCompressor compressor_value_;
};
// ---------- COMPRESSION ------------------



// ---------- DECOMPRESSION ----------------
class BitReader {
public:
    explicit BitReader(std::istream &is) : in(is), buffer_(0), count_(0) {}

    // Read single bit from the stream.
    bool readBit() {
        if (count_ == 0) {
            refreshBuffer();
            count_ = 8;
        }
        count_--;

        // 1.) Bitwise AND (0x80 = 10000000)
        // 2.) Left shift buffer on 1 bit.
        // 3.) If digit == 1, return true, false -- otherwise.
        uint8_t digit = (buffer_ & 0x80);
        buffer_ <<= 1;
        bool res = digit != 0;
        return res;
    }

    // Read single byte from the stream.
    uint8_t readByte() {
        if (count_ == 0) {
            refreshBuffer();
            return buffer_;
        }
        uint8_t byte = buffer_;
        refreshBuffer();
        byte |= (buffer_ >> count_);
        buffer_ <<= (8 - count_);
        return byte;
    }

    // Read `nbits` bits from the stream.
    uint64_t readBits(int nbits) {
        uint64_t u64 = 0;

        while (nbits >= 8) {
            uint8_t byte = readByte();
            u64 = (u64 << 8) | static_cast<uint64_t>(byte);
            nbits -= 8;
        }

        while (nbits > 0) {
            uint8_t byte = readBit();
            u64 <<= 1;
            if (byte) {
                u64 |= 1;
            }
            nbits--;
        }

        return u64;
    }

private:
    // Read a new byte from the stream.
    void refreshBuffer() {
        char read_byte;
        in.read(&read_byte, 1);
        buffer_ = read_byte;
    }

    std::istream &in;
    uint8_t buffer_;
    // How many right-most bits are available for reading in the current byte.
    // Note: reading is applied from left to right.
    uint8_t count_;
};

template<typename T>
class DecompressorBase {
public:
    explicit DecompressorBase(std::shared_ptr<BitReader> bw) : br_(std::move(bw)), first_decompressed_(false) {}

    virtual ~DecompressorBase() = default;

    std::optional<T> next() {
        if (first_decompressed_) {
            return decompressNonFirst();
        } else {
            return { decompressFirst() };
        }
    }

private:
    virtual std::optional<T> decompressFirst() = 0;

    virtual std::optional<T> decompressNonFirst() = 0;

protected:
    std::shared_ptr<BitReader> br_;
    bool first_decompressed_ = true;
};

class TimestampsDecompressor : public DecompressorBase<uint64_t > {
public:
    explicit TimestampsDecompressor(std::shared_ptr<BitReader> br) : DecompressorBase(std::move(br)) {}

    [[nodiscard]] uint64_t getHeader() const {
        return header_;
    }

    std::optional<uint64_t> decompressFirst() override {
        header_ = br_->readBits(64);
        uint64_t delta_u64 = br_->readBits(FIRST_DELTA_BITS);
        int64_t delta = *reinterpret_cast<int64_t *>(&delta_u64);

        if (delta == ((1 << FIRST_DELTA_BITS) - 1)) {
            return std::nullopt;
        }

        t_delta_ = delta;
        t_ = header_ + t_delta_;
        first_decompressed_ = true;
        return { t_ };
    }

    std::optional<uint64_t> decompressNonFirst() override {
        uint8_t n = dodTimestampBits();

        if (n == 0) {
            t_ += t_delta_;
            return t_;
        }

        uint64_t bits = br_->readBits(n);

        if (n == 64 && bits == 0xFFFFFFFFFFFFFFFF) {
            return std::nullopt;
        }

        int64_t bits_int64 = *reinterpret_cast<int64_t *>(&bits);
        int64_t dod = bits_int64;
        if (n != 64 && (1 << (n - 1)) < bits_int64) {
            dod = bits_int64 - (1 << n);
        }

        t_delta_ += dod;
        t_ += t_delta_;
        return t_;
    }

private:
    uint8_t dodTimestampBits() {
        uint8_t dod = 0;
        for (int i = 0; i < 4; i++) {
            dod <<= 1;
            bool bit = br_->readBit();
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
            return 64;
        } else {
            std::cerr << "invalid bit header for bit length to read" << std::endl;
            exit(1);
        }
    }

    uint64_t header_ = 0;
    uint64_t t_ = 0;
    int64_t t_delta_ = 0;
};

class ValuesDecompressor : public DecompressorBase<uint64_t> {
public:
    explicit ValuesDecompressor(std::shared_ptr<BitReader> br) : DecompressorBase(std::move(br)) {}

    std::optional<uint64_t> decompressFirst() override {
        uint64_t value = br_->readBits(64);

        if (value == 0xFFFFFFFFFFFFFFFF) {
            return std::nullopt;
        }

        first_decompressed_ = true;
        return { value };
    }

    std::optional<uint64_t> decompressNonFirst() override {
        uint8_t read = 0;
        for (int i = 0; i < 2; i++) {
            bool bit = br_->readBit();
            if (bit) {
                read <<= 1;
                read++;
            } else {
                break;
            }
        }

        if (read == 0x1 || read == 0x3) {
            if (read == 0x3) {
                uint8_t leading_zeroes = br_->readBits(6);
                uint8_t significant_bits = br_->readBits(6);

                if (leading_zeroes == 0x3F && significant_bits == 0x3F) {
                    return std::nullopt;
                }

                if (significant_bits == 0) {
                    significant_bits = 64;
                }
                leading_zeros_ = leading_zeroes;
                trailing_zeros_ = 64 - significant_bits - leading_zeros_;
            }

            uint64_t value_bits = br_->readBits(64 - leading_zeros_ - trailing_zeros_);
            value_bits <<= trailing_zeros_;
            value_ ^= value_bits;
        }

        return value_;
    }

private:
    uint8_t leading_zeros_ = 0;
    uint8_t trailing_zeros_ = 0;
    uint64_t value_ = 0;
};

class PairsDecompressor : public DecompressorBase<std::pair<uint64_t, uint64_t>> {
public:
    explicit PairsDecompressor(const std::shared_ptr<BitReader>& br) : DecompressorBase(br), decompressor_ts_(br), decompressor_value_(br) {}

    [[nodiscard]] uint64_t getHeader() const {
        return decompressor_ts_.getHeader();
    }

private:
    [[nodiscard]] std::optional<std::pair<uint64_t, uint64_t>> decompressFirst() override {
        auto t = decompressor_ts_.decompressFirst();
        if (!t) {
            return std::nullopt;
        }
        auto v = decompressor_value_.decompressFirst();
        if (!v) {
            return std::nullopt;
        }

        first_decompressed_ = true;
        return { std::make_pair(*t, *v) };
    }

    std::optional<std::pair<uint64_t, uint64_t>> decompressNonFirst() override {
        std::optional<uint64_t> t = decompressor_ts_.decompressNonFirst();
        if (!t) {
            return std::nullopt;
        }
        std::optional<uint64_t> v = decompressor_value_.decompressNonFirst();
        if (!v) {
            return std::nullopt;
        }
        return { std::make_pair(*t, *v) };
    }

    TimestampsDecompressor decompressor_ts_;
    ValuesDecompressor decompressor_value_;
};
// ---------- DECOMPRESSION ----------------