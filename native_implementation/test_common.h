#pragma once

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <iostream>
#include <random>
#include <vector>

using arrow::Status;

// Serialization process of 2 elements:
// { (time = 200, value = 0), (time = 400, value = 100)}:
//
// 1.)
// 00000000 00000000 00000000 00000000 00000000 00000000
// 00000000 00000000                                     <-- header.
// 2.)
// 00000000 00000000 00000000 00000000 00000000 00000000
// 00000000 00000000 00000011 001000                     <-- 14 bits of delta (0)
// 3.)
// 00000000 00000000 00000000 00000000 00000000 00000000
// 00000000 00000000 00000011 00100000 00000000 00000000
// 00000000 00000000 00000000 00000000 00000000 000000   <-- 64 bits of 0 value
// 4.)
// 00000000 00000000 00000000 00000000 00000000 00000000
// 00000000 00000000 00000011 00100000 00000000 00000000
// 00000000 00000000 00000000 00000000 00000000 0000000 <- delta of deltas (0)
// 5.)
// 00000000 00000000 00000000 00000000 00000000 00000000
// 00000000 00000000 00000011 00100000 00000000 00000000
// 00000000 00000000 00000000 00000000 00000000 00000001 <- xor is not 0
// 6.)
// 00000000 00000000 00000000 00000000 00000000 00000000
// 00000000 00000000 00000011 00100000 00000000 00000000
// 00000000 00000000 00000000 00000000 00000000 00000001
// 111001                                                <- '1` + 5 bits of leading zeroes
// 7.)
// 00000000 00000000 00000000 00000000 00000000 00000000
// 00000000 00000000 00000011 00100000 00000000 00000000
// 00000000 00000000 00000000 00000000 00000000 00000001
// 11100100 0101                                         <- 6 bits of `significant_bits`
// 8.)
// 00000000 00000000 00000000 00000000 00000000 00000000
// 00000000 00000000 00000011 00100000 00000000 00000000
// 00000000 00000000 00000000 00000000 00000000 00000001
// 11100100 01011100 1                                   <- significant_bits of XOR
// 9.)
// 00000000 00000000 00000000 00000000 00000000 00000000
// 00000000 00000000 00000011 00100000 00000000 00000000
// 00000000 00000000 00000000 00000000 00000000 00000001
// 11100100 01011100 11111                               <- finish, part 1
// 10.)
// 00000000 00000000 00000000 00000000 00000000 00000000
// 00000000 00000000 00000011 00100000 00000000 00000000
// 00000000 00000000 00000000 00000000 00000000 00000001
// 11100100 01011100 11111111 11111111 11111111 11111111
// 11111                                                 <- finish, part 2
// 11.)
// 00000000 00000000 00000000 00000000 00000000 00000000
// 00000000 00000000 00000011 00100000 00000000 00000000
// 00000000 00000000 00000000 00000000 00000000 00000001
// 11100100 01011100 11111111 11111111 11111111 11111111
// 11111000                                              <- finish, write 0 + flush 0
struct data {
    uint64_t time;
    uint64_t value;

    bool operator==(const data &other) const {
        return time == other.time && value == other.value;
    }
};

std::ostream &operator<<(std::ostream &out, const data &d) {
    return out << "([time: " << d.time << "]" << " [value: " << d.value << "]";
}

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

// Function to generate a random uint64_t within a given range
uint64_t getRandomInRange(uint64_t minVal, uint64_t maxVal) {
    std::random_device rd;
    std::mt19937_64 gen(rd());

    std::uniform_int_distribution<uint64_t> dis(minVal, maxVal);

    return dis(gen);
}

// Test data combining represented in two views:
// * std C++ vec
// * arrow RecordBatch
struct test_data {
    uint64_t data_header;
    std::vector<data> data_vec;
    std::shared_ptr<arrow::RecordBatch> data_batch;
};

const size_t DEFAULT_TEST_DATA_LEN = 5;

arrow::Result<test_data> get_test_data(size_t data_len = DEFAULT_TEST_DATA_LEN) {
    uint64_t first_timestamp = get_date_timestamp(2024, 6, 20, 6, 0, 0);
    auto data_vec = std::vector<data>(data_len);
    auto header = first_timestamp - (first_timestamp % (60 * 60 * 2));
    uint64_t current_timestamp = 0;
    uint64_t current_value = 0;
    for (int i = 0; i < data_len; i++) {
        if (0 < i && i % 10 == 0) {
            current_timestamp -= getRandomInRange(1000, 3000);
        } else {
            current_timestamp += getRandomInRange(2000, 10000);
        }

        data_vec[i] = data{current_timestamp, current_value};
        current_value += getRandomInRange(-500, 1000);
    }

    auto timeColumnBuilder = arrow::TimestampBuilder(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO),
                                                     arrow::default_memory_pool());
    arrow::UInt8Builder valueColumnBuilder;
    for (const auto &c_data: data_vec) {
        ARROW_RETURN_NOT_OK(timeColumnBuilder.Append(c_data.time));
        ARROW_RETURN_NOT_OK(valueColumnBuilder.Append(c_data.value));
    }
    std::shared_ptr<arrow::Array> timeColumnArray;
    ARROW_ASSIGN_OR_RAISE(timeColumnArray, timeColumnBuilder.Finish());
    std::shared_ptr<arrow::Array> valueColumnArray;
    ARROW_ASSIGN_OR_RAISE(valueColumnArray, valueColumnBuilder.Finish());

    std::shared_ptr<arrow::Field> fieldTime = arrow::field("Time", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO));
    std::shared_ptr<arrow::Field> fieldValue = arrow::field("Value", arrow::uint8());
    std::shared_ptr<arrow::Schema> no_compression_schema = arrow::schema({fieldTime, fieldValue});
    std::shared_ptr<arrow::RecordBatch> no_compression_batch = arrow::RecordBatch::Make(no_compression_schema, 2,
                                                                                        {timeColumnArray,
                                                                                         valueColumnArray});

    test_data res = test_data{
            header,
            data_vec,
            no_compression_batch
    };
    return {res};
}