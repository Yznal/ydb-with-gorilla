#pragma once

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <iostream>
#include <random>
#include <vector>
#include <ctime>

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
template<typename NumericT>
struct data {
    uint64_t time;
    NumericT value;

    bool operator==(const data &other) const {
        return time == other.time && value == other.value;
    }

    friend std::ostream &operator<<(std::ostream &out, const data &d) {
        return out << "([time: " << d.time << "]" << " [value: " << d.value << "]";
    }
};

std::time_t getDateTimestamp(int year, int month, int day, int hour, int min, int sec) {
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
int getRandomInRange(int minVal, int maxVal) {
    std::random_device rd;
    std::mt19937_64 gen(rd());

    std::uniform_int_distribution<int> dis(minVal, maxVal);

    return dis(gen);
}

const size_t DEFAULT_TEST_DATA_LEN = 100;

std::vector<uint64_t> getTestDataVecTs(size_t data_len = DEFAULT_TEST_DATA_LEN) {
    auto data_vec_ts = std::vector<uint64_t>(data_len);
    uint64_t current_timestamp = 0;
    for (int i = 0; i < data_len; i++) {
        if (0 < i && i % 10 == 0) {
            current_timestamp -= getRandomInRange(1000, 3000);
        } else {
            current_timestamp += getRandomInRange(2000, 10000);
        }

        data_vec_ts[i] = current_timestamp;
    }

    return data_vec_ts;
}

template<typename T>
std::vector<T> getTestDataVecValues(size_t data_len = DEFAULT_TEST_DATA_LEN) {
    auto data_vec_values = std::vector<T>(data_len);
    T current_value = 0;
    for (int i = 0; i < data_len; i++) {
        data_vec_values[i] = current_value;
        auto current_value_delta = getRandomInRange(-500, 1000);
        if (current_value_delta < 0 && std::abs(current_value_delta) > current_value) {
            current_value = 0;
        } else {
            current_value += current_value_delta;
            if (std::is_same_v<T, double>) {
                current_value += 0.34567;
            }
        }
    }

    return data_vec_values;
}

template<typename T>
std::vector<data<T>> getTestDataVec(size_t data_len = DEFAULT_TEST_DATA_LEN) {
    auto data_vec_ts = getTestDataVecTs(data_len);
    auto data_vec_values = getTestDataVecValues<T>(data_len);

    auto data_vec = std::vector<data<T>>(data_len);
    for (int i = 0; i < data_len; i++) {
        data_vec[i] = data { data_vec_ts[i], data_vec_values[i] };
    }

    return data_vec;
}

const std::string TEST_BATCH_COLUMN_NAME_TIME = "Time";
const std::string TEST_BATCH_COLUMN_NAME_VALUE = "Value";

arrow::Result<std::shared_ptr<arrow::RecordBatch>> getTestDataBatchTs(const std::vector<uint64_t>& data_vec) {
    auto timeColumnBuilder = arrow::TimestampBuilder(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO),
                                                     arrow::default_memory_pool());
    for (const auto &data: data_vec) {
        ARROW_RETURN_NOT_OK(timeColumnBuilder.Append(data));
    }
    std::shared_ptr<arrow::Array> timeColumnArray;
    ARROW_ASSIGN_OR_RAISE(timeColumnArray, timeColumnBuilder.Finish());

    std::shared_ptr<arrow::Field> fieldTime = arrow::field(TEST_BATCH_COLUMN_NAME_TIME, arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO));

    std::shared_ptr<arrow::Schema> schema = arrow::schema({fieldTime});
    std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(schema, data_vec.size(),
                                                                         {timeColumnArray});

    return {batch};
}

template<typename T>
arrow::Result<std::shared_ptr<arrow::RecordBatch>> getTestDataBatchVs(const std::vector<T>& data_vec) {
    std::shared_ptr<arrow::ArrayBuilder> valueColumnBuilder;
    if (std::is_same_v<T, uint64_t>) {
        valueColumnBuilder = std::make_shared<arrow::UInt64Builder>();
    } else if (std::is_same_v<T, uint32_t>) {
        valueColumnBuilder = std::make_shared<arrow::UInt32Builder>();
    } else if (std::is_same_v<T, double>) {
        valueColumnBuilder = std::make_shared<arrow::DoubleBuilder>();
    } else {
        std::cerr << "Unknown value column type to create batch." << std::endl;
        exit(1);
    }
    for (const auto &data: data_vec) {
        if (std::is_same_v<T, uint64_t>) {
            ARROW_RETURN_NOT_OK(std::dynamic_pointer_cast<arrow::UInt64Builder>(valueColumnBuilder)->Append(data));
        } else if (std::is_same_v<T, uint32_t>) {
            ARROW_RETURN_NOT_OK(std::dynamic_pointer_cast<arrow::UInt32Builder>(valueColumnBuilder)->Append(data));
        } else if (std::is_same_v<T, double>) {
            ARROW_RETURN_NOT_OK(std::dynamic_pointer_cast<arrow::DoubleBuilder>(valueColumnBuilder)->Append(data));
        } else {
            std::cerr << "Unknown value column type met to append." << std::endl;
            exit(1);
        }
    }
    std::shared_ptr<arrow::Array> valueColumnArray;
    ARROW_ASSIGN_OR_RAISE(valueColumnArray, valueColumnBuilder->Finish());

    std::shared_ptr<arrow::Field> fieldValue;
    if (std::is_same_v<T, uint64_t>) {
        fieldValue = arrow::field(TEST_BATCH_COLUMN_NAME_VALUE, arrow::uint64());
    } else if (std::is_same_v<T, uint32_t>) {
        fieldValue = arrow::field(TEST_BATCH_COLUMN_NAME_VALUE, arrow::uint32());
    } else if (std::is_same_v<T, double>) {
        fieldValue = arrow::field(TEST_BATCH_COLUMN_NAME_VALUE, std::make_shared<arrow::DoubleType>());
    } else {
        std::cerr << "Unknown value column type met to create column field." << std::endl;
        exit(1);
    }


    std::shared_ptr<arrow::Schema> schema = arrow::schema({fieldValue});
    std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(schema, data_vec.size(),
                                                                         {valueColumnArray});

    return {batch};
}

std::shared_ptr<arrow::RecordBatch> mergeRecordBatches(
        const std::shared_ptr<arrow::RecordBatch>& batch1,
        const std::shared_ptr<arrow::RecordBatch>& batch2
) {
    auto schema_builder = std::make_shared<arrow::SchemaBuilder>();
    for (const auto& field : batch1->schema()->fields()) {
        auto new_field_ptr = std::make_shared<arrow::Field>(field->name(), field->type());
        (void) schema_builder->AddField(new_field_ptr);
    }
    for (const auto& field : batch2->schema()->fields()) {
        auto new_field_ptr = std::make_shared<arrow::Field>(field->name(), field->type());
        (void) schema_builder->AddField(new_field_ptr);
    }
    auto combined_schema = schema_builder->Finish().ValueOrDie();

    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(batch1->num_columns() + batch2->num_columns());
    for (int i = 0; i < batch1->num_columns(); ++i) {
        columns.push_back(batch1->column(i));
    }
    for (int i = 0; i < batch2->num_columns(); ++i) {
        columns.push_back(batch2->column(i));
    }

    auto merged_batch = arrow::RecordBatch::Make(combined_schema, batch1->num_rows(), columns);
    return merged_batch;
}

std::shared_ptr<arrow::RecordBatch> getTestDataBatchPairs(
        const std::shared_ptr<arrow::RecordBatch>& batch_ts,
        const std::shared_ptr<arrow::RecordBatch>& batch_vs
) {
    return mergeRecordBatches(batch_ts, batch_vs);
}

void compareTwoBatches(
        const std::shared_ptr<arrow::RecordBatch>& first,
        const std::shared_ptr<arrow::RecordBatch>& second,
        size_t columns_number
) {
    auto outSink = &std::cout;
    if (!first->schema()->Equals(second->schema())) {
        std::cerr << "Schemas are not equal after deserialization." << std::endl;
        exit(0);
    } else {
        std::cout << "Schema is:" << std::endl;
        (void) arrow::PrettyPrint(*first->schema(), 0, outSink);
        std::cout << std::endl;
    }

    // Somewhy batch->Equals(another_batch) doesn't work.
    for (int columns_index = 0; columns_index < columns_number; columns_index++) {
        auto batch_column_expected = first->column(columns_index);
        auto batch_column_actual = second->column(columns_index);

        if (!batch_column_expected->Equals(batch_column_actual)) {
            std::cout << "Columns with index " << columns_index << " are not equal." << std::endl;
            std::cout << "Expected column: " << std::endl;
            (void) arrow::PrettyPrint(*batch_column_expected, 0, outSink);
            std::cout << "Actual column: " << std::endl;
            (void) arrow::PrettyPrint(*batch_column_actual, 0, outSink);
        }
    }
}