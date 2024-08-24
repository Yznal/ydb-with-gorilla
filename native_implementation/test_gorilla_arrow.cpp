#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <iostream>
#include <ctime>
#include <string>
#include <ranges>

#include "gorilla_utils.h"

using arrow::Status;

// ---- ARROW HELPER FUNCTIONS BASED ON ARROW COLUMN TYPES ----
uint64_t getU64FromValuesData(
        std::shared_ptr<arrow::DataType> &column_type,
        std::shared_ptr<arrow::ArrayData> &array_data,
        size_t i
) {
    uint64_t reinterpretedValue;
    if (column_type->Equals(arrow::uint64())) {
        uint64_t value = array_data->GetValues<uint64_t>(1)[i];
        reinterpretedValue = *reinterpret_cast<uint64_t*>(&value);
    } else if (column_type->Equals(arrow::uint32())) {
        uint32_t value = array_data->GetValues<uint32_t>(1)[i];
        reinterpretedValue = *reinterpret_cast<uint64_t*>(&value);
    } else if (column_type->Equals(arrow::DoubleType())) {
        double value = array_data->GetValues<double>(1)[i];
        reinterpretedValue = *reinterpret_cast<uint64_t*>(&value);
    } else {
        std::cerr << "Unknown value column type met for uint64_t serialization: " << *column_type << std::endl;
        exit(1);
    }
    return reinterpretedValue;
}

std::shared_ptr<arrow::ArrayBuilder> getColumnBuilder(
        std::shared_ptr<arrow::DataType> &column_type
) {
    std::shared_ptr<arrow::ArrayBuilder> valueColumnBuilder;
    if (column_type->Equals(arrow::uint64())) {
        valueColumnBuilder = std::make_shared<arrow::UInt64Builder>();
    } else if (column_type->Equals(arrow::uint32())) {
        valueColumnBuilder = std::make_shared<arrow::UInt32Builder>();
    } else if (column_type->Equals(arrow::DoubleType())) {
        valueColumnBuilder = std::make_shared<arrow::DoubleBuilder>();
    } else {
        std::cerr << "Unknown value column type met to get column builder: " << *column_type << std::endl;
        exit(1);
    }
    return valueColumnBuilder;
}

arrow::Status builderAppendValue(
        std::shared_ptr<arrow::DataType> &column_type,
        std::shared_ptr<arrow::ArrayBuilder> &column_builder,
        uint64_t value
) {
    if (column_type->Equals(arrow::uint64())) {
        ARROW_RETURN_NOT_OK(std::dynamic_pointer_cast<arrow::UInt64Builder>(column_builder)->Append(value));
    } else if (column_type->Equals(arrow::uint32())) {
        uint32_t reinterpreted_value = *reinterpret_cast<uint32_t*>(&value);
        ARROW_RETURN_NOT_OK(std::dynamic_pointer_cast<arrow::UInt32Builder>(column_builder)->Append(reinterpreted_value));
    } else if (column_type->Equals(arrow::DoubleType())) {
        double reinterpreted_value = *reinterpret_cast<double*>(&value);
        ARROW_RETURN_NOT_OK(std::dynamic_pointer_cast<arrow::DoubleBuilder>(column_builder)->Append(reinterpreted_value));
    } else {
        std::cerr << "Unknown value column type met to append value to builder: " << *column_type << std::endl;
        exit(1);
    }
    return arrow::Status::OK();
}
// ---- ARROW HELPER FUNCTIONS BASED ON ARROW COLUMN TYPES ----

arrow::Result<std::string> serializeForUnknownSchemaTimestampsOnly(
        const std::shared_ptr<arrow::RecordBatch>& batch,
        size_t timestamp_column_index
) {
    std::stringstream outStream;

    auto timestampData = batch->column_data()[timestamp_column_index];
    arrow::TimestampArray castedTimestampData(timestampData);
    auto arraysSize = timestampData->length;

    uint64_t header = getHeaderFromTimestamp(castedTimestampData.Value(0));
    auto schemaSerializedBuffer = arrow::ipc::SerializeSchema(*batch->schema()).ValueOrDie();
    auto schemaSerializedStr = schemaSerializedBuffer->ToString();

    TimestampsCompressor c(outStream, header);
    for (int i = 0; i < arraysSize; i++) {
        c.compress(castedTimestampData.Value(i));
    }
    c.finish();

    std::string compressed = outStream.str();
    return { std::to_string(schemaSerializedStr.length()) + "\n" + schemaSerializedStr + compressed };
}

arrow::Result<std::string> serializeForUnknownSchema(const std::shared_ptr<arrow::RecordBatch>& batch) {
    std::stringstream outStream;

    auto timestampData = batch->column_data()[0];
    arrow::TimestampArray castedTimestampData(timestampData);
    auto valuesData = batch->column_data()[1];
    auto valueColumnType = batch->schema()->field(1)->type();
    auto arraysSize = timestampData->length;

    uint64_t header = getHeaderFromTimestamp(castedTimestampData.Value(0));
    auto schemaSerializedBuffer = arrow::ipc::SerializeSchema(*batch->schema()).ValueOrDie();
    auto schemaSerializedStr = schemaSerializedBuffer->ToString();

    PairsCompressor c(outStream, header);
    for (int i = 0; i < arraysSize; i++) {
        uint64_t reinterpretedValue = getU64FromValuesData(valueColumnType, valuesData, i);
        c.compress(std::make_pair(castedTimestampData.Value(i), reinterpretedValue));
    }
    c.finish();

    std::string compressed = outStream.str();
    return { std::to_string(schemaSerializedStr.length()) + "\n" + schemaSerializedStr + compressed };
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> deserializeForUnknownSchema(const std::string& data) {
    size_t divPos = data.find_first_of('\n');
    if (divPos == std::string::npos) {
        std::cerr << "Newline divider not found in serialized file." << std::endl;
        exit(1);
    }
    size_t schemaLength;
    std::stringstream header_ss((data.substr(0, divPos)));
    header_ss >> schemaLength;

    size_t schemaFromPos = divPos + 1;
    auto readerStream = arrow::io::BufferReader::FromString(data.substr(schemaFromPos));
    arrow::ipc::DictionaryMemo dictMemo;
    auto schemaDeserialized = arrow::ipc::ReadSchema(readerStream.get(), &dictMemo).ValueOrDie();
    auto valueColumnType = schemaDeserialized->field(1)->type();

    std::stringstream in_stream(data.substr(schemaFromPos + schemaLength));

    auto timeColumnBuilder = arrow::TimestampBuilder(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool());
    std::shared_ptr<int> a = std::make_shared<int>(1);
    std::shared_ptr<arrow::ArrayBuilder> valueColumnBuilder = getColumnBuilder(valueColumnType);

    PairsDecompressor d(in_stream);
    std::optional<std::pair<uint64_t, uint64_t>> current_pair = std::nullopt;
    int rows_counter = 0;
    do {
        current_pair = d.next();
        if (current_pair) {
            ARROW_RETURN_NOT_OK(timeColumnBuilder.Append((*current_pair).first));

            uint64_t value = (*current_pair).second;
            ARROW_RETURN_NOT_OK(builderAppendValue(valueColumnType, valueColumnBuilder, value));

            rows_counter++;
        }
    } while (current_pair);

    std::shared_ptr<arrow::Array> timeColumnArray;
    ARROW_ASSIGN_OR_RAISE(timeColumnArray, timeColumnBuilder.Finish());
    std::shared_ptr<arrow::Array> valueColumnArray;
    ARROW_ASSIGN_OR_RAISE(valueColumnArray, valueColumnBuilder->Finish());

    std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(schemaDeserialized, rows_counter, {timeColumnArray, valueColumnArray});

    auto validation = batch->Validate();
    if (!validation.ok()) {
        std::cerr << "Validation error: " << validation.ToString() << std::endl;
        return arrow::Status(arrow::StatusCode::SerializationError, "");
    }
    return { batch };
}

template<typename T>
void deserialization_scenario_without_known_schema() {
    auto [_, data_vec] = get_test_data_vec<T>();
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> batch_res = get_test_data_batch<T>(data_vec);
    if (!batch_res.ok()) {
        std::cerr << "Arrow throw an error on getting batch result." << std::endl;
        exit(1);
    }
    auto batch = batch_res.ValueOrDie();

    auto serialization_res = serializeForUnknownSchema(batch);
    if (!serialization_res.ok()) {
        std::cerr << "Arrow throw an error on serializeForUnknownSchema." << std::endl;
        exit(1);
    }
    auto serializedBatch = serialization_res.ValueOrDie();

    auto deserialization_res = deserializeForUnknownSchema(serializedBatch);
    if (!deserialization_res.ok()) {
        std::cerr << "Arrow throw an error on deserializeForUnknownSchema." << std::endl;
        exit(1);
    }
    auto batchDeserialized = deserialization_res.ValueOrDie();
    auto outSink = &std::cout;

    if (!batch->schema()->Equals(batchDeserialized->schema())) {
        std::cerr << "Schemas are not equal after deserialization." << std::endl;
        exit(0);
    } else {
        std::cout << "Schema is:" << std::endl;
        (void) arrow::PrettyPrint(*batch->schema(), 0, outSink);
        std::cout << std::endl;
    }

    // Somewhy batch->Equals(another_batch) doesn't work.
    for (int columns_index = 0; columns_index < 2; columns_index++) {
        auto batch_column_expected = batch->column(columns_index);
        auto batch_column_actual = batchDeserialized->column(columns_index);

        if (!batch_column_expected->Equals(batch_column_actual)) {
            std::cout << "Columns with index " << columns_index << " are not equal." << std::endl;
            std::cout << "Expected column: " << std::endl;
            (void) arrow::PrettyPrint(*batch_column_expected, 0, outSink);
            std::cout << "Actual column: " << std::endl;
            (void) arrow::PrettyPrint(*batch_column_actual, 0, outSink);
        }
    }
}

void test_compress_decompress_pairs() {
    auto [_, data_vec] = get_test_data_vec<uint64_t>();
    serialize_data_compressed(data_vec);

    auto batch_res = get_test_data_batch<uint64_t>(data_vec);
    if (!batch_res.ok()) {
        std::cerr << "Arrow throw an error." << std::endl;
        exit(1);
    }
    auto batch = batch_res.ValueOrDie();
    Status serialization_no_compression_st = serialize_data_uncompressed_batch(batch);
    if (!serialization_no_compression_st.ok()) {
        std::cerr << "Serialization status (no compression): " << serialization_no_compression_st << std::endl;
        exit(1);
    }
    Status serialization_compression_st = serialize_data_compressed_to_batch(data_vec);
    if (!serialization_compression_st.ok()) {
        std::cerr << "Serialization status (with compression): " << serialization_compression_st << std::endl;
        exit(1);
    }

    arrow::Result<std::vector<std::pair<uint64_t, uint64_t>>> des_data_res = decompress_data_batch();
    std::vector<std::pair<uint64_t, uint64_t>> data_vec_des = des_data_res.ValueOrDie();

    auto expected_data_vec_size = data_vec.size();
    auto actual_data_vec_size = data_vec_des.size();
//    if (expected_data_vec_size != actual_data_vec_size) {
//        std::cerr << "Deserialized less rows. Expected: " << expected_data_vec_size << ", got: " << actual_data_vec_size << "." << std::endl;
//
//        exit(1);
//    }

    for (int i = 0; i < data_vec.size(); i++) {
        auto [expected_time, expected_value] = data_vec[i];
        auto [actual_time, actual_value] = data_vec_des[i];
        if (expected_time != actual_time) {
            std::cerr << "i = " << i << ". Times not equal: " << expected_time << " != " << actual_time << std::endl;
            exit(1);
        }
        if (expected_value != actual_value) {
            std::cerr << "i = " << i << ". Values not equal." << expected_value << " != " << actual_value << std::endl;
            exit(1);
        }
    }
}

// Prerequisites:
// Install arrow using package manager or build from source.
// `sudo apt install -y -V libarrow-dev`
//
// To run execute:
// `cmake . && make arrow_gorilla_test && ./arrow_gorilla_test`
//
// To see binary output:
// `xxd -b cmake-build-debug/arrow_test_output.bin`
//
// To see binary size:
// `du -sh cmake-build-debug/arrow_output.arrow`
// `du -sh cmake-build-debug/arrow_output_no_compression.arrow`
// Average compression value is ~0.7
int main() {
    test_compress_decompress_pairs();
    deserialization_scenario_without_known_schema<uint64_t>();
}