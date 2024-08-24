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
uint64_t getU64FromArrayData(
        std::shared_ptr<arrow::DataType> &column_type,
        std::shared_ptr<arrow::ArrayData> &array_data,
        size_t i
) {
    uint64_t reinterpreted_value;
    if (column_type->Equals(arrow::uint64())) {
        uint64_t value = array_data->GetValues<uint64_t>(1)[i];
        reinterpreted_value = *reinterpret_cast<uint64_t*>(&value);
    } else if (column_type->Equals(arrow::uint32())) {
        uint32_t value = array_data->GetValues<uint32_t>(1)[i];
        reinterpreted_value = *reinterpret_cast<uint64_t*>(&value);
    } else if (column_type->Equals(arrow::DoubleType())) {
        double value = array_data->GetValues<double>(1)[i];
        reinterpreted_value = *reinterpret_cast<uint64_t*>(&value);
    } else if (column_type->Equals(arrow::TimestampType(arrow::TimeUnit::MICRO))) {
        arrow::TimestampArray casted_timestamp_data(array_data);
        double value = casted_timestamp_data.Value(i);
        reinterpreted_value = *reinterpret_cast<uint64_t*>(&value);
    } else {
        std::cerr << "Unknown value column type met for uint64_t serialization: " << *column_type << std::endl;
        exit(1);
    }
    return reinterpreted_value;
}

std::shared_ptr<arrow::ArrayBuilder> getColumnBuilderByType(
        std::shared_ptr<arrow::DataType> &column_type
) {
    std::shared_ptr<arrow::ArrayBuilder> value_column_builder;
    if (column_type->Equals(arrow::uint64())) {
        value_column_builder = std::make_shared<arrow::UInt64Builder>();
    } else if (column_type->Equals(arrow::uint32())) {
        value_column_builder = std::make_shared<arrow::UInt32Builder>();
    } else if (column_type->Equals(arrow::DoubleType())) {
        value_column_builder = std::make_shared<arrow::DoubleBuilder>();
    } else if (column_type->Equals(arrow::TimestampType(arrow::TimeUnit::MICRO))) {
        value_column_builder = std::make_shared<arrow::TimestampBuilder>(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool());
    } else {
        std::cerr << "Unknown value column type met to get column builder: " << *column_type << std::endl;
        exit(1);
    }
    return value_column_builder;
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
    } else if (column_type->Equals(arrow::TimestampType(arrow::TimeUnit::MICRO))) {
        ARROW_RETURN_NOT_OK(std::dynamic_pointer_cast<arrow::TimestampBuilder>(column_builder)->Append(value));
    } else {
        std::cerr << "Unknown value column type met to append value to builder: " << *column_type << std::endl;
        exit(1);
    }
    return arrow::Status::OK();
}
// ---- ARROW HELPER FUNCTIONS BASED ON ARROW COLUMN TYPES ----

std::vector<uint64_t> getU64VecFromBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch,
        size_t column_index
) {
    std::vector<uint64_t> values_vec;
    auto data = batch->column_data()[column_index];
    auto column_type = batch->schema()->field(column_index)->type();
    auto array_size = data->length;

    values_vec.reserve(array_size);
    for (int i = 0; i < array_size; i++) {
        uint64_t reinterpretedValue = getU64FromArrayData(column_type, data, i);
        values_vec.push_back(reinterpretedValue);
    }

    return values_vec;
}

template<typename T, typename F>
arrow::Result<std::string> serializeForUnknownSchema(
        const std::shared_ptr<arrow::Schema>& batch_schema,
        std::vector<T> &entities,
        F create_c_func
) {
    auto schema_serialized_buffer = arrow::ipc::SerializeSchema(*batch_schema).ValueOrDie();
    auto schema_serialized_str = schema_serialized_buffer->ToString();

    std::stringstream out_stream;
    auto arrays_size = entities.size();

    std::unique_ptr<CompressorBase<T>> c = create_c_func(out_stream);
    for (int i = 0; i < arrays_size; i++) {
        c->compress(entities[i]);
    }
    c->finish();
    std::string compressed = out_stream.str();

    return {std::to_string(schema_serialized_str.length()) + "\n" + schema_serialized_str + compressed };
}

template<typename T>
std::vector<T> deserializeEntities(
        std::unique_ptr<DecompressorBase<T>>& d
) {
    std::vector<T> entities;
    std::optional<T> current_pair;
    do {
        current_pair = d->next();
        if (current_pair) {
            entities.push_back(*current_pair);
        }
    } while (current_pair);
    return entities;
}

template<typename T, typename F>
std::pair<std::shared_ptr<arrow::Schema>, std::vector<T>> deserializeForUnknownSchema(
        const std::string& data,
        F create_d_func
) {
    // Deserialize batch schema.
    size_t div_pos = data.find_first_of('\n');
    if (div_pos == std::string::npos) {
        std::cerr << "Newline divider not found in serialized file." << std::endl;
        exit(1);
    }
    size_t schema_length;
    std::stringstream header_ss((data.substr(0, div_pos)));
    header_ss >> schema_length;
    size_t schema_from_pos = div_pos + 1;
    auto reader_stream = arrow::io::BufferReader::FromString(data.substr(schema_from_pos));
    arrow::ipc::DictionaryMemo dictMemo;
    auto schema_deserialized = arrow::ipc::ReadSchema(reader_stream.get(), &dictMemo).ValueOrDie();

    // Deserialize data.
    std::stringstream in_stream(data.substr(schema_from_pos + schema_length));
    std::unique_ptr<DecompressorBase<T>> d = create_d_func(in_stream);
    auto deserialized_entities = deserializeEntities(d);

    return std::make_pair(schema_deserialized, deserialized_entities);
}

void compare_two_batches(
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

arrow::Status test_deserialization_scenario_without_known_schema_for_single_column_batch(
        const std::shared_ptr<arrow::RecordBatch>& batch
) {
    auto initial_schema = batch->schema();
    auto column_type = initial_schema->field(0)->type();

    auto entities_vec = getU64VecFromBatch(batch, 0);
    arrow::Result<std::string> serialization_res;
    if (column_type->Equals(arrow::TimestampType(arrow::TimeUnit::MICRO))) {
        serialization_res = serializeForUnknownSchema(initial_schema, entities_vec, [](std::stringstream &out_stream) {
            auto bw = std::make_shared<BitWriter>(out_stream);
            return std::make_unique<TimestampsCompressor>(bw);
        });
    } else {
        serialization_res = serializeForUnknownSchema(initial_schema, entities_vec, [](std::stringstream &out_stream) {
            auto bw = std::make_shared<BitWriter>(out_stream);
            return std::make_unique<ValuesCompressor>(bw);
        });
    }

    if (!serialization_res.ok()) {
        std::cerr << "Arrow throw an error on serializeForUnknownSchema." << std::endl;
        exit(1);
    }
    auto serializedBatch = serialization_res.ValueOrDie();

    std::shared_ptr<arrow::Schema> schema;
    std::vector<uint64_t> entities;
    if (column_type->Equals(arrow::TimestampType(arrow::TimeUnit::MICRO))) {
        auto [des_schema, des_entities] = deserializeForUnknownSchema<uint64_t>(serializedBatch, [](std::stringstream &in_stream) {
            auto br = std::make_shared<BitReader>(in_stream);
            return std::make_unique<TimestampsDecompressor>(br);
        });
        schema = des_schema;
        entities = des_entities;
    } else {
        auto [des_schema, des_entities] = deserializeForUnknownSchema<uint64_t>(serializedBatch, [](std::stringstream &in_stream) {
            auto br = std::make_shared<BitReader>(in_stream);
            return std::make_unique<ValuesDecompressor>(br);
        });
        schema = des_schema;
        entities = des_entities;
    }
    auto column_builder = getColumnBuilderByType(column_type);
    for (auto e : entities) {
        ARROW_RETURN_NOT_OK(builderAppendValue(column_type, column_builder, e));
    }

    std::shared_ptr<arrow::Array> column_array;
    ARROW_ASSIGN_OR_RAISE(column_array, column_builder->Finish());

    std::shared_ptr<arrow::RecordBatch> batch_deserialized = arrow::RecordBatch::Make(schema, entities.size(), {column_array});

    auto validation = batch->Validate();
    if (!validation.ok()) {
        std::cerr << "Validation error: " << validation.ToString() << std::endl;
        return arrow::Status(arrow::StatusCode::SerializationError, "");
    }

    compare_two_batches(batch, batch_deserialized, 1);
    return arrow::Status::OK();
}

arrow::Status test_deserialization_scenario_without_known_schema_timestamps() {
    auto data_vec = get_test_data_vec_ts();
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> batch_res = get_test_data_batch_ts(data_vec);
    if (!batch_res.ok()) {
        std::cerr << "Arrow throw an error on getting batch result." << std::endl;
        exit(1);
    }
    auto batch = batch_res.ValueOrDie();
    auto batch_schema = batch->schema();

    auto ts_vec = getU64VecFromBatch(batch, 0);

    auto serialization_res = serializeForUnknownSchema(batch_schema, ts_vec, [](std::stringstream &out_stream) {
        auto bw = std::make_shared<BitWriter>(out_stream);
        return std::make_unique<TimestampsCompressor>(bw);
    });
    if (!serialization_res.ok()) {
        std::cerr << "Arrow throw an error on serializeForUnknownSchema." << std::endl;
        exit(1);
    }
    auto serializedBatch = serialization_res.ValueOrDie();

    auto [schema, entities] = deserializeForUnknownSchema<uint64_t>(serializedBatch, [](std::stringstream &in_stream) {
        auto br = std::make_shared<BitReader>(in_stream);
        return std::make_unique<TimestampsDecompressor>(br);
    });
    auto time_column_builder = arrow::TimestampBuilder(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool());
    for (auto t : entities) {
        ARROW_RETURN_NOT_OK(time_column_builder.Append(t));
    }

    std::shared_ptr<arrow::Array> time_column_array;
    ARROW_ASSIGN_OR_RAISE(time_column_array, time_column_builder.Finish());

    std::shared_ptr<arrow::RecordBatch> batch_deserialized = arrow::RecordBatch::Make(schema, entities.size(), {time_column_array});

    auto validation = batch->Validate();
    if (!validation.ok()) {
        std::cerr << "Validation error: " << validation.ToString() << std::endl;
        return arrow::Status(arrow::StatusCode::SerializationError, "");
    }

    compare_two_batches(batch, batch_deserialized, 1);
    return arrow::Status::OK();
}

template<typename T>
arrow::Status test_deserialization_scenario_without_known_schema_values() {
    auto data_vec = get_test_data_vec_values<T>();
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> batch_res = get_test_data_batch_vs<T>(data_vec);
    if (!batch_res.ok()) {
        std::cerr << "Arrow throw an error on getting batch result." << std::endl;
        exit(1);
    }
    auto batch = batch_res.ValueOrDie();
    auto batch_schema = batch->schema();

    auto vs_vec = getU64VecFromBatch(batch, 0);
    auto serialization_res = serializeForUnknownSchema(batch_schema, vs_vec, [](std::stringstream &outStream) {
        auto bw = std::make_shared<BitWriter>(outStream);
        return std::make_unique<ValuesCompressor>(bw);
    });
    if (!serialization_res.ok()) {
        std::cerr << "Arrow throw an error on serializeForUnknownSchema." << std::endl;
        exit(1);
    }
    auto serializedBatch = serialization_res.ValueOrDie();

    auto [schema, entities] = deserializeForUnknownSchema<uint64_t>(serializedBatch, [](std::stringstream &in_stream) {
        auto br = std::make_shared<BitReader>(in_stream);
        return std::make_unique<ValuesDecompressor>(br);
    });
    auto value_column_type = schema->field(0)->type();
    std::shared_ptr<arrow::ArrayBuilder> value_column_builder = getColumnBuilderByType(value_column_type);
    for (auto v : entities) {
        ARROW_RETURN_NOT_OK(builderAppendValue(value_column_type, value_column_builder, v));
    }

    std::shared_ptr<arrow::Array> value_column_array;
    ARROW_ASSIGN_OR_RAISE(value_column_array, value_column_builder->Finish());

    std::shared_ptr<arrow::RecordBatch> batch_deserialized = arrow::RecordBatch::Make(schema, entities.size(), {value_column_array});

    compare_two_batches(batch, batch_deserialized, 1);
    return arrow::Status::OK();
}

template<typename T>
arrow::Status test_deserialization_scenario_without_known_schema_pairs() {
    auto data_vec = get_test_data_vec<T>();
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> batch_res = get_test_data_batch_pairs<T>(data_vec);
    if (!batch_res.ok()) {
        std::cerr << "Arrow throw an error on getting batch result." << std::endl;
        exit(1);
    }
    auto batch = batch_res.ValueOrDie();
    auto batch_schema = batch->schema();

    auto ts_vec = getU64VecFromBatch(batch, 0);
    auto vs_vec = getU64VecFromBatch(batch, 1);

    std::vector<std::pair<uint64_t, uint64_t>> zipped(ts_vec.size());
    std::transform(ts_vec.begin(), ts_vec.end(), vs_vec.begin(), zipped.begin(),
                   [](uint64_t a, uint64_t b) { return std::make_pair(a, b); });

    auto serialization_res = serializeForUnknownSchema(batch_schema, zipped, [](std::stringstream &outStream) {
        auto bw = std::make_shared<BitWriter>(outStream);
        return std::make_unique<PairsCompressor>(bw);
    });
    if (!serialization_res.ok()) {
        std::cerr << "Arrow throw an error on serializeForUnknownSchema." << std::endl;
        exit(1);
    }
    auto serializedBatch = serialization_res.ValueOrDie();

    auto [schema, entities] = deserializeForUnknownSchema<std::pair<uint64_t, uint64_t >>(serializedBatch, [](std::stringstream &in_stream) {
        auto br = std::make_shared<BitReader>(in_stream);
        return std::make_unique<PairsDecompressor>(br);
    });
    auto time_column_builder = arrow::TimestampBuilder(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool());
    auto value_column_type = schema->field(1)->type();
    std::shared_ptr<arrow::ArrayBuilder> value_column_builder = getColumnBuilderByType(value_column_type);
    for (auto [t, v] : entities) {
        ARROW_RETURN_NOT_OK(time_column_builder.Append(t));
        ARROW_RETURN_NOT_OK(builderAppendValue(value_column_type, value_column_builder, v));
    }

    std::shared_ptr<arrow::Array> time_column_array;
    ARROW_ASSIGN_OR_RAISE(time_column_array, time_column_builder.Finish());
    std::shared_ptr<arrow::Array> value_column_array;
    ARROW_ASSIGN_OR_RAISE(value_column_array, value_column_builder->Finish());

    std::shared_ptr<arrow::RecordBatch> batch_deserialized = arrow::RecordBatch::Make(schema, entities.size(), {time_column_array, value_column_array});

    auto validation = batch_deserialized->Validate();
    if (!validation.ok()) {
        std::cerr << "Validation error: " << validation.ToString() << std::endl;
        return arrow::Status(arrow::StatusCode::SerializationError, "");
    }

    compare_two_batches(batch, batch_deserialized, 2);
    return arrow::Status::OK();
}

template<typename T>
void test_deserialization_scenario_without_known_schema() {
    arrow::Status res;
    res = test_deserialization_scenario_without_known_schema_values<uint64_t>();
    if (!res.ok()) {
        std::cerr << "Arrow throw an error on values deserialization." << std::endl;
        exit(1);
    }
    res = test_deserialization_scenario_without_known_schema_timestamps();
    if (!res.ok()) {
        std::cerr << "Arrow throw an error on timestamps deserialization." << std::endl;
        exit(1);
    }
    res = test_deserialization_scenario_without_known_schema_pairs<uint64_t>();
    if (!res.ok()) {
        std::cerr << "Arrow throw an error on pairs deserialization." << std::endl;
        exit(1);
    }
}

void test_compress_decompress_pairs() {
    auto data_vec = get_test_data_vec<uint64_t>();
    serialize_data_compressed(data_vec);

    auto batch_res = get_test_data_batch_pairs<uint64_t>(data_vec);
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
    if (expected_data_vec_size != actual_data_vec_size) {
        std::cerr << "Deserialized less rows. Expected: " << expected_data_vec_size << ", got: " << actual_data_vec_size << "." << std::endl;

        exit(1);
    }

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
    test_deserialization_scenario_without_known_schema<uint64_t>();
}