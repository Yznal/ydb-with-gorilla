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

arrow::Status testDeserializationScenarioWithoutKnownSchemaForSingleColumnBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch
) {
    arrow::Result<std::string> serialization_res = serializeSingleColumnBatch(batch);

    if (!serialization_res.ok()) {
        std::cerr << "Arrow throw an error on batch serialization." << std::endl;
        exit(1);
    }
    auto serialized_batch = serialization_res.ValueOrDie();

    auto batch_deserialized_res = deserializeSingleColumnBatch(serialized_batch);
    if (!batch_deserialized_res.ok()) {
        std::cerr << "Arrow throw an error on batch deserialization." << std::endl;
        exit(1);
    }
    auto batch_deserialized = batch_deserialized_res.ValueOrDie();

    compareTwoBatches(batch, batch_deserialized, 1);
    return arrow::Status::OK();
}

template<typename T>
arrow::Status testDeserializationScenarioWithoutKnownSchemaPairs(
        const std::shared_ptr<arrow::RecordBatch>& batch_ts,
        const std::shared_ptr<arrow::RecordBatch>& batch_vs
) {
    auto batch = getTestDataBatchPairs(batch_ts, batch_vs);
    auto serialized_batch = serializePairsBatch(batch).ValueOrDie();

    auto batch_deserialized_res = deserializePairsBatch(serialized_batch);

    if (!batch_deserialized_res.ok()) {
        std::cerr << "Arrow throw an error on batch deserialization." << std::endl;
        exit(1);
    }
    auto batch_deserialized = batch_deserialized_res.ValueOrDie();

    compareTwoBatches(batch, batch_deserialized, 2);
    return arrow::Status::OK();
}

template<typename T>
void testDeserializationScenarioWithoutKnownSchema() {
    arrow::Status res;
    auto ts_vec = getTestDataVecTs();
    auto batch_ts = getTestDataBatchTs(ts_vec).ValueOrDie();
    res = testDeserializationScenarioWithoutKnownSchemaForSingleColumnBatch(batch_ts);
    if (!res.ok()) {
        std::cerr << "Arrow throw an error on timestamps deserialization." << std::endl;
        exit(1);
    }
    auto vs_vec = getTestDataVecValues<T>();
    auto batch_vs = getTestDataBatchVs(vs_vec).ValueOrDie();
    res = testDeserializationScenarioWithoutKnownSchemaForSingleColumnBatch(batch_vs);
    if (!res.ok()) {
        std::cerr << "Arrow throw an error on values deserialization." << std::endl;
        exit(1);
    }
    res = testDeserializationScenarioWithoutKnownSchemaPairs<uint64_t>(
            batch_ts, batch_vs
    );
    if (!res.ok()) {
        std::cerr << "Arrow throw an error on pairs deserialization." << std::endl;
        exit(1);
    }
}

void testCompressDecompressPairs() {
    auto data_vec = getTestDataVec<uint64_t>();
    serializeDataCompressed(data_vec);

    auto ts_vec = getTestDataVecTs();
    auto batch_ts = getTestDataBatchTs(ts_vec).ValueOrDie();
    auto vs_vec = getTestDataVecValues<uint64_t>();
    auto batch_vs = getTestDataBatchVs(vs_vec).ValueOrDie();
    auto batch = getTestDataBatchPairs(batch_ts, batch_vs);
    Status serialization_no_compression_st = serializeDataUncompressedBatch(batch);
    if (!serialization_no_compression_st.ok()) {
        std::cerr << "Serialization status (no compression): " << serialization_no_compression_st << std::endl;
        exit(1);
    }
    Status serialization_compression_st = serializeDataCompressedToBatch(data_vec);
    if (!serialization_compression_st.ok()) {
        std::cerr << "Serialization status (with compression): " << serialization_compression_st << std::endl;
        exit(1);
    }

    arrow::Result<std::vector<std::pair<uint64_t, uint64_t>>> des_data_res = decompressDataBatch();
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
    testCompressDecompressPairs();
    testDeserializationScenarioWithoutKnownSchema<uint64_t>();
}