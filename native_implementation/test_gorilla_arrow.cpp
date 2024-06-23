#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <iostream>
#include <ctime>

#include "arrow_gorilla.h"

using arrow::Status;

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
    auto test_data_res = get_test_data();
    if (!test_data_res.ok()) {
        std::cerr << "Arrow throw an error." << std::endl;
        exit(1);
    }
    auto test_data = test_data_res.ValueOrDie();
    Status serialization_no_compression_st = serialize_data_uncompressed(test_data.data_batch);
    Status serialization_st = serialize_data_compressed(test_data.data_vec);
    if (!serialization_st.ok()) {
        std::cerr << "Serialization status: " << serialization_st << std::endl;
        exit(1);
    }

    Status deserialization_st = decompress_data();
    if (!deserialization_st.ok()) {
        std::cerr << "Deserialization status: " << serialization_st << std::endl;
        exit(1);
    }
}