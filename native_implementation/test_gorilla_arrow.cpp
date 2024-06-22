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
int main() {
    data_vec d_vec {
        // 06/20/2024, 12:41:57
        std::make_pair(1718869317, 10),
        std::make_pair(1718879317, 20),
        std::make_pair(1718889317, 40),
        std::make_pair(1718899317, 30),
    };

    Status serialization_st = compress_column(d_vec);
    if (!serialization_st.ok()) {
        std::cerr << "Serialization status: " << serialization_st << std::endl;
        exit(1);
    }

    Status deserialization_st = decompress_column();
    if (!deserialization_st.ok()) {
        std::cerr << "Deserialization status: " << serialization_st << std::endl;
        exit(1);
    }
}