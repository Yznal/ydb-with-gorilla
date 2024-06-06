#pragma once

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <sstream>
#include "compressor.h"
#include "decompressor.h"

using arrow::Status;

using data_vec = std::vector<std::pair<uint64_t, uint64_t>>;

struct column_data {
    data_vec data;
    std::string name;
};

using column_vec = std::vector<column_data>;

arrow::Status compress_column(uint64_t header, const column_data& c_data) {
    std::stringstream stream;

    Compressor c(stream, header);
    for (auto &[time, value] : c_data.data) {
        c.compress(time, value);
    }
    c.finish();

    arrow::UInt8Builder data_array_builder;
    const std::string& bytes = stream.str();
    for (auto char_byte : bytes) {
        auto byte = static_cast<uint8_t>(char_byte);
        ARROW_RETURN_NOT_OK(data_array_builder.Append(byte));
    }
    std::shared_ptr<arrow::Array> data_array;
    ARROW_ASSIGN_OR_RAISE(data_array, data_array_builder.Finish());
    std::shared_ptr<arrow::Field> column_field = arrow::field("Data", arrow::uint8());
    std::shared_ptr<arrow::Schema> schema = arrow::schema({column_field});
    std::shared_ptr<arrow::RecordBatch> rbatch = arrow::RecordBatch::Make(schema, bytes.size(), {data_array});

    // NOTE: Just to see arrow output.
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("arrow_test_output.csv"));
    ARROW_ASSIGN_OR_RAISE(auto csv_writer,
                          arrow::csv::MakeCSVWriter(outfile, rbatch->schema()));
    ARROW_RETURN_NOT_OK(csv_writer->WriteRecordBatch(*rbatch));
    ARROW_RETURN_NOT_OK(csv_writer->Close());

    return arrow::Status::OK();
}

// YDB data flow:
// Columns data -> std::shared_ptr<arrow::RecordBatch> -> TString (written to disk/memory).
arrow::Status compress_data(uint64_t header, const column_vec& c_vec) {
    // TODO.
    return arrow::Status::OK();
}

// YDB data flow:
// TString (+ optional std::shared_ptr<arrow::Schema>) -> std::shared_ptr<arrow::RecordBatch> -> Column data.
column_vec decompress_data() {
    // TODO.
    column_vec decompressed_data;
    return decompressed_data;
}
