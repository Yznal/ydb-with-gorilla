#pragma once

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <sstream>
#include "compressor.h"
#include "decompressor.h"
#include "test_common.h"

using arrow::Status;

const std::string TEST_OUTPUT_FILE_NAME_BIN = "arrow_output.bin";
const std::string TEST_OUTPUT_FILE_NAME_CSV = "arrow_output.csv";
const std::string TEST_OUTPUT_FILE_NAME_ARROW = "arrow_output.arrow";
const std::string TEST_OUTPUT_FILE_NAME_ARROW_NO_COMPRESSION = "arrow_output_no_compression.arrow";

arrow::Status serialize_data_uncompressed_batch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(TEST_OUTPUT_FILE_NAME_ARROW_NO_COMPRESSION));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> writer,
                          arrow::ipc::MakeFileWriter(outfile, batch->schema()));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    ARROW_RETURN_NOT_OK(writer->Close());
    return arrow::Status::OK();
}

void serialize_data_compressed(uint64_t header, std::vector<data<uint64_t>>& data) {
    std::ofstream bin_ofstream(TEST_OUTPUT_FILE_NAME_BIN, std::ios::binary);
    if (!bin_ofstream.is_open()) {
        std::cerr << "Failed to open integration file as test output buffer." << std::endl;
        exit(1);
    }
    Compressor c_bin(bin_ofstream, header);
    for (auto data_pair : data) {
        c_bin.compress(data_pair.time, data_pair.value);
    }
    c_bin.finish();
    bin_ofstream.close();
}

void serialize_data_compressed(std::vector<data<uint64_t>>& data) {
    auto first_time = data[0].time;
    auto header = first_time - (first_time % (60 * 60 * 2));

    return serialize_data_compressed(header, data);
}

arrow::Status serialize_data_compressed_to_batch(uint64_t header, std::vector<data<uint64_t>>& data) {
    std::stringstream stream;
    Compressor c(stream, header);
    for (auto data_pair : data) {
        c.compress(data_pair.time, data_pair.value);
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

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(TEST_OUTPUT_FILE_NAME_ARROW));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer,
                          arrow::ipc::MakeFileWriter(outfile, schema));
    ARROW_RETURN_NOT_OK(ipc_writer->WriteRecordBatch(*rbatch));
    ARROW_RETURN_NOT_OK(ipc_writer->Close());

    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(TEST_OUTPUT_FILE_NAME_CSV));
    ARROW_ASSIGN_OR_RAISE(auto csv_writer,
                          arrow::csv::MakeCSVWriter(outfile, rbatch->schema()));
    ARROW_RETURN_NOT_OK(csv_writer->WriteRecordBatch(*rbatch));
    ARROW_RETURN_NOT_OK(csv_writer->Close());

    return arrow::Status::OK();
}

arrow::Status serialize_data_compressed_to_batch(std::vector<data<uint64_t>>& data) {
    auto first_time = data[0].time;
    auto header = first_time - (first_time % (60 * 60 * 2));

    return serialize_data_compressed_to_batch(header, data);
}

arrow::Result<std::vector<std::pair<uint64_t, uint64_t>>> decompress_data_batch() {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(
            TEST_OUTPUT_FILE_NAME_ARROW, arrow::default_memory_pool()));

    ARROW_ASSIGN_OR_RAISE(auto ipc_reader, arrow::ipc::RecordBatchFileReader::Open(infile));

    std::shared_ptr<arrow::RecordBatch> rbatch;
    ARROW_ASSIGN_OR_RAISE(rbatch, ipc_reader->ReadRecordBatch(0));

    auto columns_data = rbatch->column_data();
    const auto& c_data = columns_data[0];
    auto column_data_type = c_data->type;

    std::stringstream in_stream;

    arrow::UInt8Array casted_array(c_data);

    for (auto value : casted_array) {
        in_stream << *value;
    }

    Decompressor d(in_stream);
    auto d_header = d.get_header();

    std::vector<std::pair<uint64_t, uint64_t>> data_res;
    std::optional<std::pair<uint64_t, uint64_t>> current_pair = std::nullopt;
    do {
        current_pair = d.next();
        if (current_pair) {
            data_res.push_back(*current_pair);
        }
    } while (current_pair);

    return { data_res };
}
