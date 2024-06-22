#pragma once

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <sstream>
#include "compressor.h"
#include "decompressor.h"

using arrow::Status;

// Pairs of { time, value }.
using data_vec = std::vector<std::pair<uint64_t, uint64_t>>;

const std::string TEST_OUTPUT_FILE_NAME_CSV = "arrow_test_output.csv";
const std::string TEST_OUTPUT_FILE_NAME_ARROW = "arrow_test_output.arrow";

arrow::Status compress_column(uint64_t header, const data_vec& data) {
    std::cout << "SERIALIZATION   -- START." << std::endl;
    std::stringstream stream;

    Compressor c(stream, header);
    for (auto &[time, value] : data) {
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

    std::cout << "SERIALIZATION   -- FINISH." << std::endl;

    return arrow::Status::OK();
}

arrow::Status compress_column(const data_vec& data) {
    auto first_time = data[0].first;
    auto header = first_time - (first_time % (60 * 60 * 2));

    return compress_column(header, data);
}

Status decompress_column() {
    std::cout << "DESERIALIZATION -- START." << std::endl;
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(
            TEST_OUTPUT_FILE_NAME_ARROW, arrow::default_memory_pool()));

    ARROW_ASSIGN_OR_RAISE(auto ipc_reader, arrow::ipc::RecordBatchFileReader::Open(infile));

    std::shared_ptr<arrow::RecordBatch> rbatch;
    ARROW_ASSIGN_OR_RAISE(rbatch, ipc_reader->ReadRecordBatch(0));

    arrow::Schema schema_des = *rbatch->schema();
    for (int i = 0; i < schema_des.num_fields(); i++) {
        const auto& field = schema_des.field(i);
        auto field_type = field->type();

        std::cout << "Handling field: " << i << std::endl;
        std::cout << "Field name: " << field->name() << std::endl;
        std::cout << "Field type: " << *field_type << std::endl;

        if (field_type->Equals(arrow::TimestampType(arrow::TimeUnit::MICRO))) {
            std::cout << "Faced timestamp" << std::endl;
        }
        std::cout << std::endl;
    }

    auto columns_data = rbatch->column_data();
    for (const auto& c_data : columns_data) {
        auto column_data_type = c_data->type;
        std::cout << "Type: " << *column_data_type << std::endl;

        std::stringstream in_stream;

        arrow::UInt8Array casted_array(c_data);

        for (auto value : casted_array) {
            in_stream << *value;
        }

        Decompressor d(in_stream);
        auto d_header = d.get_header();
        std::cout << "Decompressed header: " << d_header << std::endl;
        for (int i = 0; i < 10; i++) {
            std::pair<uint64_t, uint64_t> current_pair = d.next();
            std::cout << "Decompressed time: " << current_pair.first << std::endl;
            std::cout << "Decompressed value: " << current_pair.second << std::endl;
        }
    }

    std::cout << "DESERIALIZATION -- FINISH." << std::endl;
    return arrow::Status::OK();
}
