#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <iostream>

#include "arrow_gorilla.h"

using arrow::Status;

arrow::Status generate_test_example_data() {
    arrow::Int8Builder int8builder;
    int8_t days_raw[5] = {1, 12, 17, 23, 28};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw, 5));
    std::shared_ptr<arrow::Array> days;
    ARROW_ASSIGN_OR_RAISE(days, int8builder.Finish());

    int8_t months_raw[5] = {1, 3, 5, 7, 1};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw, 5));
    std::shared_ptr<arrow::Array> months;
    ARROW_ASSIGN_OR_RAISE(months, int8builder.Finish());

    arrow::Int16Builder int16builder;
    int16_t years_raw[5] = {1990, 2000, 1995, 2000, 1995};
    ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw, 5));
    std::shared_ptr<arrow::Array> years;
    ARROW_ASSIGN_OR_RAISE(years, int16builder.Finish());

    std::vector<std::shared_ptr<arrow::Array>> columns = {days, months, years};
    std::shared_ptr<arrow::Field> field_day, field_month, field_year;
    std::shared_ptr<arrow::Schema> schema;

    field_day = arrow::field("Day", arrow::int8());
    field_month = arrow::field("Month", arrow::int8());
    field_year = arrow::field("Year", arrow::int16());

    schema = arrow::schema({field_day, field_month, field_year});
    std::shared_ptr<arrow::Table> table;
    table = arrow::Table::Make(schema, columns);

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.arrow"));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer,
                          arrow::ipc::MakeFileWriter(outfile, schema));
    ARROW_RETURN_NOT_OK(ipc_writer->WriteTable(*table));
    ARROW_RETURN_NOT_OK(ipc_writer->Close());

    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.csv"));
    ARROW_ASSIGN_OR_RAISE(auto csv_writer,
                          arrow::csv::MakeCSVWriter(outfile, table->schema()));
    ARROW_RETURN_NOT_OK(csv_writer->WriteTable(*table));
    ARROW_RETURN_NOT_OK(csv_writer->Close());

    return arrow::Status::OK();
}

Status test_arrow() {
    ARROW_RETURN_NOT_OK(generate_test_example_data());

    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(
            "test_in.arrow", arrow::default_memory_pool()));

    ARROW_ASSIGN_OR_RAISE(auto ipc_reader, arrow::ipc::RecordBatchFileReader::Open(infile));

    std::shared_ptr<arrow::RecordBatch> rbatch;
    ARROW_ASSIGN_OR_RAISE(rbatch, ipc_reader->ReadRecordBatch(0));

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_out.arrow"));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer,
                          arrow::ipc::MakeFileWriter(outfile, rbatch->schema()));
    ARROW_RETURN_NOT_OK(ipc_writer->WriteRecordBatch(*rbatch));
    ARROW_RETURN_NOT_OK(ipc_writer->Close());

    return Status::OK();
}

arrow::Status custom_play() {
    auto first_field_builder = arrow::TimestampBuilder(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool());
    ARROW_RETURN_NOT_OK(first_field_builder.Append(11));
    ARROW_RETURN_NOT_OK(first_field_builder.Append(21));
    std::shared_ptr<arrow::Array> first_data_array;
    ARROW_ASSIGN_OR_RAISE(first_data_array, first_field_builder.Finish());

    arrow::UInt64Builder second_field_builder;
    ARROW_RETURN_NOT_OK(first_field_builder.Append(12));
    ARROW_RETURN_NOT_OK(first_field_builder.Append(22));
    std::shared_ptr<arrow::Array> second_data_array;
    ARROW_ASSIGN_OR_RAISE(second_data_array, second_field_builder.Finish());

    std::shared_ptr<arrow::Field> first_field = arrow::field("First", arrow::timestamp(arrow::TimeUnit::MICRO));
    std::shared_ptr<arrow::Field> second_field = arrow::field("Second", arrow::uint64());
    std::shared_ptr<arrow::Schema> test_schema = arrow::schema({first_field, second_field});
    std::shared_ptr<arrow::RecordBatch> test_rbatch = arrow::RecordBatch::Make(test_schema, 2, {first_data_array, second_data_array});


    std::cout << *test_schema << std::endl;

    arrow::Schema schema_des = *test_rbatch->schema();
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

    return arrow::Status::OK();
}

// Prerequisites:
// Install arrow using package manager or build from source.
// `sudo apt install -y -V libarrow-dev`
//
// To run execute:
// `cmake . && make arrow_gorilla_test && ./arrow_gorilla_test`
int main() {
    data_vec d_vec {
        std::make_pair(69, 6969)
    };
    column_data c_data = { d_vec, "My_column" };
    uint64_t header = 42;

    Status serialization_st = compress_column(header, c_data);
    Status deserialization_st = decompress_column();
    if (!serialization_st.ok() || !deserialization_st.ok()) {
        std::cerr << serialization_st << std::endl;
        exit(1);
    }
}