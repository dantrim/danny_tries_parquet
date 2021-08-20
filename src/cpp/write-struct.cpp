#include <iostream>
#include <string>
#include <memory>
#include <stdint.h>
#include <map>
#include <cmath>

#include "json.hpp"

// arrow/parquet
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include "arrow/util/checked_cast.h"
#include <arrow/type.h> // struct_
#include <arrow/ipc/json_simple.h>

// just messing around with schema with arbitrary depths and getting
// used to the API

#define check_result(expression) \
    if (!expression.ok()) \
       throw std::logic_error(#expression);

std::shared_ptr<arrow::Table> generate_table() {
    using nlohmann::json;

    // lets make the following struct
    // {
    //    "node" : {
    //                "myIntVal" : int,
    //                "myFloatVal" : float
    //              }
    //


    auto pool = arrow::default_memory_pool();
    auto int_builder = std::make_shared<arrow::Int32Builder>(pool);
    auto float_builder = std::make_shared<arrow::FloatBuilder>(pool);
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders{int_builder, float_builder};
    std::vector fields{arrow::field("myIntVal", arrow::int32()), arrow::field("myFloatVal", arrow::float32())};
    std::shared_ptr<arrow::DataType> struct_type = arrow::struct_(fields);

    auto struct_builder = std::make_shared<arrow::StructBuilder>(struct_type, pool, std::move(field_builders));

    // add some data
    check_result(struct_builder->Append());
    check_result(int_builder->Append(42));
    check_result(float_builder->Append(99.2));
    std::shared_ptr<arrow::Array> array;
    check_result(struct_builder->Finish(&array));


    std::shared_ptr<arrow::Schema> schema;
    schema = arrow::schema({arrow::field("myStruct", struct_type)});

	return arrow::Table::Make(schema, {array});

}

std::shared_ptr<arrow::Table> generate_table2() {
    using nlohmann::json;

    // lets make the following struct
    // {
    //    "myIntVal0" : int,
    //    "myFloatVal0" : float,
    //    "myStruct1" : {
    //                "myIntVal1" : int,
    //                "myFloatVal1" : float,
    //                "myStruct2" : {
    //                  "myIntVal2" : int,
    //                  "myFloatVal2" : float,
    //                  "myList2" : list[int]
    //                }
    //              }
    //


    auto pool = arrow::default_memory_pool();


    // create the inner struct
    auto builder_int2 = std::make_shared<arrow::Int32Builder>(pool);
    auto builder_float2 = std::make_shared<arrow::FloatBuilder>(pool);
    auto builder_listInt2 = std::make_shared<arrow::Int32Builder>(pool);
    auto builder_list2 = std::make_shared<arrow::ListBuilder>(pool, builder_listInt2);
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders2{builder_int2, builder_float2, builder_list2};
    std::vector fields2{arrow::field("myIntVal2", arrow::int32()),
        arrow::field("myFloatVal2", arrow::float32()),
        arrow::field("myList2", arrow::list(arrow::int32()))
    };
    std::shared_ptr<arrow::DataType> type_struct2 = arrow::struct_(fields2);
    auto builder_struct2 = std::make_shared<arrow::StructBuilder>(type_struct2, pool, std::move(builders2));

    // create the outer struct
    auto builder_int1 = std::make_shared<arrow::Int32Builder>(pool);
    auto builder_float1 = std::make_shared<arrow::FloatBuilder>(pool);
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders1{builder_int1, builder_float1, builder_struct2};
    std::vector fields1{arrow::field("myIntVal", arrow::int32()),
                        arrow::field("myFloatVal", arrow::float32()),
                        arrow::field("myStruct2", type_struct2)
    };
    std::shared_ptr<arrow::DataType> type_struct1 = arrow::struct_(fields1);

    auto builder_struct1 = std::make_shared<arrow::StructBuilder>(type_struct1, pool, std::move(builders1));

    // create the top level flat fields
    auto builder_int0 = std::make_shared<arrow::Int32Builder>(pool);
    auto builder_float0 = std::make_shared<arrow::FloatBuilder>(pool);
    auto field_int0 = arrow::field("myInt0", arrow::int32());
    auto field_float0 = arrow::field("myFloat0", arrow::float32());


    // add data
    check_result(builder_int0->Append(0));
    check_result(builder_float0->Append(0.5));

    check_result(builder_struct1->Append());
    check_result(builder_int1->Append(1));
    check_result(builder_float1->Append(1.5));

    check_result(builder_struct2->Append());
    check_result(builder_int2->Append(2));
    check_result(builder_float2->Append(2.5));
    check_result(builder_list2->Append());
    check_result(builder_listInt2->AppendValues({3,4,5}));


    std::shared_ptr<arrow::Array> struct_array;
    check_result(builder_struct1->Finish(&struct_array));

    std::shared_ptr<arrow::Array> int0_array;
    std::shared_ptr<arrow::Array> float0_array;
    check_result(builder_int0->Finish(&int0_array));
    check_result(builder_float0->Finish(&float0_array));

    std::shared_ptr<arrow::Schema> schema;
    schema = arrow::schema({arrow::field("myInt0", arrow::int32()),
            arrow::field("myFloat0", arrow::float32()),
            arrow::field("myStruct1", type_struct1)
    });
    std::cout << "FOO schema: " << std::endl;
    std::cout << schema->ToString() << std::endl;

	return arrow::Table::Make(schema, {int0_array, float0_array, struct_array});

}

void write_parquet_file(const arrow::Table& table, std::string outname) {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile,
      arrow::io::FileOutputStream::Open(outname));
  auto writer_props = parquet::WriterProperties::Builder().compression(arrow::Compression::SNAPPY)->build();
  auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(table,
                            arrow::default_memory_pool(),
                            outfile,
                            4,
                            writer_props,
                            arrow_props
      )
  );

}

int main(int argc, char* argv[]) {
    std::shared_ptr<arrow::Table> table = generate_table();
    write_parquet_file(*table, "struct1.parquet");

    std::shared_ptr<arrow::Table> table2 = generate_table2();
    write_parquet_file(*table2, "struct2.parquet");
    return 0;
}
