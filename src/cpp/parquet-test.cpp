#include <iostream>
#include <string>
#include <memory>
#include <stdint.h>


// arrow/parquet
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include "arrow/util/checked_cast.h"

template <typename BuilderType, typename T>
void AppendValues(BuilderType* builder, const std::vector<T>& values,
                  const std::vector<bool>& is_valid) {
  for (size_t i = 0; i < values.size(); ++i) {
    if (is_valid.size() == 0 || is_valid[i]) {
      PARQUET_THROW_NOT_OK(builder->Append(values[i]));
    } else {
      PARQUET_THROW_NOT_OK(builder->AppendNull());
    }
  }
  return;
}

template <typename ValueType, typename T>
void AppendList(arrow::ListBuilder* builder, const std::vector<std::vector<T>>& values,
                const std::vector<bool>& is_valid) {
  auto values_builder = arrow::internal::checked_cast<ValueType*>(builder->value_builder());

  for (size_t i = 0; i < values.size(); ++i) {
    if (is_valid.size() == 0 || is_valid[i]) {
      PARQUET_THROW_NOT_OK(builder->Append());
      AppendValues<ValueType, T>(values_builder, values[i], {});
    } else {
      PARQUET_THROW_NOT_OK(builder->AppendNull());
    }
  }
}

std::shared_ptr<arrow::Table> generate_table() {

//  //
//  // int field
//  //
//  arrow::Int64Builder i64builder;
//  //PARQUET_THROW_NOT_OK(i64builder.AppendValues({1, 2, 3, 4, 5}));
//  PARQUET_THROW_NOT_OK(i64builder.AppendValues({1, 2, 3, 4, 5, 7,99,44}));
//  std::shared_ptr<arrow::Array> i64array;
//  PARQUET_THROW_NOT_OK(i64builder.Finish(&i64array));
//
//  //
//  // string field
//  //
//  arrow::StringBuilder strbuilder;
//  PARQUET_THROW_NOT_OK(strbuilder.AppendValues({"foo", "bar", "baz", "butts", "tuff", "", "", ""}));//("some"));
//  std::shared_ptr<arrow::Array> strarray;
//  PARQUET_THROW_NOT_OK(strbuilder.Finish(&strarray));
//
//  //
//  // list field
//  //
//  arrow::NumericBuilder<arrow::FloatType> float_builder;
//  PARQUET_THROW_NOT_OK(float_builder.AppendValues({1.3, 2.3, 3.4, 2.2}));
//
//  arrow::ListBuilder list_builder(arrow::default_memory_pool(), &float_builder);
//  //auto list_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), &float_builder);
//
//
//  std::shared_ptr<arrow::Schema> schema = arrow::schema(
//      {arrow::field("MyInt", arrow::int64()), arrow::field("MyStr", arrow::utf8())});
//
//  return arrow::Table::Make(schema, {i64array, strarray});

	auto f0 = arrow::field("f0", arrow::int32());
	auto f1 = arrow::field("f1", arrow::float32());
	auto f2 = arrow::field("f2", arrow::list(arrow::int8()));
	auto schema = arrow::schema({f0, f1, f2});


	// setup some values
	std::vector<bool> is_valid = {true, true, true, true};
	std::vector<int32_t> f0_values{0,1,2,3};
	std::vector<float> f1_values{0.5, 1.5, 2.5, 3.5};
	std::vector<std::vector<int8_t>> f2_values{{}, {0,1,2}, {}, {99, 100, 108, 110}};

	auto AppendData = [&](arrow::Int32Builder* b0, arrow::FloatBuilder* b1, arrow::ListBuilder* b2) -> void {
		AppendValues<arrow::Int32Builder, int32_t>(b0, f0_values, is_valid);
		AppendValues<arrow::FloatBuilder, float>(b1, f1_values, is_valid);
		AppendList<arrow::Int8Builder, int8_t>(b2, f2_values, is_valid);
		return;
	};

	arrow::Int32Builder ex_b0;
	arrow::FloatBuilder ex_b1;
	auto pool = arrow::default_memory_pool();
	arrow::ListBuilder ex_b2(pool, std::unique_ptr<arrow::Int8Builder>(new arrow::Int8Builder(pool)));
	//auto f2_builder = std::make_shared<arrow::Int8Builder>(arrow::default_memory_pool());
	//arrow::ListBuilder ex_b2(arrow::default_memory_pool(), f2_builder);


	AppendData(&ex_b0, &ex_b1, &ex_b2);
	//for(size_t i = 0; i < f2_values.size(); ++i) {
	//	PARQUET_THROW_NOT_OK(f2_builder->AppendValues(f2_values[i]));
	//}


	std::shared_ptr<arrow::Array> a0, a1, a2;
	PARQUET_THROW_NOT_OK(ex_b0.Finish(&a0));
	PARQUET_THROW_NOT_OK(ex_b1.Finish(&a1));
	PARQUET_THROW_NOT_OK(ex_b2.Finish(&a2));
//  std::shared_ptr<arrow::Schema> schema = arrow::schema(
//      {arrow::field("MyInt", arrow::int64()), arrow::field("MyStr", arrow::utf8())});
//  return arrow::Table::Make(schema, {i64array, strarray});

	return arrow::Table::Make(schema, {a0, a1, a2});

}

void write_parquet_file(const arrow::Table& table) {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile,
      //arrow::io::FileOutputStream::Open("parquet-arrow-example.parquet"));
      arrow::io::FileOutputStream::Open("sample1.parquet"));
  // The last argument to the function call is the size of the RowGroup in
  // the parquet file. Normally you would choose this to be rather large but
  // for the example, we use a small value to have multiple RowGroups.
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, 3));

}

int main(int argc, char* argv[]) {
    std::cout << "hello world" << std::endl;

    std::shared_ptr<arrow::Table> table = generate_table();
    write_parquet_file(*table);
    return 0;
}
