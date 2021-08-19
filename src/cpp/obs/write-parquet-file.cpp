//std/stl
#include <string>
#include <memory>
#include <stdint.h>
#include <map>

// arrow/parquet
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

std::shared_ptr<arrow::Table> generate_table() {
  arrow::Int64Builder i64builder;
  PARQUET_THROW_NOT_OK(i64builder.AppendValues({1, 2, 3, 4, 5}));
  std::shared_ptr<arrow::Array> i64array;
  PARQUET_THROW_NOT_OK(i64builder.Finish(&i64array));

  // generate some key-value pairs to store as metadata
  std::unordered_map<std::string, std::string> keyvals;
  keyvals["foo"] = "bar";
  keyvals["bar"] = "foo";
  arrow::KeyValueMetadata metadata(keyvals);
  auto field = arrow::field("MyInt", arrow::int64(), false, metadata.Copy());
  std::shared_ptr<arrow::Schema> schema = arrow::schema({field});
  return arrow::Table::Make(schema, {i64array});
}

void write_parquet_file(const arrow::Table& table) {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile,
      arrow::io::FileOutputStream::Open("test.parquet"));
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, 3));
}

int main(int argc, char* argv[]) {
    std::shared_ptr<arrow::Table> table = generate_table();
    write_parquet_file(*table);
    return 0;
}
