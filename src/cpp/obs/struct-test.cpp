#include <iostream>
#include <string>
#include <memory>
#include <stdint.h>
#include <map>
#include <cmath>
#include <filesystem>
#include <sstream>
#include <random>

#include "json.hpp"

// arrow/parquet
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include "arrow/util/checked_cast.h"
#include <arrow/filesystem/filesystem.h>
#include <arrow/type.h>
#include "arrow/ipc/json_simple.h"

size_t MAX_SIZE = 10;

std::shared_ptr<arrow::Array> ArrayFromJSON(const std::shared_ptr<arrow::DataType>& type, arrow::util::string_view json) {
    std::shared_ptr<arrow::Array> out;
    PARQUET_THROW_NOT_OK(arrow::ipc::internal::json::ArrayFromJSON(type, json, &out));
    return out;
}

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
void AppendList(arrow::ListBuilder* builder, std::vector<std::vector<T>>& values,
                std::vector<bool>& is_valid) {
  auto values_builder = arrow::internal::checked_cast<ValueType*>(builder->value_builder());

  for(auto& value_vec : values) {
    size_t n_remaining = MAX_SIZE - value_vec.size();
    for(size_t i = 0; i < n_remaining; i++) {
        value_vec.push_back(std::nan("1"));
        //value_vec.push_back(NAN);
    }
  }

  for (size_t i = 0; i < values.size(); ++i) {
    if (is_valid.size() == 0 || is_valid[i]) {
      PARQUET_THROW_NOT_OK(builder->Append());
      AppendValues<ValueType, T>(values_builder, values[i], {});
    } else {
      PARQUET_THROW_NOT_OK(builder->AppendNull());
    }
  }
}


int main(int argc, char* argv[]) {
    //auto arrow::struct_array = arrow::ArrayFromJSON(

    using nlohmann::json;

    size_t n_particles = 4;
    std::vector<float> pt;
    std::vector<float> eta;
    std::vector<float> phi;
    for(size_t i = 0; i < n_particles; i++) {
        pt.push_back(1.0 * i);
        eta.push_back(2.0 * i);
        phi.push_back(3.0 * i);
    }

    json particle_fields;
    std::vector<json> particles_array;
    for(size_t i = 0; i < n_particles; i++) {
        json p;
        p["pt"] = pt[i];
        p["eta"] = eta[i];
        p["phi"] = phi[i];
        particles_array.push_back(p);
    }
    //particle_fields["pt"] = pt;
    //particle_fields["eta"] = eta;
    //particle_fields["phi"] = phi;
    //std::vector<json> field_array;
    //field_array.push_back(particles_array);

    json particles;
    particles["particles"] = particles_array;

    //json event;
    //json event["event"] = particles;

    std::string jstr = particles.dump();
    jstr = "[" + jstr + "," + jstr + "]";
    std::cout << "jstr = " << jstr << std::endl;
    //std::string_literal rstr{jstr};


    auto field_type = arrow::struct_({arrow::field("particles",
            arrow::list(
                arrow::struct_({arrow::field("pt", arrow::float32()),
                                arrow::field("eta", arrow::float32()),
                                arrow::field("phi", arrow::float32())})))});
    auto event_struct_array = ArrayFromJSON(
            field_type, arrow::util::string_view(jstr));

    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.push_back(arrow::field("reco", field_type));
    std::shared_ptr<arrow::Schema> schema;
    schema = arrow::schema(fields);
    std::cout << "schema num_fields = " << schema->num_fields() << std::endl;
    std::cout << "schema to string : " << std::endl;
    std::cout << schema->ToString() << std::endl;

    auto table = arrow::Table::Make(schema, {event_struct_array});

    // setup the output file
    std::string output_name = "struct.parquet";
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(
            outfile,
            arrow::io::FileOutputStream::Open(output_name));
    PARQUET_THROW_NOT_OK(
                parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                    outfile,
                    1,
                    parquet::default_writer_properties(),
                    parquet::default_arrow_writer_properties()
            ));


}
