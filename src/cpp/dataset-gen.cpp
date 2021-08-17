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
    size_t n_remaining = 1000 - value_vec.size();
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


class DatasetGenerator {

    public :

        DatasetGenerator() :
            _n_rows_in_group(-1),
            _outdir("dummy_dataset/"),
            _dataset_name("dataset.parquet"),
            _event_count(0)
        {
        }

        void init() {
            std::string internalpath;   
            auto fs = arrow::fs::FileSystemFromUriOrPath(std::filesystem::absolute(_outdir), &internalpath).ValueOrDie();
            std::cout << "DatasetGenerator::init    output dir   = " << _outdir << std::endl;
            std::cout << "DatasetGenerator::init    internalpath = " << internalpath << std::endl;
            PARQUET_THROW_NOT_OK(fs->CreateDir(internalpath));
            auto internal_fs = std::make_shared<arrow::fs::SubTreeFileSystem>(internalpath, fs);

            //
            // initialize the writer
            //
            std::shared_ptr<arrow::io::OutputStream> outfile;
            PARQUET_ASSIGN_OR_THROW(
                    outfile,
                    internal_fs->OpenOutputStream(_dataset_name)
            );

            create_fields(outfile);

        }

        void create_fields(std::shared_ptr<arrow::io::OutputStream> outfile) {
            using nlohmann::json;

            _fields.clear();
            auto f0 = arrow::field("jet_pt", arrow::list(arrow::float32())); _fields.push_back(f0);
            auto f1 = arrow::field("jet_eta", arrow::list(arrow::float32())); _fields.push_back(f1);
            auto f2 = arrow::field("jet_phi", arrow::list(arrow::float32())); _fields.push_back(f2);
            auto f3 = arrow::field("jet_m", arrow::list(arrow::float32())); _fields.push_back(f3);
            auto f4 = arrow::field("jet_mask", arrow::list(arrow::boolean())); _fields.push_back(f4);
            auto f5 = arrow::field("jet_n", arrow::uint16()); _fields.push_back(f5);

            auto f6 = arrow::field("event_id", arrow::uint32()); _fields.push_back(f6);
            auto f7 = arrow::field("event_w", arrow::float64()); _fields.push_back(f7);

            auto f8 = arrow::field("lep_pt", arrow::list(arrow::float32())); _fields.push_back(f8);
            auto f9 = arrow::field("lep_eta", arrow::list(arrow::float32())); _fields.push_back(f9);
            auto f10 = arrow::field("lep_phi", arrow::list(arrow::float32())); _fields.push_back(f10);
            auto f11 = arrow::field("lep_mask", arrow::list(arrow::boolean())); _fields.push_back(f11);
            auto f12 = arrow::field("lep_n", arrow::uint16()); _fields.push_back(f12);


            //
            // create the schema and metadata
            //
            json j_schema_metadata;
            j_schema_metadata["dsid"] = 410472;
            j_schema_metadata["campaign"] = "mc16d";
            j_schema_metadata["sample_name"] = "foobar.410472.ttbar";
            j_schema_metadata["tag"] = "v1.0.3";
            j_schema_metadata["creation_date"] = "2021-08-16";
            std::string s_schema_metadata = j_schema_metadata.dump();
            std::unordered_map<std::string, std::string> schema_metadata;
            schema_metadata["metadata"] = s_schema_metadata;
            arrow::KeyValueMetadata keyval_metadata(schema_metadata);

            _schema = arrow::schema(_fields);
            _schema = _schema->WithMetadata(keyval_metadata.Copy());

            if(_n_rows_in_group < 0) {
                _n_rows_in_group = 32*1024*1024/_fields.size();
            } 

            //
            // create the properties for the output file
            //
            auto writer_props = parquet::WriterProperties::Builder().compression(arrow::Compression::SNAPPY)->build();
            auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
            PARQUET_THROW_NOT_OK(parquet::arrow::FileWriter::Open(*_schema,
                            arrow::default_memory_pool(),
                            outfile,
                            writer_props,
                            arrow_props,
                            &_writer
                        ));
        }

        void generate_event() {
            std::uniform_real_distribution<int> n_lep_dis(0.0, 2.0);
            std::uniform_real_distribution<int> n_jet_dis(0.0, 10.0);
            std::uniform_real_distribution<float> pt_dis(0, 100.);
            std::uniform_real_distribution<float> eta_dis(-2.7, 2.7);
            std::uniform_real_distribution<float> phi_dis(-3.14, 3.14);
            std::normal_distribution<double> weight_dis(1.0, 0.03);


            uint16_t n_leptons = 2;//n_lep_dis(rng);
            uint16_t n_jets = 4;
            //int n_jets = n_jet_dis(rng);
            //n_leptons = 2;
            //n_jets = 4;

            std::vector<float> lepton_pt;
            std::vector<float> lepton_eta;
            std::vector<float> lepton_phi;
            std::vector<bool> lepton_mask;
            for(size_t i = 0; i < n_leptons; i++) {
                lepton_pt.push_back( pt_dis(rng) );
                lepton_eta.push_back( eta_dis(rng) );
                lepton_phi.push_back( phi_dis(rng) );
                lepton_mask.push_back(true);
            } // i
            uint16_t n_remaining = 1000 - n_leptons;
            for(size_t i = 0; i <  n_remaining; i++) lepton_mask.push_back(false);

            std::vector<float> jet_pt;
            std::vector<float> jet_eta;
            std::vector<float> jet_phi;
            std::vector<float> jet_m;
            std::vector<bool> jet_mask;
            for(size_t i = 0; i < n_jets; i++) {
                jet_pt.push_back( pt_dis(rng) );
                jet_eta.push_back( eta_dis(rng) );
                jet_phi.push_back( phi_dis(rng) );
                jet_m.push_back(pt_dis(rng) );
                jet_mask.push_back(true);
            } // i
            n_remaining = 1000 - n_jets;
            for(size_t i = 0; i <  n_remaining; i++) jet_mask.push_back(false);

            double event_weight = weight_dis(rng);

            //
            // fill
            //
            auto pool = arrow::default_memory_pool();
            arrow::ListBuilder b_lepton_pt(pool, std::unique_ptr<arrow::FloatBuilder>(new arrow::FloatBuilder(pool)));
            arrow::ListBuilder b_lepton_eta(pool, std::unique_ptr<arrow::FloatBuilder>(new arrow::FloatBuilder(pool)));
            arrow::ListBuilder b_lepton_phi(pool, std::unique_ptr<arrow::FloatBuilder>(new arrow::FloatBuilder(pool)));
            arrow::ListBuilder b_lepton_mask(pool, std::unique_ptr<arrow::BooleanBuilder>(new arrow::BooleanBuilder(pool)));
            arrow::UInt16Builder b_lepton_n;

            arrow::ListBuilder b_jet_pt(pool, std::unique_ptr<arrow::FloatBuilder>(new arrow::FloatBuilder(pool)));
            arrow::ListBuilder b_jet_eta(pool, std::unique_ptr<arrow::FloatBuilder>(new arrow::FloatBuilder(pool)));
            arrow::ListBuilder b_jet_phi(pool, std::unique_ptr<arrow::FloatBuilder>(new arrow::FloatBuilder(pool)));
            arrow::ListBuilder b_jet_m(pool, std::unique_ptr<arrow::FloatBuilder>(new arrow::FloatBuilder(pool)));
            arrow::ListBuilder b_jet_mask(pool, std::unique_ptr<arrow::BooleanBuilder>(new arrow::BooleanBuilder(pool)));
            arrow::UInt16Builder b_jet_n;

            arrow::DoubleBuilder b_event_w;
            arrow::UInt32Builder b_event_id;

            std::vector<bool> is_valid{true};

            std::vector<std::vector<float>> foo;
            std::vector<uint16_t> foo_n;
            std::vector<std::vector<bool>> foo_bool;

            foo.push_back(lepton_pt);
            AppendList<arrow::FloatBuilder, float>(&b_lepton_pt, foo, is_valid); foo.clear();
            foo.push_back(lepton_eta);
            AppendList<arrow::FloatBuilder, float>(&b_lepton_eta, foo, is_valid); foo.clear();
            foo.push_back(lepton_phi);
            AppendList<arrow::FloatBuilder, float>(&b_lepton_phi, foo, is_valid); foo.clear();
            foo_bool.push_back(lepton_mask);
            AppendList<arrow::BooleanBuilder, bool>(&b_lepton_mask, foo_bool, is_valid); foo_bool.clear();
            foo_n.push_back(n_leptons);
            AppendValues<arrow::UInt16Builder, uint16_t>(&b_lepton_n, foo_n, is_valid); foo_n.clear();

            foo.push_back(jet_pt);
            AppendList<arrow::FloatBuilder, float>(&b_jet_pt, foo, is_valid); foo.clear();
            foo.push_back(jet_eta);
            AppendList<arrow::FloatBuilder, float>(&b_jet_eta, foo, is_valid); foo.clear();
            foo.push_back(jet_phi);
            AppendList<arrow::FloatBuilder, float>(&b_jet_phi, foo, is_valid); foo.clear();
            foo.push_back(jet_m);
            AppendList<arrow::FloatBuilder, float>(&b_jet_m, foo, is_valid); foo.clear();
            foo_bool.push_back(jet_mask);
            AppendList<arrow::BooleanBuilder, bool>(&b_jet_mask, foo_bool, is_valid); foo_bool.clear();
            foo_n.push_back(n_jets);
            AppendValues<arrow::UInt16Builder, uint16_t>(&b_jet_n, foo_n, is_valid); foo_n.clear();


            std::vector<double> foo_double;
            foo_double.push_back(event_weight);
            AppendValues<arrow::DoubleBuilder, double>(&b_event_w, foo_double, is_valid); foo_double.clear();

            std::vector<uint32_t> foo_count;
            foo_count.push_back(_event_count);
            AppendValues<arrow::UInt32Builder, uint32_t>(&b_event_id, foo_count, is_valid);


            //
            // now flush
            //
            std::vector<std::shared_ptr<arrow::Array>> arrays = {};
            std::shared_ptr<arrow::Array> array;

            PARQUET_THROW_NOT_OK(b_jet_pt.Finish(&array));
            arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet_eta.Finish(&array));
            arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet_phi.Finish(&array));
            arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet_m.Finish(&array));
            arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet_mask.Finish(&array));
            arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet_n.Finish(&array));
            arrays.push_back(array);

            PARQUET_THROW_NOT_OK(b_event_id.Finish(&array));
            arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_event_w.Finish(&array));
            arrays.push_back(array);

            PARQUET_THROW_NOT_OK(b_lepton_pt.Finish(&array));
            arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_lepton_eta.Finish(&array));
            arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_lepton_phi.Finish(&array));
            arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_lepton_mask.Finish(&array));
            arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_lepton_n.Finish(&array));
            arrays.push_back(array);
            

            std::shared_ptr<arrow::Table> table = arrow::Table::Make(_schema, arrays);
            PARQUET_THROW_NOT_OK(_writer->WriteTable(*table, array->length()));

            _event_count++;

        }

        void finish() {
            PARQUET_THROW_NOT_OK(_writer->Close());
        }


    private :
        std::unique_ptr<parquet::arrow::FileWriter> _writer;
        int32_t _n_rows_in_group;
        std::string _outdir;
        std::string _dataset_name;
        uint32_t _event_count;

        std::shared_ptr<arrow::Schema> _schema;
        std::vector<std::shared_ptr<arrow::Field>> _fields;
        std::vector<std::shared_ptr<arrow::Array>> _arrays;
        std::default_random_engine rng;


};

std::shared_ptr<arrow::Table> generate_table() {
    using nlohmann::json;

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

    json f2_metadata;
    f2_metadata["units"] = "GeV";
    f2_metadata["nice-name"] = "$m_{T2}$";
    std::unordered_map<std::string, std::string> keyval;
    keyval["metadata"] = f2_metadata.dump();
    arrow::KeyValueMetadata metadata(keyval);
	auto f2 = arrow::field("f2", arrow::list(arrow::int8()), false, metadata.Copy());
    std::cout << "foo f2 = " << f2->ToString(true) << std::endl;
	auto schema = arrow::schema({f0, f1, f2});



    json dataset_metadata;
    dataset_metadata["dsid"] = 410472;
    dataset_metadata["campaign"] = "mc16d";
    dataset_metadata["sample_name"] = "foobar";
    json sample_metadata;
    sample_metadata["creation-command"] = "ntupler -s foobar -c mc16d -n 1";
    sample_metadata["tag"] = "n0207";
    sample_metadata["creation-date"] = "2021-08-16";

    json file_metadata;
    file_metadata["dataset"] = dataset_metadata;
    file_metadata["sample"] = sample_metadata;

    std::string sm = file_metadata.dump();

    std::unordered_map<std::string, std::string> sm_map;
    sm_map["metadata"] = sm;
    arrow::KeyValueMetadata sm_metadata(sm_map);
    schema = schema->WithMetadata(sm_metadata.Copy());


	// setup some values
	std::vector<bool> is_valid = {true, true, true, true};
	std::vector<int32_t> f0_values{0, 3, 0, 4};
	std::vector<float> f1_values{0.5, 1.5, 2.5, 3.5};
	std::vector<std::vector<int8_t>> f2_values{{}, {0,1,2}, {}, {99, 100, 108, 110}};

	auto AppendData = [&](arrow::Int32Builder* b0, arrow::FloatBuilder* b1, arrow::ListBuilder* b2) -> void {
		AppendValues<arrow::Int32Builder, int32_t>(b0, f0_values, is_valid);
		AppendValues<arrow::FloatBuilder, float>(b1, f1_values, is_valid);
		AppendList<arrow::Int8Builder, int8_t>(b2, f2_values, is_valid);
	};

	arrow::Int32Builder ex_b0;
	arrow::FloatBuilder ex_b1;
	auto pool = arrow::default_memory_pool();
	arrow::ListBuilder ex_b2(pool, std::unique_ptr<arrow::Int8Builder>(new arrow::Int8Builder(pool)));

	AppendData(&ex_b0, &ex_b1, &ex_b2);


	std::shared_ptr<arrow::Array> a0, a1, a2;
	PARQUET_THROW_NOT_OK(ex_b0.Finish(&a0));
	PARQUET_THROW_NOT_OK(ex_b1.Finish(&a1));
	PARQUET_THROW_NOT_OK(ex_b2.Finish(&a2));

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
  //auto writer_props = parquet::default_writer_properties();
  auto writer_props = parquet::WriterProperties::Builder().compression(arrow::Compression::UNCOMPRESSED)->build();
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

    DatasetGenerator dsgen;
    dsgen.init();
    for(size_t i = 0; i < 10000; i++) {
        if(i%100 == 0) {
            std::cout << "generating event " << i << std::endl;
        }
        dsgen.generate_event();
    }
    dsgen.finish();
    return 0;
}
