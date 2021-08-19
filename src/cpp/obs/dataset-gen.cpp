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

size_t MAX_SIZE = 10;

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


class DatasetGenerator {

    public :

        DatasetGenerator() :
            _n_rows_in_group(-1),
            _outdir("dummy_dataset_flat/"),
            _dataset_name("dataset"),
            _event_count(0),
            _file_count(0)
        {
        }

        void init() {
            std::string internalpath;   
            auto fs = arrow::fs::FileSystemFromUriOrPath(std::filesystem::absolute(_outdir), &internalpath).ValueOrDie();
            //if(_file_count == 0) {
                std::cout << "DatasetGenerator::init    output dir   = " << _outdir << std::endl;
                std::cout << "DatasetGenerator::init    internalpath = " << internalpath << std::endl;
                PARQUET_THROW_NOT_OK(fs->CreateDir(internalpath));
            //i}
            //else {
            //    PARQUET_THROW_NOT_OK(_writer->Close());
            //}
            auto internal_fs = std::make_shared<arrow::fs::SubTreeFileSystem>(internalpath, fs);

            //
            // initialize the writer
            //
            std::stringstream outfilename;
            outfilename << _dataset_name << "_" << _file_count << ".parquet";
            PARQUET_ASSIGN_OR_THROW(
                    outfile,
                    internal_fs->OpenOutputStream(outfilename.str())
            );
            _file_count++;

            //create_fields_flat(outfile);
            create_fields(outfile);

        }

        void create_fields(std::shared_ptr<arrow::io::OutputStream> outfile) {
            using nlohmann::json;

            auto pool = arrow::default_memory_pool();
            b_lepton_pt = new arrow::ListBuilder(pool, std::unique_ptr<arrow::FloatBuilder>(new arrow::FloatBuilder(pool)));
            b_lepton_eta = new arrow::ListBuilder(pool, std::unique_ptr<arrow::FloatBuilder>(new arrow::FloatBuilder(pool)));
            b_lepton_phi = new arrow::ListBuilder(pool, std::unique_ptr<arrow::FloatBuilder>(new arrow::FloatBuilder(pool)));
            b_lepton_mask = new arrow::ListBuilder(pool, std::unique_ptr<arrow::BooleanBuilder>(new arrow::BooleanBuilder(pool)));

            b_jet_pt = new arrow::ListBuilder(pool, std::unique_ptr<arrow::FloatBuilder>(new arrow::FloatBuilder(pool)));
            b_jet_eta = new arrow::ListBuilder(pool, std::unique_ptr<arrow::FloatBuilder>(new arrow::FloatBuilder(pool)));
            b_jet_phi = new arrow::ListBuilder(pool, std::unique_ptr<arrow::FloatBuilder>(new arrow::FloatBuilder(pool)));
            b_jet_m = new arrow::ListBuilder(pool, std::unique_ptr<arrow::FloatBuilder>(new arrow::FloatBuilder(pool)));
            b_jet_mask = new arrow::ListBuilder(pool, std::unique_ptr<arrow::BooleanBuilder>(new arrow::BooleanBuilder(pool)));


            _fields.clear();
            _fields.push_back(arrow::field("jet_pt", arrow::list(arrow::float32())));
            _fields.push_back(arrow::field("jet_eta", arrow::list(arrow::float32())));
            _fields.push_back(arrow::field("jet_phi", arrow::list(arrow::float32())));
            _fields.push_back(arrow::field("jet_m", arrow::list(arrow::float32())));
            _fields.push_back(arrow::field("jet_mask", arrow::list(arrow::boolean())));
            _fields.push_back(arrow::field("jet_n", arrow::uint16()));
            _fields.push_back(arrow::field("lep_pt", arrow::list(arrow::float32())));
            _fields.push_back(arrow::field("lep_eta", arrow::list(arrow::float32())));
            _fields.push_back(arrow::field("lep_phi", arrow::list(arrow::float32())));
            _fields.push_back(arrow::field("lep_mask", arrow::list(arrow::boolean())));
            _fields.push_back(arrow::field("lep_n", arrow::uint16()));
            _fields.push_back(arrow::field("event_id", arrow::uint32()));
            _fields.push_back(arrow::field("event_w", arrow::float64()));

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
                _n_rows_in_group = 250000 / _fields.size(); // (32*1024*1024/ 10*_fields.size()) / 10;
            } 

            //
            // create the properties for the output file
            //
            auto writer_props = parquet::WriterProperties::Builder()
                .compression(arrow::Compression::UNCOMPRESSED)
                ->data_pagesize(1024*1024 * 512)
                //->max_row_group_length(1024)
                //->write_batch_size(1024)
                ->build();
            auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
            PARQUET_THROW_NOT_OK(parquet::arrow::FileWriter::Open(*_schema,
                            arrow::default_memory_pool(),
                            outfile,
                            writer_props,
                            arrow_props,
                            &_writer
                        ));
        }

        void create_fields_flat(std::shared_ptr<arrow::io::OutputStream> outfile) {
            using nlohmann::json;

            _fields.clear();
            _fields.push_back(arrow::field("jet0_pt", arrow::float32()));
            _fields.push_back(arrow::field("jet0_eta", arrow::float32()));
            _fields.push_back(arrow::field("jet0_phi", arrow::float32()));
            _fields.push_back(arrow::field("jet0_m", arrow::float32()));
            _fields.push_back(arrow::field("jet1_pt", arrow::float32()));
            _fields.push_back(arrow::field("jet1_eta", arrow::float32()));
            _fields.push_back(arrow::field("jet1_phi", arrow::float32()));
            _fields.push_back(arrow::field("jet1_m", arrow::float32()));
            _fields.push_back(arrow::field("jet2_pt", arrow::float32()));
            _fields.push_back(arrow::field("jet2_eta", arrow::float32()));
            _fields.push_back(arrow::field("jet2_phi", arrow::float32()));
            _fields.push_back(arrow::field("jet2_m", arrow::float32()));
            _fields.push_back(arrow::field("jet3_pt", arrow::float32()));
            _fields.push_back(arrow::field("jet3_eta", arrow::float32()));
            _fields.push_back(arrow::field("jet3_phi", arrow::float32()));
            _fields.push_back(arrow::field("jet3_m", arrow::float32()));

            _fields.push_back(arrow::field("lep0_pt", arrow::float32()));
            _fields.push_back(arrow::field("lep0_eta", arrow::float32()));
            _fields.push_back(arrow::field("lep0_phi", arrow::float32()));
            _fields.push_back(arrow::field("lep1_pt", arrow::float32()));
            _fields.push_back(arrow::field("lep1_eta", arrow::float32()));
            _fields.push_back(arrow::field("lep1_phi", arrow::float32()));

            _fields.push_back(arrow::field("event_id", arrow::uint32()));
            _fields.push_back(arrow::field("event_w", arrow::float64()));

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
            auto writer_props = parquet::WriterProperties::Builder()
                .compression(arrow::Compression::UNCOMPRESSED)
                //->data_pagesize(1024*1024 * 512)
                ->build();
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
            //if(_event_count > 0 && _event_count % 10000 == 0) {
            //    init();
            //}

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
            uint16_t n_remaining = MAX_SIZE - n_leptons;
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
            n_remaining = MAX_SIZE - n_jets;
            for(size_t i = 0; i <  n_remaining; i++) jet_mask.push_back(false);

            double event_weight = weight_dis(rng);

            //
            // fill
            //

            std::vector<bool> is_valid{true};

            std::vector<std::vector<float>> foo;
            std::vector<uint16_t> foo_n;
            std::vector<std::vector<bool>> foo_bool;

            foo.push_back(lepton_pt);
            AppendList<arrow::FloatBuilder, float>(b_lepton_pt, foo, is_valid); foo.clear();
            foo.push_back(lepton_eta);
            AppendList<arrow::FloatBuilder, float>(b_lepton_eta, foo, is_valid); foo.clear();
            foo.push_back(lepton_phi);
            AppendList<arrow::FloatBuilder, float>(b_lepton_phi, foo, is_valid); foo.clear();
            foo_bool.push_back(lepton_mask);
            AppendList<arrow::BooleanBuilder, bool>(b_lepton_mask, foo_bool, is_valid); foo_bool.clear();
            foo_n.push_back(n_leptons);
            AppendValues<arrow::UInt16Builder, uint16_t>(&b_lepton_n, foo_n, is_valid); foo_n.clear();

            foo.push_back(jet_pt);
            AppendList<arrow::FloatBuilder, float>(b_jet_pt, foo, is_valid); foo.clear();
            foo.push_back(jet_eta);
            AppendList<arrow::FloatBuilder, float>(b_jet_eta, foo, is_valid); foo.clear();
            foo.push_back(jet_phi);
            AppendList<arrow::FloatBuilder, float>(b_jet_phi, foo, is_valid); foo.clear();
            foo.push_back(jet_m);
            AppendList<arrow::FloatBuilder, float>(b_jet_m, foo, is_valid); foo.clear();
            foo_bool.push_back(jet_mask);
            AppendList<arrow::BooleanBuilder, bool>(b_jet_mask, foo_bool, is_valid); foo_bool.clear();
            foo_n.push_back(n_jets);
            AppendValues<arrow::UInt16Builder, uint16_t>(&b_jet_n, foo_n, is_valid); foo_n.clear();


            std::vector<double> foo_double;
            foo_double.push_back(event_weight);
            AppendValues<arrow::DoubleBuilder, double>(&b_event_w, foo_double, is_valid); foo_double.clear();

            std::vector<uint32_t> foo_count;
            foo_count.push_back(_event_count);
            AppendValues<arrow::UInt32Builder, uint32_t>(&b_event_id, foo_count, is_valid);


            _event_count++;

            if(_event_count % _n_rows_in_group == 0) {
            //if(_event_count % 10000 == 0) {
                write();
            }

        }

        void write() {

            auto length = b_jet_pt->length();

            //
            // now flush
            //
            std::shared_ptr<arrow::Array> array;

            PARQUET_THROW_NOT_OK(b_jet_pt->Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet_eta->Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet_phi->Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet_m->Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet_mask->Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet_n.Finish(&array)); _arrays.push_back(array);


            PARQUET_THROW_NOT_OK(b_lepton_pt->Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_lepton_eta->Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_lepton_phi->Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_lepton_mask->Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_lepton_n.Finish(&array)); _arrays.push_back(array);

            PARQUET_THROW_NOT_OK(b_event_id.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_event_w.Finish(&array)); _arrays.push_back(array);
            

            std::shared_ptr<arrow::Table> table = arrow::Table::Make(_schema, _arrays);
            PARQUET_THROW_NOT_OK(_writer->WriteTable(*table, length));

            PARQUET_THROW_NOT_OK(outfile->Flush());
            _arrays.clear();

        }


        void generate_event_flat() {
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

            double event_weight = weight_dis(rng);

            //
            // fill
            //

            PARQUET_THROW_NOT_OK(b_jet0_pt.Append(jet_pt.at(0)));
            PARQUET_THROW_NOT_OK(b_jet0_eta.Append(jet_eta.at(0)));
            PARQUET_THROW_NOT_OK(b_jet0_phi.Append(jet_phi.at(0)));
            PARQUET_THROW_NOT_OK(b_jet0_m.Append(jet_m.at(0)));
            PARQUET_THROW_NOT_OK(b_jet1_pt.Append(jet_pt.at(1)));
            PARQUET_THROW_NOT_OK(b_jet1_eta.Append(jet_eta.at(1)));
            PARQUET_THROW_NOT_OK(b_jet1_phi.Append(jet_phi.at(1)));
            PARQUET_THROW_NOT_OK(b_jet1_m.Append(jet_m.at(1)));
            PARQUET_THROW_NOT_OK(b_jet2_pt.Append(jet_pt.at(2)));
            PARQUET_THROW_NOT_OK(b_jet2_eta.Append(jet_eta.at(2)));
            PARQUET_THROW_NOT_OK(b_jet2_phi.Append(jet_phi.at(2)));
            PARQUET_THROW_NOT_OK(b_jet2_m.Append(jet_m.at(2)));
            PARQUET_THROW_NOT_OK(b_jet3_pt.Append(jet_pt.at(3)));
            PARQUET_THROW_NOT_OK(b_jet3_eta.Append(jet_eta.at(3)));
            PARQUET_THROW_NOT_OK(b_jet3_phi.Append(jet_phi.at(3)));
            PARQUET_THROW_NOT_OK(b_jet3_m.Append(jet_m.at(3)));
            PARQUET_THROW_NOT_OK(b_lep0_pt.Append(lepton_pt.at(0)));
            PARQUET_THROW_NOT_OK(b_lep0_eta.Append(lepton_eta.at(0)));
            PARQUET_THROW_NOT_OK(b_lep0_phi.Append(lepton_phi.at(0)));
            PARQUET_THROW_NOT_OK(b_lep1_pt.Append(lepton_pt.at(1)));
            PARQUET_THROW_NOT_OK(b_lep1_eta.Append(lepton_eta.at(1)));
            PARQUET_THROW_NOT_OK(b_lep1_phi.Append(lepton_phi.at(1)));
            PARQUET_THROW_NOT_OK(b_event_id.Append(_event_count));
            PARQUET_THROW_NOT_OK(b_event_w.Append(event_weight));

            _event_count++;

            if(_event_count % _n_rows_in_group == 0) {
                write_flat();
            }

        }

        void write_flat() {
            auto length = b_jet0_pt.length();
            //
            // now flush
            //
            _arrays.clear();
            std::shared_ptr<arrow::Array> array;

            PARQUET_THROW_NOT_OK(b_jet0_pt.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet0_eta.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet0_phi.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet0_m.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet1_pt.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet1_eta.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet1_phi.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet1_m.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet2_pt.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet2_eta.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet2_phi.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet2_m.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet3_pt.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet3_eta.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet3_phi.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_jet3_m.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_lep0_pt.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_lep0_eta.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_lep0_phi.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_lep1_pt.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_lep1_eta.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_lep1_phi.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_event_id.Finish(&array)); _arrays.push_back(array);
            PARQUET_THROW_NOT_OK(b_event_w.Finish(&array)); _arrays.push_back(array);

            std::shared_ptr<arrow::Table> table = arrow::Table::Make(_schema, _arrays);
            PARQUET_THROW_NOT_OK(_writer->WriteTable(*table, length));//512 * 1024 * 1024));//1024 * 1024 * 512));

            PARQUET_THROW_NOT_OK(outfile->Flush());
            _arrays.clear();
        }

        void finish() {
            write();
            //write_flat();
            PARQUET_THROW_NOT_OK(_writer->Close());
        }


    private :
        std::unique_ptr<parquet::arrow::FileWriter> _writer;
        int32_t _n_rows_in_group;
        std::string _outdir;
        std::string _dataset_name;
        uint32_t _event_count;
        uint32_t _file_count;

        std::shared_ptr<arrow::Schema> _schema;
        std::vector<std::shared_ptr<arrow::Field>> _fields;
        std::vector<std::shared_ptr<arrow::Array>> _arrays;
        std::default_random_engine rng;

        arrow::FloatBuilder b_jet0_pt;
        arrow::FloatBuilder b_jet0_eta;
        arrow::FloatBuilder b_jet0_phi;
        arrow::FloatBuilder b_jet0_m;
        arrow::FloatBuilder b_jet1_pt;
        arrow::FloatBuilder b_jet1_eta;
        arrow::FloatBuilder b_jet1_phi;
        arrow::FloatBuilder b_jet1_m;
        arrow::FloatBuilder b_jet2_pt;
        arrow::FloatBuilder b_jet2_eta;
        arrow::FloatBuilder b_jet2_phi;
        arrow::FloatBuilder b_jet2_m;
        arrow::FloatBuilder b_jet3_pt;
        arrow::FloatBuilder b_jet3_eta;
        arrow::FloatBuilder b_jet3_phi;
        arrow::FloatBuilder b_jet3_m;
        arrow::FloatBuilder b_lep0_pt;
        arrow::FloatBuilder b_lep0_eta;
        arrow::FloatBuilder b_lep0_phi;
        arrow::FloatBuilder b_lep1_pt;
        arrow::FloatBuilder b_lep1_eta;
        arrow::FloatBuilder b_lep1_phi;

        arrow::ListBuilder* b_lepton_pt;
        arrow::ListBuilder* b_lepton_eta;
        arrow::ListBuilder* b_lepton_phi;
        arrow::ListBuilder* b_lepton_mask;
        arrow::UInt16Builder b_lepton_n;
        arrow::ListBuilder* b_jet_pt;
        arrow::ListBuilder* b_jet_eta;
        arrow::ListBuilder* b_jet_phi;
        arrow::ListBuilder* b_jet_m;
        arrow::ListBuilder* b_jet_mask;
        arrow::UInt16Builder b_jet_n;

        arrow::DoubleBuilder b_event_w;
        arrow::UInt32Builder b_event_id;

        std::shared_ptr<arrow::io::OutputStream> outfile;

};

int main(int argc, char* argv[]) {

    DatasetGenerator dsgen;
    dsgen.init();
    for(size_t i = 0; i < 5000000; i++) {
        if(i%10000 == 0) {
            std::cout << "generating event " << i << std::endl;
        }
        //dsgen.generate_event_flat();
        dsgen.generate_event();
    }
    dsgen.finish();
    return 0;
}
