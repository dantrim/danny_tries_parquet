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
#include <arrow/type.h> // struct_
#include <arrow/ipc/json_simple.h> // ArrayFromJSON
using nlohmann::json;

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


class DatasetGenerator {

    public :

        DatasetGenerator() :
            _n_rows_in_group(-1),
            _outdir("dataset_struct/"),
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

            create_fields(outfile);

        }

        void create_fields(std::shared_ptr<arrow::io::OutputStream> outfile) {
            using nlohmann::json;

            _fields.clear();

            // leptons
            auto lepton_struct = arrow::struct_(
                        {arrow::field("pt", arrow::float32()),
                         arrow::field("eta", arrow::float32()),
                         arrow::field("phi", arrow::float32()),
                         arrow::field("flavor", arrow::uint8()),
                         arrow::field("trigMatched", arrow::list(arrow::boolean()))
                        }
                    );
			lepton_field = arrow::struct_(
						{
							arrow::field("n", arrow::uint32()),
							arrow::field("leptons", arrow::list(lepton_struct))
						}
					);

            // jets
            auto jet_struct = arrow::struct_(
                        {
                            arrow::field("pt", arrow::float32()),
                            arrow::field("eta", arrow::float32()),
                            arrow::field("phi", arrow::float32()),
                            arrow::field("m", arrow::float32()),
                            arrow::field("truthHadronPt", arrow::float32()),
                            arrow::field("truthHadronId", arrow::uint8()),
                            arrow::field("nTrk", arrow::uint32()),
                            arrow::field("bjet", arrow::uint8()),
                            arrow::field("bjet_score", arrow::float32())
                        }
                    );
			jet_field = arrow::struct_(
						{
							arrow::field("n", arrow::uint32()),
							arrow::field("jets", arrow::list(jet_struct))
						}
					);

            // met
            met_field = arrow::struct_(
                        {
                            arrow::field("sumEt", arrow::float32()),
                            arrow::field("met", arrow::float32()),
                            arrow::field("metPhi", arrow::float32()),
                            arrow::field("electron", arrow::float32()),
                            arrow::field("muon", arrow::float32()),
                            arrow::field("jet", arrow::float32()),
                            arrow::field("soft", arrow::float32())
                        }
                    );

            // event
            event_field = arrow::struct_(
                        {
                            arrow::field("w", arrow::float64()),
                            arrow::field("id", arrow::uint64()),
                            arrow::field("trig", arrow::list(arrow::boolean()))
                        }
                    );

            _fields.push_back(arrow::field("leptons", lepton_field));
            _fields.push_back(arrow::field("jets", jet_field));
            _fields.push_back(arrow::field("met", met_field));
            _fields.push_back(arrow::field("event", event_field));

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

        void generate_event() {

            std::uniform_int_distribution<int> lep_eff(0, 2);
            std::uniform_int_distribution<int> jet_eff(0, 10);
            std::uniform_real_distribution<double> n_lep_dis(0.0, 1.0);
            std::uniform_real_distribution<double> n_jet_dis(0.0, 10.0);
            std::uniform_real_distribution<double> pt_dis(0, 100.);
            std::uniform_real_distribution<double> eta_dis(-2.7, 2.7);
            std::uniform_real_distribution<double> phi_dis(-3.14, 3.14);
            std::normal_distribution<double> weight_dis(1.0, 0.03);

            // leptons
            int n_leptons = lep_eff(rng);
            std::vector<float> lepton_pt;
            std::vector<float> lepton_eta;
            std::vector<float> lepton_phi;
            std::vector<uint8_t> lepton_flavor;
            std::vector<std::vector<bool>> lepton_trig;
            for(size_t i = 0; i < n_leptons; i++) {
                lepton_pt.push_back( pt_dis(rng) );
                lepton_eta.push_back( eta_dis(rng) );
                lepton_phi.push_back( phi_dis(rng) );
                lepton_flavor.push_back(i*3);
                std::vector<bool> trig;
                for(size_t j = 0; j < 8; j++) {
                    trig.push_back(j%2==0);
                }
                lepton_trig.push_back(trig);
            } // i
            json lepton_field;
            std::vector<json> lepton_field_array;
            for(size_t i = 0; i < n_leptons; i++) {
                json p;
                p["pt"] = lepton_pt[i];
                p["eta"] = lepton_eta[i];
                p["phi"] = lepton_phi[i];
                p["flavor"] = lepton_flavor[i];
                p["trigMatched"] = lepton_trig[i];
                lepton_field_array.push_back(p);
            }
			lepton_field = {{"n", n_leptons}, {"leptons", lepton_field_array}};

            // jets
            int n_jets = jet_eff(rng);
            std::vector<float> jet_pt;
            std::vector<float> jet_eta;
            std::vector<float> jet_phi;
            std::vector<float> jet_m;
            std::vector<float> jet_hadPt;
            std::vector<uint8_t> jet_hadId;
            std::vector<uint32_t> jet_nTrk;
            std::vector<uint8_t> jet_bjet;
            std::vector<float> jet_bjet_score;
            for(size_t i = 0; i < n_jets; i++) {
                jet_pt.push_back( pt_dis(rng) );
                jet_eta.push_back( eta_dis(rng) );
                jet_phi.push_back( phi_dis(rng) );
                jet_m.push_back(pt_dis(rng) );
                jet_hadPt.push_back(pt_dis(rng) );
                jet_hadId.push_back(i%2==0);
                jet_nTrk.push_back(i*3);
				jet_bjet.push_back(i);
                jet_bjet_score.push_back(eta_dis(rng));
            } // i

            json jet_field;
            std::vector<json> jet_field_array;
            for(size_t i = 0; i < n_jets; i++) {
                json p;
                p["pt"] = jet_pt[i];
                p["eta"] = jet_eta[i];
                p["phi"] = jet_phi[i];
                p["m"] = jet_m[i];
                p["truthHadronPt"] = jet_hadPt[i];
                p["truthHadronId"] = jet_hadId[i];
                p["nTrk"] = jet_nTrk[i];
                p["bjet"] = jet_bjet[i];
                p["bjet_score"] = jet_bjet_score[i];
                jet_field_array.push_back(p);
            }
			jet_field["jets"] = jet_field_array;
			jet_field = {{"n", n_jets}, {"jets", jet_field_array}};

            // met
            float sumet = pt_dis(rng);
            float met = eta_dis(rng);
            float metPhi = phi_dis(rng);
            float met_electron = pt_dis(rng);
            float met_muon = pt_dis(rng);
            float met_jet = pt_dis(rng);
            float met_soft = phi_dis(rng);

            json jmet;
            jmet["sumEt"] = sumet;
            jmet["met"] = met;
            jmet["metPhi"] = metPhi;
            jmet["electron"] = met_electron;
            jmet["muon"] = met_muon;
            jmet["jet"] = met_jet;
            jmet["soft"] = met_soft;
			json met_field;
			met_field["met"] = jmet;

            // event
            double event_w = weight_dis(rng);
            uint64_t event_id = _event_count;
            std::vector<bool> trigger_match_list;
            for(size_t i = 0; i < 15; i++) {
                trigger_match_list.push_back(i%2 == 0);
            }
            json jevent;
            jevent["w"] = event_w;
            jevent["id"] = event_id;
            jevent["trig"] = trigger_match_list;

            json event_field;
            event_field["event"] = jevent;

            //
            // fill
            //
			lepton_vec.push_back(lepton_field);
			jet_vec.push_back(jet_field);
			met_vec.push_back(jmet);
            event_vec.push_back(jevent);

            _event_count++;
            if(_event_count % _n_rows_in_group == 0) {
                write();
            }

        }

        void write() {

            auto length = event_vec.size();

			// leptons
			std::string lepton_jstr = lepton_vec.at(0).dump();
			if(length > 1) {
				for(size_t i = 1; i < event_vec.size(); i++) {
					lepton_jstr += ",";
					lepton_jstr += lepton_vec.at(i).dump();
				}
			}
			lepton_jstr = "[" + lepton_jstr + "]";

			// jets
			std::string jet_jstr = jet_vec.at(0).dump();
			if(length > 1) {
				for(size_t i = 1; i < event_vec.size(); i++) {
					jet_jstr += ",";
					jet_jstr += jet_vec.at(i).dump();
				}
			}
			jet_jstr = "[" + jet_jstr + "]";

			// mets
			std::string met_jstr = met_vec.at(0).dump();
			if(length > 1) {
				for(size_t i = 1; i < event_vec.size(); i++) {
					met_jstr += ",";
					met_jstr += met_vec.at(i).dump();
				}
			}
			met_jstr = "[" + met_jstr + "]";

			// events
			std::string event_jstr = event_vec.at(0).dump();
			if(length > 1) {
				for(size_t i = 1; i < event_vec.size(); i++) {
					event_jstr += ",";
					event_jstr += event_vec.at(i).dump();
				}
			}
			event_jstr = "[" + event_jstr + "]";

			auto lepton_array = ArrayFromJSON(lepton_field, arrow::util::string_view(lepton_jstr));
			auto jet_array = ArrayFromJSON(jet_field, arrow::util::string_view(jet_jstr));
			auto met_array = ArrayFromJSON(met_field, arrow::util::string_view(met_jstr));
			auto event_array = ArrayFromJSON(event_field, arrow::util::string_view(event_jstr));

            auto table = arrow::Table::Make(_schema, {lepton_array, jet_array, met_array, event_array});
            PARQUET_THROW_NOT_OK(_writer->WriteTable(*table, length));

            PARQUET_THROW_NOT_OK(outfile->Flush());
            _arrays.clear();

			lepton_vec.clear();
			jet_vec.clear();
			met_vec.clear();
			event_vec.clear();

        }

        void finish() {
            write();
            PARQUET_THROW_NOT_OK(_writer->Close());
        }


    private :
        std::unique_ptr<parquet::arrow::FileWriter> _writer;
        std::shared_ptr<arrow::io::OutputStream> outfile;
        int32_t _n_rows_in_group;
        std::string _outdir;
        std::string _dataset_name;
        uint32_t _event_count;
        uint32_t _file_count;

        std::shared_ptr<arrow::Schema> _schema;
        std::vector<std::shared_ptr<arrow::Field>> _fields;
        std::vector<std::shared_ptr<arrow::Array>> _arrays;
        std::default_random_engine rng;



		std::shared_ptr<arrow::DataType> lepton_field;
		std::shared_ptr<arrow::DataType> jet_field;
		std::shared_ptr<arrow::DataType> met_field;
		std::shared_ptr<arrow::DataType> event_field;

        std::vector<json> lepton_vec;
        std::vector<json> jet_vec;
        std::vector<json> met_vec;
        std::vector<json> event_vec;

};

int main(int argc, char* argv[]) {

    DatasetGenerator dsgen;
    dsgen.init();
    for(size_t i = 0; i < 10; i++) {
        if(i%10000 == 0) {
            std::cout << "generating event " << i << std::endl;
        }
        //dsgen.generate_event_flat();
        dsgen.generate_event();
    }
    dsgen.finish();
    return 0;
}
