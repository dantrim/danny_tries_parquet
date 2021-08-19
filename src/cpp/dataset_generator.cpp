#include "dataset_generator.h"

// std/stl
#include <iostream>
#include <stdint.h>
#include <map>
#include <cmath>
#include <filesystem> // absolute
#include <sstream>

// json
using nlohmann::json;

namespace helpers {

std::shared_ptr<arrow::Array> ArrayFromJSON(
        const std::shared_ptr<arrow::DataType>& type,
        const std::string& json) {
    std::shared_ptr<arrow::Array> out;
    PARQUET_THROW_NOT_OK(arrow::ipc::internal::json::ArrayFromJSON(type, json, &out));
    return out;
}

};

DatasetGenerator::DatasetGenerator(int32_t n_rows_per_group) :
    _n_rows_in_group(n_rows_per_group),
    _outdir("./dataset_gen"),
    _dataset_name("dummy"),
    _event_count(0),
    _file_count(0)
{
    _lep_eff_dist = std::uniform_int_distribution<int>(0,2);
    _jet_eff_dist = std::uniform_int_distribution<int>(0, 10);
    _pt_dist = std::uniform_real_distribution<double>(0., 100.);
    _eta_dist = std::uniform_real_distribution<double>(-2.7, 2.7);
    _phi_dist = std::uniform_real_distribution<double>(-3.14, 3.14);
    _weight_dist = std::normal_distribution<double>(1.0, 0.3);
}

void DatasetGenerator::init(const std::string& dataset_name,
        const std::string& output_dir,
        const std::string& select_compression) {
    //
    // setup the output file and it's path
    //
    _outdir = output_dir;
    _dataset_name = dataset_name;
    std::string internalpath;
    auto fs = arrow::fs::FileSystemFromUriOrPath(std::filesystem::absolute(_outdir),
            &internalpath).ValueOrDie();
    PARQUET_THROW_NOT_OK(fs->CreateDir(internalpath));
    auto internal_fs = std::make_shared<arrow::fs::SubTreeFileSystem>(internalpath, fs);

    //
    // initialize the output Parquet file writer
    //
    std::stringstream outfilename;
    outfilename << dataset_name << "_" << _file_count << ".parquet";
    PARQUET_ASSIGN_OR_THROW(
                _outfile,
                internal_fs->OpenOutputStream(outfilename.str())
            );
    _file_count++;

    // setup the dataset columns and structure
    create_fields();

    // metadata to attach to the output file
    json j_metadata;
    j_metadata["dsid"] = 410472;
    j_metadata["campaign"] = "mc16d";
    j_metadata["sample_name"] = "mc16d.410472.foobar.ttbar";
    j_metadata["tag"] = "v0.1.0";
    j_metadata["creation_date"] = "2021-08-18";
    std::unordered_map<std::string, std::string> metadata_map;
    metadata_map["metadata"] = j_metadata.dump();
    arrow::KeyValueMetadata keyval_metadata(metadata_map);

    // setup the file schema
    _schema = arrow::schema(_fields);
    _schema = _schema->WithMetadata(keyval_metadata.Copy());

    if(_n_rows_in_group < 0) {
        _n_rows_in_group = 250000 / _fields.size();
    }

    initialize_writer(select_compression);
}

void DatasetGenerator::initialize_writer(const std::string& select_compression) {

    auto compression = arrow::Compression::UNCOMPRESSED;
    if(select_compression == "SNAPPY") {
        compression = arrow::Compression::SNAPPY;
    } else if(select_compression == "GZIP") {
        compression = arrow::Compression::GZIP;
    } else if(select_compression == "UNCOMPRESSED") {
        compression = arrow::Compression::UNCOMPRESSED;
    } else {
        std::cout << "WARNING: Unhandled compression type \"" << select_compression << "\" specified, falling back to Compression::UNCOMPRESSED" << std::endl;
        compression = arrow::Compression::UNCOMPRESSED;
    }

    // setup the output writers
    auto writer_props = parquet::WriterProperties::Builder()
        .compression(compression)
        ->data_pagesize(1024*1024*10)
        ->build();

    // we must call "store_schema" in order for the KeyvalueMetadata to be persistifed in the output Parquet file
    auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
    PARQUET_THROW_NOT_OK(parquet::arrow::FileWriter::Open(*_schema,
                arrow::default_memory_pool(),
                _outfile,
                writer_props,
                arrow_props,
                &_writer
    ));

}

void DatasetGenerator::create_fields() {

    _fields.clear();

    //
    // leptons
    //

    // lepton struct to represent each lepton object
    auto lepton_struct = arrow::struct_(
                {
                    arrow::field("pt", arrow::float32()),
                    arrow::field("eta", arrow::float32()),
                    arrow::field("phi", arrow::float32()),
                    arrow::field("flavor", arrow::int8()),
                    arrow::field("isLoose", arrow::boolean()),
                    arrow::field("isMedium", arrow::boolean()),
                    arrow::field("isTight", arrow::boolean()),
                    arrow::field("isTrigMatched", arrow::list(arrow::boolean()))
                }
            );
    // the lepton column will be composed of some top-level fields/metadata
    // associated with the event's leptons in addition to the actual list
    // of leptons
    _lepton_field = arrow::struct_(
                {
                    arrow::field("n", arrow::uint8()),
                    arrow::field("leptons", arrow::list(lepton_struct))
                }
            );

    //
    // jets
    //

    // jet struct to represent each jet object
    auto jet_struct = arrow::struct_(
                {
                    arrow::field("pt", arrow::float32()),
                    arrow::field("eta", arrow::float32()),
                    arrow::field("phi", arrow::float32()),
                    arrow::field("m", arrow::float32()),
                    arrow::field("truthHadronPt", arrow::float32()),
                    arrow::field("truthHadronId", arrow::float32()),
                    arrow::field("nTrk", arrow::uint8()),
                    arrow::field("isBjet", arrow::boolean()),
                    arrow::field("bTagScore", arrow::float32())
                }
            );
    // the jet column will be composed of some top-level fields/metadata
    // associated with the event's jets in addition to the actual list
    // of jets
    _jet_field = arrow::struct_(
                {
                    arrow::field("n", arrow::uint8()),
                    arrow::field("jets", arrow::list(jet_struct))
                }
            );

    //
    // met
    //

    // the met field is a per-event quantity, so there is no additional metadata
    _met_field = arrow::struct_(
                {
                    arrow::field("sumEt", arrow::float32()),
                    arrow::field("met", arrow::float32()),
                    arrow::field("metPhi", arrow::float32()),
                    arrow::field("electronTerm", arrow::float32()),
                    arrow::field("muonTerm", arrow::float32()),
                    arrow::field("jetTerm", arrow::float32()),
                    arrow::field("softTerm", arrow::float32())
                }
            );

    //
    // event
    //

    // event wide information: event weights, event trigger masks, etc...
    _event_field = arrow::struct_(
                {
                    arrow::field("w", arrow::float64()),
                    arrow::field("sumw2", arrow::float64()),
                    arrow::field("id", arrow::uint64()),
                    arrow::field("trigMask", arrow::list(arrow::boolean()))
                }
            );


    //
    // store
    //
    _fields.push_back(arrow::field("leptons", _lepton_field));
    _fields.push_back(arrow::field("jets", _jet_field));
    _fields.push_back(arrow::field("met", _met_field));
    _fields.push_back(arrow::field("event", _event_field));

}

void DatasetGenerator::generate_event() {

    //
    // generate leptons with random values for their attributes
    //
    int n_leptons = _lep_eff_dist(_rng);
    std::vector<json> lepton_field_array;
    for(size_t i = 0; i < n_leptons; i++) {
        json p;
        p["pt"] = _pt_dist(_rng);
        p["eta"] = _eta_dist(_rng);
        p["phi"] = _phi_dist(_rng);
        p["flavor"] = _lep_eff_dist(_rng);
        p["isLoose"] = i*2;
        p["isMedium"] = i*3;
        p["isTight"] = i*4;
        std::vector<bool> trigMatched;
        for(size_t itrig = 0; itrig < 8; itrig++) {
            trigMatched.push_back(itrig%2 == 0);
        } // itrig
        p["isTrigMatched"] = trigMatched;
        lepton_field_array.push_back(p);
    } // i
    json lepton_field = {{"n", n_leptons}, {"leptons", lepton_field_array}};

    //
    // generate jets with random values for their attributes
    //
    int n_jets = _jet_eff_dist(_rng);
    std::vector<json> jet_field_array;
    for(size_t i = 0; i < n_jets; i++) {
        json p;
        p["pt"] = _pt_dist(_rng);
        p["eta"] = _eta_dist(_rng);
        p["phi"] = _phi_dist(_rng);
        p["m"] = _pt_dist(_rng);
        p["truthHadronPt"] = _weight_dist(_rng) * p["pt"].get<float>();;
        p["truthHadronId"] = i*4;
        p["nTrk"] = i*3;
        p["isBjet"] = i%2 == 0;
        p["bTagScore"] = _pt_dist(_rng);
        jet_field_array.push_back(p);
    } // i
    json jet_field = {{"n", n_jets}, {"jets", jet_field_array}};

    //
    // generate random met
    //
    json met_field;
    met_field["sumEt"] = _pt_dist(_rng);
    met_field["met"] = _pt_dist(_rng);
    met_field["metPhi"] = _phi_dist(_rng);
    met_field["electronTerm"] = 0.3 * met_field["met"].get<float>();
    met_field["muonTerm"] = 0.05 * met_field["met"].get<float>();
    met_field["jetTerm"] = 0.6 * met_field["met"].get<float>();
    met_field["softTerm"] = 0.05 * met_field["met"].get<float>();

    //
    // event fields
    //
    json event_field;
    event_field["w"] = _weight_dist(_rng);
    event_field["sumw2"] = event_field["w"].get<float>() * event_field["w"].get<float>();
    event_field["id"] = _event_count;
    std::vector<bool> eventTrigMatch;
    for(size_t i = 0; i < 15; i++) {
        eventTrigMatch.push_back(i%2==0);
    }
    event_field["trigMask"] = eventTrigMatch;

    //
    // now store the event's quantities in the buffers, to be writen
    // out in the next RowGroup
    //
    _lepton_buffer.push_back(lepton_field);
    _jet_buffer.push_back(jet_field);
    _met_buffer.push_back(met_field);
    _event_buffer.push_back(event_field);

    //
    // increment the event counter and flush buffers if needed
    //
    _event_count++;
    if(_event_count % _n_rows_in_group == 0) {
        fill();
    }
}

void DatasetGenerator::finish() {
    fill();
    PARQUET_THROW_NOT_OK(_writer->Close());
}

void DatasetGenerator::fill() {
    _arrays.clear();

    fill_leptons();
    fill_jets();
    fill_met();
    fill_event();

    auto table = arrow::Table::Make(_schema, _arrays);
    PARQUET_THROW_NOT_OK(_writer->WriteTable(*table, _event_buffer.size()));

    // flush
    flush();
}

void DatasetGenerator::flush() {
    PARQUET_THROW_NOT_OK(_outfile->Flush());
    clear_buffers();
}

void DatasetGenerator::clear_buffers() {
    _arrays.clear();
    _lepton_buffer.clear();
    _jet_buffer.clear();
    _met_buffer.clear();
    _event_buffer.clear();
}

void DatasetGenerator::fill_leptons() {
    size_t n_events = _event_buffer.size();

    std::string jstr;
    for(size_t i = 0; i < n_events; i++) {
        if(i>0) {
            jstr += ",";
        }
        jstr += _lepton_buffer.at(i).dump();
    } // i
    jstr = "[" + jstr + "]";
    auto lepton_array = helpers::ArrayFromJSON(_lepton_field, jstr);
    _arrays.push_back(lepton_array);
}

void DatasetGenerator::fill_jets() {
    size_t n_events = _event_buffer.size();

    std::string jstr;
    for(size_t i = 0; i < n_events; i++) {
        if(i>0) {
            jstr += ",";
        }
        jstr += _jet_buffer.at(i).dump();
    } // i
    jstr = "[" + jstr + "]";
    auto jet_array = helpers::ArrayFromJSON(_jet_field, jstr);
    _arrays.push_back(jet_array);
}

void DatasetGenerator::fill_met() {
    size_t n_events = _event_buffer.size();

    std::string jstr;
    for(size_t i = 0; i < n_events; i++) {
        if(i>0) {
            jstr += ",";
        }
        jstr += _met_buffer.at(i).dump();
    } // i
    jstr = "[" + jstr + "]";
    auto met_array = helpers::ArrayFromJSON(_met_field, jstr);
    _arrays.push_back(met_array);
}

void DatasetGenerator::fill_event() {
    size_t n_events = _event_buffer.size();

    std::string jstr;
    for(size_t i = 0; i < n_events; i++) {
        if(i>0) {
            jstr += ",";
        }
        jstr += _event_buffer.at(i).dump();
    } // i
    jstr = "[" + jstr + "]";
    auto event_array = helpers::ArrayFromJSON(_event_field, jstr);
    _arrays.push_back(event_array);
}

