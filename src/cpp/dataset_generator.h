//#pragma once

//std/stl
#include <string>
#include <vector>
#include <memory>
#include <random>

//arrow/parquet
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/type.h> // struct_
#include <arrow/ipc/json_simple.h> // ArrayFromJSON
namespace parquet {
    namespace arrow {
        class FileWriter;
    }
}

//nlohmann
#include "json.hpp"

namespace helpers {

    std::shared_ptr<arrow::Array> ArrayFromJSON(
            const std::shared_ptr<arrow::DataType>& type,
            const std::string& json
    );
}; // namespace helpers

class DatasetGenerator {
    public:
        DatasetGenerator(int32_t n_rows_per_group = -1);
        ~DatasetGenerator() = default;

        void init(const std::string& dataset_name, const std::string& output_dir,
                const std::string& select_compression = "UNCOMPRESSED");
        void generate_event();
        void finish();

    private :

        //
        // output
        //
        std::unique_ptr<parquet::arrow::FileWriter> _writer;
        std::shared_ptr<arrow::io::OutputStream> _outfile;
        std::string _outdir;
        std::string _dataset_name;
        uint32_t _file_count; // in case we want to partition the dataset

        //
        // parquet file properties
        //

        std::shared_ptr<arrow::Schema> _schema;
        std::vector<std::shared_ptr<arrow::Field>> _fields;
        std::vector<std::shared_ptr<arrow::Array>> _arrays;

        // number of rows (events) per RowGroup in the output Parquet file
        int32_t _n_rows_in_group;

        //
        // event quantities
        //

        std::default_random_engine _rng;
        std::uniform_int_distribution<int> _lep_eff_dist;
        std::uniform_int_distribution<int> _jet_eff_dist;
        std::uniform_real_distribution<double> _pt_dist;
        std::uniform_real_distribution<double> _eta_dist;
        std::uniform_real_distribution<double> _phi_dist;
        std::normal_distribution<double> _weight_dist;

        // number of events processed so far
        uint32_t _event_count;

        // per-event containers for our data fields (one per column in the output Parquet file)
        std::shared_ptr<arrow::DataType> _lepton_field;
        std::shared_ptr<arrow::DataType> _jet_field;
        std::shared_ptr<arrow::DataType> _met_field;
        std::shared_ptr<arrow::DataType> _event_field;

        // buffers for stored rows for each of the columns, will be flushed
        // to the output file every _n_rows_in_group events
        std::vector<nlohmann::json> _lepton_buffer;
        std::vector<nlohmann::json> _jet_buffer;
        std::vector<nlohmann::json> _met_buffer;
        std::vector<nlohmann::json> _event_buffer;

        void create_fields();
        void initialize_writer(const std::string& compression = "UNCOMPRESSED");
        void fill();
        void fill_leptons();
        void fill_jets();
        void fill_met();
        void fill_event();
        void flush();
        void clear_buffers();
}; // class DatasetGenerator
