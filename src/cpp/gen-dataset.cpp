#include "dataset_generator.h"

//std/stl
#include <iostream>
#include <sstream>

void print_usage(char* argv[]) {
    std::cout << "---------------------------------------------------------------------------" << std::endl;
    std::cout << " Generate a dummy HEP-like dataset and store it in the Parquet file format" << std::endl;
    std::cout << std::endl;
    std::cout << " Usage: " << argv[0] << " [OPTIONS]" << std::endl;
    std::cout << std::endl;
    std::cout << " Options:" << std::endl;
    std::cout << "   --name                 Name of output dataset [default: \"dummy\"]" << std::endl;
    std::cout << "   -o|--outdir            Output directory to store files in [default: \"./dataset_gen\"]" << std::endl;
    std::cout << "   -n|--n-events          Number of events to generate [default: 5000]" << std::endl;
    std::cout << "   -c|--compression       Compression setting (Options: UNCOMPRESSED, SNAPPY, GZIP) [default: UNCOMPRESSED]" << std::endl;
    std::cout << "   -r|--row-group-size    Number of events per Parquet RowGroup [default: 250000/# of fields]" << std::endl;
    std::cout << "   -h|--help              Print this help message and exit" << std::endl;
    std::cout << "---------------------------------------------------------------------------" << std::endl;

}

int main(int argc, char* argv[]) {

    uint64_t n_events = 5000;
    std::string outdir = "./dataset_gen";
    std::string dataset_name = "dummy";
    std::string compression = "UNCOMPRESSED";
    int32_t row_group_size = -1;

    for(size_t i = 1; i < argc; i++) {
        if      (strcmp(argv[i], "--name") == 0) { dataset_name = argv[++i]; }
        else if (strcmp(argv[i], "-o") == 0 || strcmp(argv[i], "--outdir") == 0) { outdir = argv[++i]; }
        else if (strcmp(argv[i], "-n") == 0 || strcmp(argv[i], "--n-events") == 0) { n_events = std::stoi(argv[++i]); }
        else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) { print_usage(argv); return 0; }
        else if (strcmp(argv[i], "-r") == 0 || strcmp(argv[i], "--row-group-size") == 0) { row_group_size = std::stoi(argv[++i]); }
        else if (strcmp(argv[i], "-c") == 0 || strcmp(argv[i], "--compression") == 0) { compression = argv[++i]; }
        else {
            std::cout << argv[0] << " Unknown command line argument provided: " << argv[i] << std::endl;
            return 1;
        }
    }

    uint32_t count_rate = 100;
    if(n_events >= 500000) {
        count_rate = 50000;
    } else if(n_events >= 100000) {
        count_rate = 10000;
    } else if(n_events >= 5000) {
        count_rate = 1000;
    }
    DatasetGenerator ds(row_group_size);
    ds.init(dataset_name, outdir, compression);
    for(size_t i = 0; i < n_events; i++) {
        if(i%count_rate ==0) {
            std::cout << "INFO: *** Generating event " << i << " / " << n_events << " (" << static_cast<float>(i)/n_events * 100. << " %) ***" << std::endl;
        }
        ds.generate_event();
    }
    ds.finish();

}
