# danny_tries_parquet

## Installation

Follow the usual CMake routine:
```
$ cd /path/to/danny_tries_parquet
$ source set-arrow
$ mkdir build
$ cd build
$ cmake ..
$ make -j4
```

## Generate a HEP-like dataset
Upon successful [installation](#installation), run the script `gen-dataset`:
```
$ cd /path/to/danny_tries_parquet/build
$ ./gen-dataset
```
Which will generate a single-file Parquet dataset `dataset_gen/dummy_0.parquet`.

Additional options can be given to `gen-dataset` by specifing the `-h|--help` option, `./gen-dataset -h`.
You can specify the number of events to generate, the compression algorithm (`UNCOMPRESSED`, `SNAPPY`, or `GZIP`),
and other things like the number of events to store per RowGroup in the output Parquet file.
The latter specification of the RowGroup size will have noticeable impact on the write speed.

Currently, the dataset generation relies on `ArrayFromJSON` calls to build up
the set of arrays to write out.
This means that during each write cycle large arrays of JSON objects are constructed, serialized to `std::string`, and then
concatenated to create large strings that then represent the `arrow::Table` to be written out.
This is probably not very performant, but is used for initial testing purposes.
Moving forward, a more realistic use case will use the `StructBuilder` API directly for
constructing the HEP-like data structures.

## Check how fast Parquet datasets can be read using Awkward
[Awkward](https://awkward-array.readthedocs.io/en/latest/) can be used to read Parquet
files and is nicely suited given that its internal memory representation
is columnar and fashioned on Arrow (the in-memory representation of Parquet).

The script [test-parquet.py](src/python/test-parquet.py) can be used to test the
performance of iterating over an entire Parquet file using [awkward] arrays.
The script uses the `timeit` module to repeatedly iterate over an entire file and
then report the statistics (mean +/- std. deviation) of the time it takes
to do the full iteration.

The script [test-parquet.py](src/python/test-parquet.py) iterates over the input
Parquet file in chunks, where each chunk is a specified number of row groups
read from disk at a time in each iteration. The number of row-groups to chunk by
during the read process can be specified by the `-c|--chunk-size` option.
A typical example run could be,
```
$ python test-parquet.py --repeats 100 -c 100000
Average of 100 trials: 0.01263 +/- 0.00100 seconds # for a 1_000_000 event dataset
```

## Getting Arrow+Parquet
On MacOS, use `homebrew`:
```
brew install arrow==5.0.0
```
Using the [FindArrow.cmake](FindArrow.cmake) and [FindParquet.cmake](FindParquet.cmake)
cmake modules for defining the build variables needed for the CMake configuration.

## Useful Sources

- [parquet-cpp/examples](https://github.com/apache/parquet-cpp/tree/master/examples/parquet-arrow)
- [arrow/table_test.cc](https://github.com/apache/arrow/blob/8e43f23dcc6a9e630516228f110c48b64d13cec6/cpp/src/arrow/table_test.cc)
- [arrow/table_builder_test.cc](https://github.com/apache/arrow/blob/8e43f23dcc6a9e630516228f110c48b64d13cec6/cpp/src/arrow/table_builder_test.cc)
- [arrow/stl_test.cc](https://github.com/apache/arrow/blob/8e43f23dcc6a9e630516228f110c48b64d13cec6/cpp/src/arrow/stl_test.cc)
- [maybe this struct builder](https://github.com/apache/arrow/issues/4253)
