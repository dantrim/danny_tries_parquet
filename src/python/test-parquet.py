#!/usr/bin/env python

from argparse import ArgumentParser
import sys
import timeit

import numpy as np
import awkward as ak
import pyarrow as pa
import pyarrow.parquet as pq
import json

from pathlib import Path

SETUP = """
from pathlib import Path
import glob
import numpy as np
import awkward as ak
import pyarrow as pa
import pyarrow.parquet as pq
import json
def chunkgen(parquet_file, row_groups_per_chunk = 1000, use_threads = True, lazy = True, columns = None) :

    pf = pq.ParquetFile(parquet_file)
    num_row_groups = pf.num_row_groups
    if row_groups_per_chunk >= num_row_groups :
        row_groups_per_chunk = num_row_groups
    start = 0
    stop = row_groups_per_chunk
    for x in range(0, num_row_groups, row_groups_per_chunk) :
        which = np.arange(start, stop)
        yield ak.from_parquet(parquet_file,
                        columns = columns,
                        use_threads = use_threads,
                        lazy = lazy,
                        row_groups = which
        )
        start = x
        stop = start + row_groups_per_chunk


def job_loop(parquet_file, chunk_size = 1000, use_threads = True, columns = None) :
    files = []
    if Path(parquet_file).is_dir() :
        files = glob.glob(f"{parquet_file}/*.parquet")
    else :
        files = [parquet_file]
    n_total = 0
    for file in files :
        for ichunk, chunk in enumerate(chunkgen(file, chunk_size, use_threads = use_threads, columns = columns)) :
            n_total += len(chunk)
            pass
            #jpt = chunk["jet_pt"]
    print(f"n events = {n_total}")
"""

def main() :

    parser = ArgumentParser()
    parser.add_argument("input_file")
    parser.add_argument("-c", "--chunk-size", type = int, default = 1)
    parser.add_argument("-t", "--threads", action = "store_true", default = False)
    parser.add_argument("--n-columns", default = -1, type = int)
    parser.add_argument("--repeats", default = 5, type = int)
    args = parser.parse_args()

    if not Path(args.input_file).exists() :
        raise Exception(f"ERROR Input file {args.input_file} not found")

    #pf = pq.ParquetFile(args.input_file)

    columns_to_read = None
    if args.n_columns > 0 :
        pf = pq.ParquetDataset(args.input_file)
        fields = pf.schema.to_arrow_schema().names
        if args.n_columns >= len(fields) :
            columns_to_read = fields
        else :
            columns_to_read = fields[:args.n_columns]


    CODE_TO_RUN = f"""
job_loop('{args.input_file}', chunk_size = {args.chunk_size}, use_threads = {args.threads}, columns = {columns_to_read})
"""

    #job_loop(args.input_file, args.chunk_size, use_threads = args.threads)
    #print(timeit.timeit(CODE_TO_RUN, setup=SETUP, number = 1, repeat = 5))
    repeats = np.array(timeit.repeat(CODE_TO_RUN, setup=SETUP, number = 1, repeat = args.repeats))
    print(f"Average of {args.repeats} trials: {np.mean(repeats):.5f} +/- {np.std(repeats):.5f} seconds")


    



if __name__ == "__main__" :
    main()
