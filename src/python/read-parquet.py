#!/usr/bin/env python

from argparse import ArgumentParser
from pathlib import Path
import sys

import numpy as np
import awkward as ak
import pyarrow as pa
import pyarrow.parquet as pq

import json

def chunk(filepath, chunksize = 100000, use_threads = False) :

    pf = pq.ParquetFile(filepath)
    num_row_groups = pf.num_row_groups
    print(f"FOO num_row_groups = {num_row_groups}")

    print(70 * "=")
    start = 0
    stop = chunksize
    for x in range(0, num_row_groups, chunksize) :
        which = np.arange(start, stop)
        yield ak.from_arrow(pf.read_row_groups(which, use_threads = use_threads))
        #yield ak.from_parquet(filepath, columns = ["jet_pt", "jet_mask"], use_threads = True, row_groups = which)
        start = x
        stop = start + chunksize

def main() :

    parser = ArgumentParser()
    parser.add_argument("input_file")
    args = parser.parse_args()

    p = Path(args.input_file)
    if not p.exists() :
        raise Exception("bad input file")


    ##
    ## get field metadata
    ##
    pf = pq.ParquetDataset(p)
    arrow_schema = pf.schema.to_arrow_schema()
    field_names = arrow_schema.names
    print(f"field names = {field_names}")
    metadata = arrow_schema.metadata
    tmp = {}
    for key, val in metadata.items() :
        key = key.decode("utf-8")
        val = val.decode("utf-8")
        tmp[key] = val
    metadata = tmp
    metadata = json.loads(metadata["metadata"])
    print(f"metadata = {metadata}")
    print(json.dumps(metadata, indent = 4))

    ##
    ## get field metadat
    ##
    field_metadata = {}



    ##
    ## some awkward array stuff
    #
    #arr = ak.from_parquet(p, columns = ["jet_pt", "jet_mask"], use_threads = True)#, row_groups = which)
    #print(arr)
    #for x in arr :
    #n_jets = x["jet_n"]
    #jpt = arr["jet_pt"][arr["jet_mask"]]
    #print(jpt)
    ##print(x["jet_pt"])
    ##print(x["jet_pt"][~np.isnan(x["jet_pt"])])
    #sys.exit()

    #arr = ak.from_parquet(p,  columns = ["jet_pt", "jet_mask"],use_threads = True)#,  row_groups=[0,1])
    #print(arr)
    #sys.exit()
    #sys.exit()
    for iarr, arr in enumerate(chunk(p, 100, True)) :
        print(55 * "-")
        print(f" iarr = {iarr}, arr len = {len(arr)}")
        #print(arr)
        #for x in arr :
        #n_jets = x["jet_n"]
        jpt = arr["jet_pt"]#[arr["jet_mask"]]
        print(jpt)
        #print(x["jet_pt"])
        #print(x["jet_pt"][~np.isnan(x["jet_pt"])])
        #print(x["jet_pt"][x["jet_mask"]])
    sys.exit()

    print(70 * "=")
    arr = ak.from_parquet(p, columns=["f0", "f1", "f2"],use_threads = False, lazy = False, row_groups = np.arange(0,4,1))
    print(arr)
    print(len(arr))
    ev0 = arr[0]
    f0 = ev0["f0"]
    f2 = ev0["f2"]
    for ievent, elem in enumerate(arr) :
        print(55 * '-')
        print(f"EVent {ievent}")
        n_f2 = elem["f0"]
        f2 = elem["f2"]
        if n_f2 :
            print(f" {n_f2} f2s: {f2}")#[:n_f2]}")
        #print(f"f0 = {f0}, f1 = {f1}, f2 len = {len(f2)}")
        #for item in f2 :
        #    print(f"  -> f2: {item}")
    sys.exit()
    #nparrf0 = ak.to_numpy(arr["f0"])
    #nparrf1 = ak.to_numpy(arr["f1"])
    #padded = ak.to_numpy(ak.pad_none(arr["f2"], 2, clip = True))
    #print(f"padded = {padded}")
    #for p in padded :
    #    print(f" -> {p}")
    #sys.exit()
    #nparrf2 = ak.to_numpy(arr["f2"])
    #print(f"nparr_f0 = {nparrf0}")
    #print(f"nparr_f1 = {nparrf1}")
    #print(f"nparr_f2 = {nparrf2}")

    ###
    ### pyarrow
    ###
    print(70 * "=")
    #pf = pq.ParquetFile(p)
    #print(pf.metadata)
    #print(pf.schema)
    #table = pq.read_table(p)
    #sys.exit()

    #pf = pq.ParquetDataset(p)
    #print(pf.schema)

    #metadata_collector = []
    #root_path = "dummy_dataset"
    ##partition_cols = ["f0"]
    #partition_cols = []
    #pq.write_to_dataset(table,
    #        root_path=root_path,
    #        partition_cols=partition_cols,
    #        #partition_filename_cb = lambda x : "foo.parquet",
    #        metadata_collector = metadata_collector
    #)

    #print(35 * '- ')
    #filename = "dummy_dataset/"
    #pf2 = pq.ParquetDataset(filename, use_legacy_dataset = False)
    #t = pf2.read(columns = ["f0", "f1"], use_threads = True)
    #print(t)
    #for k in t :
    #    print(k)
    ##print(pf2.metadata)
    ##print(pf2.schema)





if __name__ == "__main__" :
    main()
