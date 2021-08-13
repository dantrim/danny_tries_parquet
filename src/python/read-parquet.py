#!/usr/bin/env python

from argparse import ArgumentParser
from pathlib import Path
import sys

import awkward as ak
import pyarrow as pa

def main() :

    print(f"awkward version: {ak.__version__}")
    print(f"pyarrow version: {pa.__version__}")
    parser = ArgumentParser()
    parser.add_argument("input_file")
    args = parser.parse_args()

    p = Path(args.input_file)
    if not p.exists() :
        raise Exception("bad input file")


    arr = ak.from_parquet(p)#, use_threads = True, lazy = True)
    print(arr)
    print(len(arr))
    for elem in arr :
        f0 = elem["f0"]
        f1 = elem["f1"]
        f2 = elem["f2"]
        print(f"f0 = {f0}, f1 = {f1}, f2 len = {len(f2)}")
        for item in f2 :
            print(f"  -> f2: {item}")




if __name__ == "__main__" :
    main()
