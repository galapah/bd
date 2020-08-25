#!/usr/bin/env python3
#
# 
#

import sys
import argparse
from pybamsplit.read_processor import ReadProcessor

#RECORDS_IN_BUFFER = 100000
RECORDS_IN_BUFFER = 10000000 # ~ 2.7GB RAM
RECORDS_IN_BUFFER_FASTQ = 100000
minimumSampleAssociationThreshold = 0.75
OUT_DIR = "../output_test/"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('cmd', help='command to execute', choices=['build', 'process', 'retrieve'])
    parser.add_argument('db_file', help='path to the database filename')
    parser.add_argument('-o', help='path to the output directory')
    parser.add_argument('-1', help='path to reads1 fastq file')
    parser.add_argument('-2', help='path to reads2 fastq file')
    parser.add_argument('-i', help='number of leading chars in the read ID to ignore')
    args = parser.parse_args()
    args = args.__dict__
    cmd = args["cmd"]
    db_file = args["db_file"]
    out_dir = args["o"]

    read_processor = ReadProcessor(out_dir, db_file, minimumSampleAssociationThreshold)

    if cmd == "build":
        read_processor.read_and_store(RECORDS_IN_BUFFER)
    elif cmd == "process":
        read_processor.process_db()
    elif cmd == "retrieve":
        read_processor.retrieve(args["1"], args["2"], int(args["i"]), RECORDS_IN_BUFFER_FASTQ)
    else:
        raise Exception("ERROR: unknown command provided: ", cmd)
    sys.exit()


