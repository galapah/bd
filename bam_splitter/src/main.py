#!/usr/bin/env python3

# TEST
# ./main.py -1 ../bam/test_read1.fastq.gz -2 ../bam/test_read2.fastq.gz test dbfile
#
#

import sys
import argparse
from pybamsplit.read_processor import ReadProcessor

DB_FILENAME = ".bamsplitter.db"
RECORDS_IN_BUFFER = 10000000
RECORDS_IN_BUFFER_FASTQ = 1000
OUT_DIR = "../output_test/"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('cmd', help='command to execute', choices=['build', 'process', 'read', "test"])
    parser.add_argument('db_file', help='path to the database filename')
    parser.add_argument('-1', help='path to reads1 fastq file')
    parser.add_argument('-2', help='path to reads2 fastq file')
    parser.add_argument('-i', help='number of leading chars in the read ID to ignore')
    args = parser.parse_args()
    args = args.__dict__
    cmd = args["cmd"]
    db_file = args["db_file"]

    read_processor = ReadProcessor(OUT_DIR, db_file)

    if cmd == "build":
        read_processor.read_and_store(RECORDS_IN_BUFFER)
    elif cmd == "process":
        read_processor.process_db()
    elif cmd == "read":
        read_processor.retrieve(args["1"], args["2"], int(args["i"]), RECORDS_IN_BUFFER_FASTQ)
    else:
        raise Exception("ERROR: unknown command provided: ", cmd)
    sys.exit()


