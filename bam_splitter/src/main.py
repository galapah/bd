#!/usr/bin/env python3
"""main.py: command-line interface to the read processing module"""

__author__ = "Jan Hapala"
__version__ = "1.0.0"
__maintainer__ = "Jan Hapala"
__email__ = "jan@hapala.cz"
__status__ = "Production"

import sys
import argparse
from pybamsplit.read_processor import ReadProcessor

RECORDS_IN_BUFFER = 10000000 # ~ 2.7GB RAM
RECORDS_IN_BUFFER_FASTQ = 100000
minimumSampleAssociationThreshold = 0.75


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('cmd', help='command to execute', choices=['build', 'process', 'retrieve'])
    parser.add_argument('db_file', help='path to the database filename')
    parser.add_argument('-d', help='path to the output directory')
    parser.add_argument('-1', help='path to reads1 fastq file')
    parser.add_argument('-2', help='path to reads2 fastq file')
    parser.add_argument('-f', help='buffer size in number of alignments from the BAM files that are collected before writing to the database')
    parser.add_argument('-F', help='buffer size in number of FASTQ reads loaded before looking up in the database and writing to the output files')
    parser.add_argument('--stats', help='print statistics on the processed reads, it can slow down the computation by hours in case of big input files', action="store_true")
    parser.add_argument('--no-del', dest="no_del", help='do not delete the database file after finishing', action="store_true")
    args = parser.parse_args()
    args = args.__dict__
    cmd = args["cmd"]
    db_file = args["db_file"]
    out_dir = args["d"]
    alignment_buffer_size = args["f"]
    fastq_buffer_size = args["F"]
    calc_stats = args["stats"]
    do_not_delete_db = args["no_del"]
    
    if alignment_buffer_size is None or int(alignment_buffer_size) < 0:
        alignment_buffer_size = RECORDS_IN_BUFFER
    else:
        alignment_buffer_size = int(alignment_buffer_size)
    if fastq_buffer_size is None or int(fastq_buffer_size) < 0:
        fastq_buffer_size = RECORDS_IN_BUFFER_FASTQ
    else:
        fastq_buffer_size = int(fastq_buffer_size)


    read_processor = ReadProcessor(out_dir, db_file, minimumSampleAssociationThreshold)

    if cmd == "build":
        read_processor.read_and_store(alignment_buffer_size)
    elif cmd == "process":
        read_processor.process_db(calc_stats)
    elif cmd == "retrieve":
        read_processor.retrieve(args["1"], args["2"], fastq_buffer_size, do_not_delete_db)
    else:
        raise Exception("ERROR: unknown command provided: ", cmd)


