#!/bin/bash

function clean_out_dir
{
	rm -f ${OUTPUT_DIR}/*.fastq.gz
	#rm -f ${OUTPUT_DIR}/.bamsplitter.db
}

OUTPUT_DIR="/mnt/documents/demoproj2/output_dir/"
LOGFILE="/mnt/documents/demoproj2/run"


exec > ${LOGFILE}.test_100mil.log 2>&1
set -x

clean_out_dir
INPUT_BAM="/mnt/documents/bd_bkp/bam_splitter/bam/test_100mil.bam"
FASTQ1="/mnt/documents/bd_bkp/bam_splitter/bam/test_read1.fastq.gz"
FASTQ2="/mnt/documents/bd_bkp/bam_splitter/bam/test_read1.fastq.gz"
echo "TEST WITH INDEXES"
./collect_reads_info.sh -i ${INPUT_BAM} -d ${OUTPUT_DIR} -1 ${FASTQ1} -2 ${FASTQ2}

clean_out_dir
echo "TEST WITHOUT INDEXES"
sed -i "s/self\.storage\.create_indexes/#self\.storage\.create_indexes/" /mnt/documents/bd/bam_splitter/src/pybamsplit/read_processor.py
./collect_reads_info.sh -i ${INPUT_BAM} -d ${OUTPUT_DIR} -1 ${FASTQ1} -2 ${FASTQ2}

