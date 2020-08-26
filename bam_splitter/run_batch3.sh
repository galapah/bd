#!/bin/bash

function clean_out_dir
{
	rm -f ${OUTPUT_DIR}/*.fastq.gz
	#rm -f ${OUTPUT_DIR}/.bamsplitter.db
}

OUTPUT_DIR="/mnt/documents/demoproj2/output_dir/"
LOGFILE="/mnt/documents/demoproj2/run"


exec > ${LOGFILE}.test_splitting_helperfile.log 2>&1
set -x

clean_out_dir
INPUT_BAM="/mnt/documents/demoproj2/bam/demo.bam"
FASTQ1="/mnt/documents/demoproj2/bam/orig/thousand_read1.fastq.gz"
FASTQ2="/mnt/documents/demoproj2/bam/orig/thousand_read2.fastq.gz"
./collect_reads_info.sh -i ${INPUT_BAM} -d ${OUTPUT_DIR} -1 ${FASTQ1} -2 ${FASTQ2}

clean_out_dir
INPUT_BAM="/mnt/documents/demoproj2/bam/demo.bam"
FASTQ1="/mnt/documents/demoproj2/bam/million_read1.fastq.gz"
FASTQ2="/mnt/documents/demoproj2/bam/million_read2.fastq.gz"
./collect_reads_info.sh -i ${INPUT_BAM} -d ${OUTPUT_DIR} -1 ${FASTQ1} -2 ${FASTQ2}

clean_out_dir
INPUT_BAM="/mnt/documents/demoproj2/bam/demo.bam"
FASTQ1="/mnt/documents/demoproj2/bam/hmillion_read1.fastq.gz"
FASTQ2="/mnt/documents/demoproj2/bam/hmillion_read2.fastq.gz"
./collect_reads_info.sh -i ${INPUT_BAM} -d ${OUTPUT_DIR} -1 ${FASTQ1} -2 ${FASTQ2}


