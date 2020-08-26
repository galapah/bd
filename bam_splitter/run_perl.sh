#!/bin/bash

function clean_out_dir
{
	rm -f ${OUTPUT_DIR}/*.fastq.gz
	#rm -f ${OUTPUT_DIR}/.bamsplitter.db
}

OUTPUT_DIR="/mnt/documents/demoproj2/output_dir/"
LOGFILE="/mnt/documents/demoproj2/run"


exec > ${LOGFILE}.test_splitting_PERL.log 2>&1
set -x

clean_out_dir
LIST_FILE="${OUTPUT_DIR}/linelist_thousand_UNDETERMINED.txt"
IN_FASTQ="/mnt/documents/demoproj2/bam/thousand_read1.fastq.gz"
OUT_FASTQ="${OUTPUT_DIR}/thousand_UNDETERMINED_read1.fastq.gz"
./linecp.perl ${LIST_FILE} ${IN_FASTQ} | gzip > ${OUT_FASTQ}

IN_FASTQ="/mnt/documents/demoproj2/bam/thousand_read2.fastq.gz"
OUT_FASTQ="${OUTPUT_DIR}/thousand_UNDETERMINED_read2.fastq.gz"
./linecp.perl ${LIST_FILE} ${IN_FASTQ} | gzip > ${OUT_FASTQ}


echo "Now testing two files in parallel"
clean_out_dir
IN_FASTQ="/mnt/documents/demoproj2/bam/thousand_read1.fastq.gz"
OUT_FASTQ="${OUTPUT_DIR}/thousand_UNDETERMINED_read1.fastq.gz"
./linecp.perl ${LIST_FILE} ${IN_FASTQ} | gzip > ${OUT_FASTQ} &

clean_out_dir
IN_FASTQ="/mnt/documents/demoproj2/bam/thousand_read2.fastq.gz"
OUT_FASTQ="${OUTPUT_DIR}/thousand_UNDETERMINED_read2.fastq.gz"
./linecp.perl ${LIST_FILE} ${IN_FASTQ} | gzip > ${OUT_FASTQ} 





clean_out_dir
LIST_FILE="${OUTPUT_DIR}/linelist_million_UNDETERMINED.txt"
IN_FASTQ="/mnt/documents/demoproj2/bam/million_read1.fastq.gz"
OUT_FASTQ="${OUTPUT_DIR}/million_UNDETERMINED_read1.fastq.gz"
./linecp.perl ${LIST_FILE} ${IN_FASTQ} | gzip > ${OUT_FASTQ} 

IN_FASTQ="/mnt/documents/demoproj2/bam/million_read2.fastq.gz"
OUT_FASTQ="${OUTPUT_DIR}/million_UNDETERMINED_read2.fastq.gz"
./linecp.perl ${LIST_FILE} ${IN_FASTQ} | gzip > ${OUT_FASTQ} 


echo "Now testing two files in parallel"
clean_out_dir
IN_FASTQ="/mnt/documents/demoproj2/bam/million_read1.fastq.gz"
OUT_FASTQ="${OUTPUT_DIR}/million_UNDETERMINED_read1.fastq.gz"
./linecp.perl ${LIST_FILE} ${IN_FASTQ} | gzip > ${OUT_FASTQ} &

IN_FASTQ="/mnt/documents/demoproj2/bam/million_read2.fastq.gz"
OUT_FASTQ="${OUTPUT_DIR}/million_UNDETERMINED_read2.fastq.gz"
./linecp.perl ${LIST_FILE} ${IN_FASTQ}| gzip >  ${OUT_FASTQ} 


##clean_out_dir
##LIST_FILE="${OUTPUT_DIR}/linelist_hmillion_UNDETERMINED.txt"
##IN_FASTQ="/mnt/documents/demoproj2/bam/million_read1.fastq.gz"
##OUT_FASTQ="${OUTPUT_DIR}/million_UNDETERMINED_read1.fastq.gz"
##./linecp.perl ${LIST_FILE} ${IN_FASTQ} | gzip > ${OUT_FASTQ} 
##
##IN_FASTQ="/mnt/documents/demoproj2/bam/million_read2.fastq.gz"
##OUT_FASTQ="${OUTPUT_DIR}/million_UNDETERMINED_read2.fastq.gz"
##./linecp.perl ${LIST_FILE} ${IN_FASTQ} | gzip > ${OUT_FASTQ} 
##
