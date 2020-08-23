#!/bin/bash
##
## Collects the tags CN, CB and ST from teh BAM file into one file
##	and the list of read IDs and corresponding cell IDs (CB) into a database
##

## TO RUN:
# this script:
# ./collect_reads_info.sh -i ../bam/test_bams/test_1000.bam -d ../output_test -1 ../bam/test_read1.fastq.gz -2 ../bam/test_read2.fastq.gz
#  time ./collect_reads_info.sh -i bam/original/Combined_A_EKDL200001649-1a_H33W7DSXY_final.BAM -d output
# checking the current size of the output+tmp directory
#   while true; do du -sh /mnt/documents/output/.tmp ; sleep 60; done

INPUT_BAM=""
OUTPUT_DIR="./"
NR_LINES_PRINT=1000000
READ_ID_NR_CHARS_TO_IGNORE=21
DB_FILENAME=".bamsplitter.db"

## process arguments
while getopts "hi:d:1:2:" opt; do
  case ${opt} in
    d )
      OUTPUT_DIR=${OPTARG}
      ;;
    i )
      INPUT_BAM=${OPTARG}
      ;;
    1 )
      INPUT_FASTQ_R1=${OPTARG}
      ;;
    2 )
      INPUT_FASTQ_R2=${OPTARG}
      ;;
    h )
      echo "Usage: ./collect_reads_info.sh -i PATH_TO_BAM_FILE -1 PATH_TO_FASTQ_R1 -2 PATH_TO_FASTQ_R1 [-d OUTPUT_DIRECTORY]"
      echo "  EXAMPLE: ./collect_reads_info.sh -i bam_folder/Combined_BD-Demo-WTA-SMK_final.BAM -1 fastq/reads1.fastq.gz -2 fastq/reads2.fastq.gz -d results"
      echo "    collect_reads_info.sh -h                  Display this help message."
      exit 0
      ;;
    \? )
      echo "Invalid Option: -$OPTARG" 1>&2
      exit 1
      ;;
    : )
      echo "Missing option argument for -$OPTARG" >&2
      exit 1
      ;;
  esac
done
shift $((OPTIND -1))

## do not allow missing input (BAM) file
if [[ -z $INPUT_BAM ]]; then
    echo "INPUT_BAM argument is missing."
    exit 1
  fi

WORK_DIR=`readlink -f ${PWD}`
INPUT_BAM=`readlink -f ${INPUT_BAM}`
OUTPUT_DIR=`readlink -f ${OUTPUT_DIR}`
mkdir -p ${OUTPUT_DIR}

## make sure any temporary file created by the OS will be stored in our output directory too,
##  assuming we have plenty of disk space there...
NEW_TEMP=${OUTPUT_DIR}/.tmp
mkdir -p ${NEW_TEMP}
export TMPDIR=${NEW_TEMP}

## interpret all characters as ANSI, speeds the run up
export LC_ALL=C

DB_FILENAME="${OUTPUT_DIR}/${DB_FILENAME}"

cd ${OUTPUT_DIR}
eval "$(conda shell.bash hook)"
conda activate bamsplitter

echo "Collecting information about the reads..."
## go through the BAM file and pass down reads from genuine cells (CN tag T[rue])
##  and extract read_id, cell_id and sample_name
samtools view ${INPUT_BAM} | cut -c $((${READ_ID_NR_CHARS_TO_IGNORE}+1))- | grep ".*CN:Z:T.*" | \
	mawk -f ${WORK_DIR}/extract_fields.mawk | tee >( ${WORK_DIR}/main.py build ${DB_FILENAME} ) | \
	## pass the output to three commands in parallel:
        ## 1) extract cell_id and sample_name and collapse repetitive entries
	## 2) extract read_id (ignore leading characters common to all reads)
	##      and cell_id
 	## 3) show progress on the STDOUT \
	LC_ALL=en_US.UTF-8 awk -v line_counter=${NR_LINES_PRINT} \
	   '{if (NR % line_counter == 0) printf("%'"'"'d reads collected\n", NR)} \
	      END {printf("%'"'"'d reads collected\n", NR) }'


echo "Processing the DB - deciding on the sample partitioning..."
${WORK_DIR}/main.py -i ${READ_ID_NR_CHARS_TO_IGNORE} "process" ${DB_FILENAME}

echo "Splitting the file..."
${WORK_DIR}/main.py -1 ${INPUT_FASTQ_R1} -2 ${INPUT_FASTQ_R2} -i ${READ_ID_NR_CHARS_TO_IGNORE} "read" ${DB_FILENAME}

echo "DONE"

rm -R ${NEW_TEMP}

