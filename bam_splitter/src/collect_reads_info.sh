#!/bin/bash
##
## Collects the tags CN, CB and ST from teh BAM file into one file
##	and the list of read IDs and corresponding cell IDs (CB) into a database
##

## TO RUN:
# this script:
#  time ./collect_reads_info.sh -i bam/original/Combined_A_EKDL200001649-1a_H33W7DSXY_final.BAM -d output
# checking the current size of the output+tmp directory
#   while true; do du -sh /mnt/documents/output/.tmp ; sleep 60; done

INPUT_BAM=""
INPUT_FASTQ_R1=""
INPUT_FASTQ_R2=""
OUTPUT_DIR="./"
#NR_LINES_PRINT=100
NR_LINES_PRINT=1000000
READ_ID_NR_CHARS_TO_IGNORE=21
OUTPUT_FILE_TAGS_TAB=".tags_tab.csv"
DB_FILENAME=".bamsplitter.db"

## process arguments
while getopts "hi:d:" opt; do
  case ${opt} in
    d )
      OUTPUT_DIR=${OPTARG}
      ;;
    i )
      INPUT_BAM=${OPTARG}
      ;;
    h )
      echo "Usage: ./collect_reads_info.sh -i PATH_TO_BAM_FILE [-d OUTPUT_DIRECTORY]"
      echo "  EXAMPLE: ./collect_reads_info.sh -i bam_folder/Combined_BD-Demo-WTA-SMK_final.BAM -d results"
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

## initialize output file
OUTPUT_FILE_TAGS_TAB="${OUTPUT_DIR}/${OUTPUT_FILE_TAGS_TAB}"
DB_FILENAME="${OUTPUT_DIR}/${DB_FILENAME}"
echo -e "cell_ID\tsample_tag" > ${OUTPUT_FILE_TAGS_TAB}

cd ${OUTPUT_DIR}
conda activate bamsplitter

## go through the BAM file and pass down reads from genuine cells (CN tag T[rue])
##  and extract read_id, cell_id and sample_name
samtools view ${INPUT_BAM} | cut -c ${READ_ID_NR_CHARS_TO_IGNORE}- | grep ".*CN:Z:T.*" | \
	mawk -f ${WORK_DIR}/extract_fields.mawk | ${WORK_DIR}/read_keeper.py create ${DB_FILENAME} | \
	## pass the output to three commands in parallel:
        ## 1) extract cell_id and sample_name and collapse repetitive entries
	## 2) extract read_id (ignore leading characters common to all reads)
	##      and cell_id
 	## 3) show progress on the STDOUT \
	LC_ALL=en_US.UTF-8 awk -v line_counter=${NR_LINES_PRINT} \
	   '{if (NR % line_counter == 0) printf("%'"'"'d reads collected\n", NR)} \
	      END {printf("%'"'"'d lines processed\n", NR) }'

echo "Processing the DB..."
echo "process" | ${WORK_DIR}/read_keeper.py


#read fastqs... | cat <(echo "retrieve ") - | ${WORK_DIR}/read_keeper.py
echo "retrieve" | ${WORK_DIR}/read_keeper.py -1 fff -2 ggg

