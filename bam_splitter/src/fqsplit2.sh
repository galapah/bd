#!/bin/bash
##
## Collects the tags CN, CB and ST from teh BAM file into one file
##	and the list of read IDs and corresponding cell IDs (CB) into a database
##

## TO RUN:
# this script:
# ./fqsplit.sh -i ../bam/test_bams/test_1000.bam -d ../output_test -1 ../bam/test_read1.fastq.gz -2 ../bam/test_read2.fastq.gz
#  time ./fqsplit.sh -i bam/original/Combined_A_EKDL200001649-1a_H33W7DSXY_final.BAM -d output


function timestamp
{
  date +"%Y-%m-%d_%H:%M:%S"
}

## assign default values to variables
INPUT_BAM=""
OUTPUT_DIR="./"
NR_LINES_PRINT=1000000
DB_FILENAME=".bamsplitter.db"
RECORDS_IN_BUFFER=10000000
RECORDS_IN_BUFFER_FASTQ=1000


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
      echo "Usage: ./fqsplit.sh -i PATH_TO_BAM_FILE -1 PATH_TO_FASTQ_R1 -2 PATH_TO_FASTQ_R1 [-d OUTPUT_DIRECTORY]"
      echo "  EXAMPLE: ./fqsplit.sh -i bam_folder/Combined_BD-Demo-WTA-SMK_final.BAM -1 fastq/reads1.fastq.gz -2 fastq/reads2.fastq.gz -d results"
      echo "    fqsplit.sh -h                  Display this help message."
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

if [ ! -f "$INPUT_BAM" ]; then
    echo "The input file $INPUT_BAM does not exist."
    exit 1
fi
if [ ! -f "$INPUT_FASTQ_R1" ]; then
    echo "The input file $INPUT_FASTQ_R1 does not exist."
    exit 1
fi
if [ ! -f "$INPUT_FASTQ_R2" ]; then
    echo "The input file $INPUT_FASTQ_R2 does not exist."
    exit 1
fi

echo "###########################################################################################"
echo "BAM_FILE: ${INPUT_BAM}"
echo "___________________________________________________________________________________________"

#exit 0


## make sure any temporary file created by the OS will be stored in our output directory too,
##  assuming we have plenty of disk space there...
NEW_TEMP=${OUTPUT_DIR}/.tmp
mkdir -p ${NEW_TEMP}
export TMPDIR=${NEW_TEMP}

## interpret all characters as ANSI, speeds the run up
export LC_ALL=C

DB_FILENAME="${OUTPUT_DIR}/${DB_FILENAME}"

cd ${OUTPUT_DIR}
#eval "$(conda shell.bash hook)"
#conda activate bamsplitter

#echo `timestamp`"    Collecting information about the reads..."
#### go through the BAM file and pass down reads from genuine cells (CN tag T[rue])
####  and extract read_id, cell_id and sample_name
#samtools view ${INPUT_BAM} | grep ".*CN:Z:T.*" | \
#	mawk -f ${WORK_DIR}/extract_fields.mawk | ${WORK_DIR}/main.py -d ${OUTPUT_DIR} build ${DB_FILENAME}
##
##exit 0
##echo "size of the output dir: " `du -sh ${OUTPUT_DIR} | cut -f1`
##
##
#echo `timestamp`"    Processing the DB - deciding on the sample partitioning..."
#${WORK_DIR}/main.py -d ${OUTPUT_DIR} "process" ${DB_FILENAME}
##echo "size of the output dir: " `du -sh ${OUTPUT_DIR} | cut -f1`

echo `timestamp`"    Splitting the file..."
${WORK_DIR}/main.py -1 ${INPUT_FASTQ_R1} -2 ${INPUT_FASTQ_R2} -d ${OUTPUT_DIR} "retrieve" ${DB_FILENAME}

echo `timestamp`"    DONE"

rm -R ${NEW_TEMP}

