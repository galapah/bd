#!/bin/bash
#
VERSION=1.0
#title           :fqsplit.sh
#description     :Splits reads from FASTQ files by sample tag, based on the corresponding BAM file
#author		 :Jan Hapala <jan@hapala.cz>
#date            :20200904
#usage		 :./fqsplit.sh -i PATH_TO_BAM -1 PATH_TO_FASTQ_R1 -2 PATH_TO_FASTQ_R2 [ -d OUTPUT_DIR ]
#notes           :Install samtools and python3 to use this script.
#bash		 :GNU bash, version 5.0.17(1)-release
#==============================================================================


function timestamp
{
  date +"%Y-%m-%d_%H:%M:%S"
}

function cleanup
{
	rm -R ${NEW_TEMP}
}


## assign default values to variables
INPUT_BAM=""
OUTPUT_DIR="./"
NR_LINES_PRINT=1000000
DB_FILENAME=".fastqslitter.db"
RECORDS_IN_BUFFER=10000000
RECORDS_IN_BUFFER_FASTQ=1000
CALC_STATS=0
DO_NOT_DELETE_DB=0
DELETE_DB=0


## process arguments
while getopts "b:d:1:2:hsvxX" opt; do
  case ${opt} in
    d )
      OUTPUT_DIR=${OPTARG}
      ;;
    b )
      INPUT_BAM=${OPTARG}
      ;;
    1 )
      INPUT_FASTQ_R1=${OPTARG}
      ;;
    2 )
      INPUT_FASTQ_R2=${OPTARG}
      ;;
    s )
      export CALC_STATS=1
      ;;
    x )
      export DO_NOT_DELETE_DB=1
      ;;
    X )
      export DELETE_DB=1
      ;;
    h )
      echo "Usage: ./fqsplit.sh -b PATH_TO_BAM_FILE -1 PATH_TO_FASTQ_R1 -2 PATH_TO_FASTQ_R1"
      echo ""
      echo "Optional arguments:"
      echo "-d		output directory"
      echo "-s		(no argument) Print reads statistics."
      echo "-x		(no argument) Do not delete temporary database."
      echo ""
      echo "-X		(no argument) Delete the temporary files."
      echo "-v		(no argument) Print program version."
      echo "-h          (no argument) Display this help message."
      exit 0
      ;;
    v )
      echo "fqsplit.sh version ${VERSION}"
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

OUTPUT_DIR=`readlink -f ${OUTPUT_DIR}`
DB_FILENAME="${OUTPUT_DIR}/${DB_FILENAME}"
if [ $DELETE_DB -eq 1 ]; then
      rm ${DB_FILENAME}
      if [ $? -eq 0 ]; then
	      echo "The temporary database has been deleted."
	else echo "ERROR: Could not delete the temporary database."
	fi
      exit 0
fi

## do not allow missing input (BAM) file
if [[ -z $INPUT_BAM ]]; then
    echo "INPUT_BAM argument is missing."
    exit 1
fi


WORK_DIR=`readlink -f ${PWD}`
INPUT_BAM=`readlink -f ${INPUT_BAM}`
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

## make sure any temporary file created by the OS will be stored in our output directory too,
##  assuming we have plenty of disk space there...
NEW_TEMP=${OUTPUT_DIR}/.tmp
mkdir -p ${NEW_TEMP}
export TMPDIR=${NEW_TEMP}

## interpret all characters as ANSI, speeds the run up
export LC_ALL=C



if [[ -f ${DB_FILENAME} ]]
then
	echo "ERROR: a database file already exists in the output directory."
	exit 1
fi

LOG_FILE=${OUTPUT_DIR}/fqsplit.log
exec &> >(tee -a "${LOG_FILE}")


echo "###########################################################################################"
echo "Starting fqsplit, a program for splitting FASTQ reads files from BD Rhapsody pipeline."
echo "-------------------------------------------------------------------------------------------"
#echo "___________________________________________________________________________________________"
## print input parameters
echo "INPUT BAM FILE: ${INPUT_BAM}"
echo "INPUT FASTQ READS 1 FILE: ${INPUT_FASTQ_R1}"
echo "INPUT FASTQ READS 2 FILE: ${INPUT_FASTQ_R2}"
echo "OUTPUT DIRECTORY: ${OUTPUT_DIR}"
echo "-------------------------------------------------------------------------------------------"
echo ""

echo `timestamp`"    Collecting information about the reads..."

### go through the BAM file and pass down reads from genuine cells (CN tag T[rue])
###  and extract read_id, cell_id and sample_name
samtools view ${INPUT_BAM} | grep ".*CN:Z:T.*" | mawk -f ${WORK_DIR}/extract_fields.mawk | \
		${WORK_DIR}/main.py -d ${OUTPUT_DIR} build ${DB_FILENAME} -f ${RECORDS_IN_BUFFER}
echo "                       Size of the output dir: " `du -sh ${OUTPUT_DIR} | cut -f1`
if [ $? -ne 0 ]; then echo "ERROR. Terminating."; cleanup; exit 1; fi

echo `timestamp`"    Processing the DB - deciding on the sample partitioning..."
OPT_PARAMS=""
if [ $CALC_STATS -gt 0 ]; then OPT_PARAMS="--stats"; fi
RUN_CMD="${WORK_DIR}/main.py -d ${OUTPUT_DIR} process ${DB_FILENAME} ${OPT_PARAMS}"
eval ${RUN_CMD}
echo "                       Size of the output dir: " `du -sh ${OUTPUT_DIR} | cut -f1`
if [ $? -ne 0 ]; then echo "ERROR. Terminating."; cleanup; exit 1; fi

echo `timestamp`"    Splitting the file..."
OPT_PARAMS=""
if [ $DO_NOT_DELETE_DB -gt 0 ]; then OPT_PARAMS="--no-del"; fi
${WORK_DIR}/main.py -1 ${INPUT_FASTQ_R1} -2 ${INPUT_FASTQ_R2} -d ${OUTPUT_DIR} "retrieve" \
	${DB_FILENAME} -F ${RECORDS_IN_BUFFER_FASTQ} ${OPT_PARAMS}
echo "                       Size of the output dir: " `du -sh ${OUTPUT_DIR} | cut -f1`

echo `timestamp`"    DONE"
cleanup

