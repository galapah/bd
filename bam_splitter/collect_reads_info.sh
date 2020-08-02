#!/bin/bash
##
## Collects the tags CN, CB and ST from teh BAM file into one file
##	and the list of read IDs and corresponding cell IDs (CB)
##

INPUT_BAM=""
INPUT_FASTQ_R1=""
INPUT_FASTQ_R2=""
OUTPUT_DIR=""
OUTPUT_FILE_TAGS_TAB="tags_tab.csv"
OUTPUT_FILE_IDS_TAB="ids_tab.csv"

while getopts ":hijktd" opt; do
  case ${opt} in
    d )
      OUTPUT_DIR=${opt}
      ;;
    h )
      echo "Usage:"
      echo "    collect_reads_info.sh -h                      Display this help message."
      echo "    collect_reads_info.sh                 Install a Python package."
      exit 0
      ;;
    \? )
      echo "Invalid Option: -$OPTARG" 1>&2
      exit 1
      ;;
  esac
done
shift $((OPTIND -1))

OUTPUT_FILE_TAGS_TAB=${OUTPUT_DIR}/${OUTPUT_FILE_TAGS_TAB}
OUTPUT_FILE_IDS_TAB=${OUTPUT_DIR}/${OUTPUT_FILE_IDS_TAB}

samtools view ${INPUT_BAM} | grep -v "^@" | grep ".*CN:Z:T.*" | tee >(sed "s#.*CB:Z:\([0-9]\+\)\s.*CN:Z:\([xT]\)\s.*ST:Z:\([A-Za-z0-9]\+\).*#\1\t\2\t\3#" > ${OUTPUT_DIR}/${OUTPUT_FILE_TAGS_TAB}) >(sed "s#^\([^[:space:]]\+\)\s\+.*CB:Z:\([0-9]\+\).*#\1\t\2#" > ${OUTPUT_DIR}/${OUTPUT_FILE_IDS_TAB}) >/dev/null
