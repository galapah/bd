#!/bin/bash
##
## Collects the tags CN, CB and ST from teh BAM file into one file
##	and the list of read IDs and corresponding cell IDs (CB)
##

INPUT_BAM=""
INPUT_FASTQ_R1=""
INPUT_FASTQ_R2=""
OUTPUT_DIR="./"
OUTPUT_FILE_TAGS_TAB="tags_tab.csv"
OUTPUT_FILE_IDS_TAB="ids_tab.csv"

#while getopts ":hijktd" opt; do
while getopts ":hi:d:" opt; do
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

if [[ -z $INPUT_BAM ]]; then
    echo "INPUT_BAM argument is missing."
    exit 1
  fi

OUTPUT_FILE_TAGS_TAB=${OUTPUT_DIR}/${OUTPUT_FILE_TAGS_TAB}
OUTPUT_FILE_IDS_TAB=${OUTPUT_DIR}/${OUTPUT_FILE_IDS_TAB}

## with extra middle column CN - unnecessary ???
#samtools view ${INPUT_BAM} | grep -v "^@" | grep ".*CN:Z:T.*" | tee >(sed "s#.*CB:Z:\([0-9]\+\)\s.*CN:Z:\([xT]\)\s.*ST:Z:\([A-Za-z0-9]\+\).*#\1\t\2\t\3#" > ${OUTPUT_DIR}/${OUTPUT_FILE_TAGS_TAB}) >(sed "s#^\([^[:space:]]\+\)\s\+.*CB:Z:\([0-9]\+\).*#\1\t\2#" > ${OUTPUT_DIR}/${OUTPUT_FILE_IDS_TAB}) >/dev/null

echo -e "cell_ID\tsample_tag" > ${OUTPUT_DIR}/${OUTPUT_FILE_TAGS_TAB}
echo -e "read_ID\tcell_ID" > ${OUTPUT_DIR}/${OUTPUT_FILE_IDS_TAB}
samtools view ${INPUT_BAM} | grep -v "^@" | grep ".*CN:Z:T.*" | tee >(sed "s#.*CB:Z:\([0-9]\+\)\s.*CN:Z:\([xT]\)\s.*ST:Z:\([A-Za-z0-9]\+\).*#\1\t\3#" | sort -k1,2 >> ${OUTPUT_DIR}/${OUTPUT_FILE_TAGS_TAB}) >(sed "s#^\([^[:space:]]\+\)\s\+.*CB:Z:\([0-9]\+\).*#\1\t\2#" | sort -k1,2 >> ${OUTPUT_DIR}/${OUTPUT_FILE_IDS_TAB}) >/dev/null

