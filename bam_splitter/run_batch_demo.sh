#!/bin/bash

script /mnt/documents/demoproj/run_batch_demo_million.log

rm /mnt/documents/demoproj/output_test/*.fastq.gz
./collect_reads_info.sh -i /mnt/documents/demoproj/demo.bam -d /mnt/documents/demoproj/output_test -1 /mnt/documents/demoproj/million_read1.fastq.gz -2 /mnt/documents/demoproj/million_read2.fastq.gz

rm /mnt/documents/demoproj/output_test/*.fastq.gz
echo "NOW HMILLION!!!"
./collect_reads_info.sh -i /mnt/documents/demoproj/demo.bam -d /mnt/documents/demoproj/output_test -1 /mnt/documents/demoproj/hmillion_read1.fastq.gz -2 /mnt/documents/demoproj/hmillion_read2.fastq.gz


exit


script /mnt/documents/demoproj/run_batch_demo_million_f10000.log

rm /mnt/documents/demoproj/output_test/*.fastq.gz
./collect_reads_info.sh -i /mnt/documents/demoproj/demo.bam -d /mnt/documents/demoproj/output_test -1 /mnt/documents/demoproj/million_read1.fastq.gz -2 /mnt/documents/demoproj/million_read2.fastq.gz


exit


sed -i "s/RECORDS_IN_BUFFER_FASTQ = 10000/RECORDS_IN_BUFFER_FASTQ = 100000/" main.py
script /mnt/documents/demoproj/run_batch_demo_million_f100000.log

rm /mnt/documents/demoproj/output_test/*.fastq.gz
./collect_reads_info.sh -i /mnt/documents/demoproj/demo.bam -d /mnt/documents/demoproj/output_test -1 /mnt/documents/demoproj/million_read1.fastq.gz -2 /mnt/documents/demoproj/million_read2.fastq.gz


exit

sed -i "s/RECORDS_IN_BUFFER_FASTQ = 100000/RECORDS_IN_BUFFER_FASTQ = 1000000/" main.py
script /mnt/documents/demoproj/run_batch_demo_million_f1000000.log

rm /mnt/documents/demoproj/output_test/*.fastq.gz
./collect_reads_info.sh -i /mnt/documents/demoproj/demo.bam -d /mnt/documents/demoproj/output_test -1 /mnt/documents/demoproj/million_read1.fastq.gz -2 /mnt/documents/demoproj/million_read2.fastq.gz


exit


script /mnt/documents/demoproj/run_batch_demo_hmillion_2,5nmil.log

rm /mnt/documents/demoproj/output_test/*.fastq.gz
./collect_reads_info.sh -i /mnt/documents/demoproj/demo.bam -d /mnt/documents/demoproj/output_test -1 /mnt/documents/demoproj/hmillion_read1.fastq.gz -2 /mnt/documents/demoproj/hmillion_read2.fastq.gz
exit

sed -i "s/RECORDS_IN_BUFFER_FASTQ = 10000000/RECORDS_IN_BUFFER_FASTQ = 100000000/" main.py
script /mnt/documents/demoproj/run_batch_demo_hmillion_fhmil.log

rm /mnt/documents/demoproj/output_test/*.fastq.gz
./collect_reads_info.sh -i /mnt/documents/demoproj/demo.bam -d /mnt/documents/demoproj/output_test -1 /mnt/documents/demoproj/hmillion_read1.fastq.gz -2 /mnt/documents/demoproj/hmillion_read2.fastq.gz
exit










