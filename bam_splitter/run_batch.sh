#!/bin/bash

script ../run2.log

rm -R ../output_test/
./collect_reads_info.sh -i ../bam/test_bams/test_1000.bam -d ../output_test -1 ../bam/test_read1.fastq.gz -2 ../bam/test_read2.fastq.gz

rm -R ../output_test/
./collect_reads_info.sh -i ../bam/test_bams/test_10000.bam -d ../output_test -1 ../bam/test_read1.fastq.gz -2 ../bam/test_read2.fastq.gz

rm -R ../output_test/
./collect_reads_info.sh -i ../bam/test_bams/test_100000.bam -d ../output_test -1 ../bam/test_read1.fastq.gz -2 ../bam/test_read2.fastq.gz

rm -R ../output_test/
./collect_reads_info.sh -i ../bam/test_bams/test_1000000.bam -d ../output_test -1 ../bam/test_read1.fastq.gz -2 ../bam/test_read2.fastq.gz

rm -R ../output_test/
./collect_reads_info.sh -i ../bam/test_bams/test_10000000.bam -d ../output_test -1 ../bam/test_read1.fastq.gz -2 ../bam/test_read2.fastq.gz


script /mnt/documents/demoproj/real_test_demodata__db-optim_cover-idx+real_pragma.log
rm /mnt/documents/demoproj/output_test/*.fastq.gz
./collect_reads_info.sh -i /mnt/documents/demoproj/demo.bam -d /mnt/documents/demoproj/output_test -1 /mnt/documents/demoproj/demo_read1.fastq.gz -2 /mnt/documents/demoproj/demo_read2.fastq.gz

exit

