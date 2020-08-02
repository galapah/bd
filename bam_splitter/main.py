#!python

import os, sys, getopt
from collections import Counter, defaultdict
import pandas as pd
import pysam


def process_arguments(argv, default_threshold):
    inputfile = ""
    outputfile = ""
    threshold = default_threshold
    try:
        opts, args = getopt.getopt(argv,"hi:o:t:",["ifile=","ofile="])
    except getopt.GetoptError:
        print("split_bam.py [-t threshold] -i <inputfile> -o <outputfile>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            print("split_bam.py [-t threshold] -i <inputfile> -o <outputfile>")
            sys.exit()
        elif opt == "-t":
            inputfile = arg
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg

    if inputfile is None or inputfile == "":
        inputfile = "bam/test2.bam"
    print(f"Input file is {inputfile}.")
    return(inputfile, outputfile, threshold)

def main(inputfile, threshold):
    samfile = pysam.AlignmentFile(inputfile, "rb")
    sample_info_dict = collect_sample_info(samfile)
    samfile.close()
    sample_stats = calculate_statistics(sample_info_dict, threshold)
    sample_stats = filter_by_threshold(sample_stats, threshold)
    #max_vals = stats_df.max(axis=1)
    print(sample_stats)
    return(sample_stats)

def filter_by_threshold(stats_df, threshold):
    max_vals = stats_df.max(axis=1)
    rows_with_maxval_above_threshold = stats_df[max_vals >= threshold]
    prevailing_sample_tags = rows_with_maxval_above_threshold.idxmax(axis=1)
    prevailing_sample_tags.rename("Prevailing_sample", inplace=True)
    stats_df = pd.concat([stats_df, prevailing_sample_tags], axis=1)
    ## undecided sample marked as "Multiple"
    stats_df.loc[pd.isnull(stats_df["Prevailing_sample"]), "Prevailing_sample"] = "Multiple"
    return(stats_df)

def calculate_statistics(sample_info_dict, threshold):
    sample_stats = { cell_id: Counter(samples) for cell_id, samples in sorted(sample_info_dict.items()) }
    stats_df = pd.DataFrame(sample_stats).T.sort_index()
    #stats_df = stats_df.T.sort_index()
    sum_series = stats_df.sum(axis=1)
    stats_df = stats_df.div(sum_series, axis=0)
    return(stats_df)

def collect_sample_info(samfile):
    sample_info_dict = defaultdict(list)
    cb_id = None
    st_id = None
    for read in samfile.fetch():
        try:
            putative_cell = read.get_tag("CN")
        except KeyError:
            pass
        else:
            if putative_cell == "T": #True
                try:
                    cb_id = read.get_tag("CB")
                except KeyError:
                    pass
                try:
                    st_id = read.get_tag("ST")
                except KeyError:
                    pass
            if cb_id is not None and st_id is not None:            
                sample_info_dict[cb_id].append(st_id)
    return(sample_info_dict)

if __name__ == "__main__":
    default_threshold = 0.75
    inputfile, outputfile, threshold = process_arguments(sys.argv[1:], default_threshold)
    statsfile, _ = os.path.splitext(outputfile)
    statsfile += ".stats.csv"
    print(statsfile)
    stats = main(inputfile, threshold)
    stats.to_csv(statsfile)
    