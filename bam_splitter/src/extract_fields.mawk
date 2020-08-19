{
    read_id=$1
    for (i = 1; i <= NF; i++) {
        if (index($i, "CB:Z:")) cell_id=substr($i,6)
        else if (index($i, "ST:Z:")) sample_id=substr($i,6)
    }
    print read_id,cell_id,sample_id
}
