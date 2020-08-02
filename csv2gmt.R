library(data.table)

in_dir = "~/projects/bd/"
in_filename = "Up-Regulated_phenograph_icellr__above-4.pound-delim.csv"
out_filename = "reference_gene_sets.gmt"

csv_filepath = paste0(in_dir, in_filename)
out_filepath = paste0(in_dir, out_filename)


tab = read.table(csv_filepath, sep = ";", header = T, 
      col.names = c("Cluster", "Symbol", "FoldCh", "qVal"), dec = ",")
tab.select = tab[c("Cluster", "Symbol")]
tab.grouped = setDT(tab.select)[, lapply(.SD, paste, collapse=",") , by = Cluster]
tab.grouped$CellType = sprintf("reference_cluster%02d", tab.grouped$Cluster)
tab.grouped$Cluster = tab.grouped$CellType

tab.grouped = tab.grouped[,c("Cluster", "CellType", "Symbol")]
write.table(tab.grouped[1:15,], out_filepath, sep = ",", row.names = F, col.names = F, quote = F)
