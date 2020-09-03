"""read_processor.py: provides methods for commands to run on FASTQ files returned by BD Rhapsody pipeline"""

__author__ = "Jan Hapala"
__version__ = "1.0.0"
__maintainer__ = "Jan Hapala"
__email__ = "jan@hapala.cz"
__status__ = "Production"

import os, sys
from collections import defaultdict
#from tabulate import tabulate
import gzip
from .read_storage import SQLReadStorage, DatabaseException, get_timestamp

LINE_NR_PRINT = 1000000

class NotReadIDException(Exception):
    pass

class ReadProcessorException(Exception):
    pass


def crop_read_id(read_id):
    """Cuts off prefix of the read ID until the second ":"
    """
    cid = read_id.split(":", maxsplit=2)[2]
    return(cid)


class SeqRead:
    """Structure for keeping a DNA read record
    """

    record = ""
    line_num = 0

    def __init__(self, line_num, record):
        self.record = record
        self.line_num = line_num
    
    @property
    def cid(self):
        return crop_read_id(self.id)

    @property
    def id(self):
        if not self.record.startswith(b"@"):
            record_prefix = f"{str(self.record[:min(50, len(self.record))])}..." 
            raise NotReadIDException("Error: Read ID was not found in the line:", record_prefix) 
        read_id = self.record.split(maxsplit=1)[0][1:].decode("ascii").strip()
        #read_id = self.record.split(maxsplit=1)[0][1:]
        return(read_id)


class ReadProcessor:
    """Extracts information from BAM file, builds a database; with this database 
            splits the FASTQ reads into separate files by sample tag
    """

    storage = None
    read_sample_lines_dict = defaultdict(list)
    output_files = {}
    out_dir = ""
    minimumSampleAssociationThreshold = None

    def __init__(self, out_dir, db_file, threshold = None):
        self.out_dir = out_dir
        self.storage = SQLReadStorage(db_file)
        self.minimumSampleAssociationThreshold = threshold

    def open_new_files(self, sample):
        """Open pair read output FASTQ files for the specified sample"""
        out_file1 = gzip.open(self.out_dir + "/" + sample + "_reads1.fastq.gz", 'wb')
        out_file2 = gzip.open(self.out_dir + "/" + sample + "_reads2.fastq.gz", 'wb')
        self.output_files[sample] = (out_file1, out_file2)
    
    def get_output_files(self, sample):
        """Return output file corresponding to the argument; open a new file if not existing"""
        if sample not in self.output_files:
            self.open_new_files(sample)
        return(self.output_files[sample])
    
    def close_output_files(self):
        """Close the pair read FASTQ output files"""
        for (of1, of2) in self.output_files.values():
            of1.close()
            of2.close()
    
    def retrieve(self, reads1_file, reads2_file, fastq_records_buffer_size, do_not_delete_db=False):
        """Read the input pair read FASTQ files and split reads with the
            information collected in the temporary database, built from the 
            corresponding BAM files
        """
        try:
            self._retrieve(reads1_file, reads2_file, fastq_records_buffer_size, do_not_delete_db)
        except Exception as e:
            raise e
        finally:
            self.close_output_files()
            if do_not_delete_db:
                print(f"{get_timestamp()}    Temporary database left. Should be deleted.")
            else:
                self.cleanup()

    def _retrieve(self, reads1_file, reads2_file, fastq_records_buffer_size, do_not_delete_db):
        """Read the input pair read FASTQ files and split reads with the
            information collected in the temporary database, built from the 
            corresponding BAM files
        """
        reads_buffer = dict()
    
        read1, read2 = None, None
        with gzip.open(reads1_file, 'rb') as f1, gzip.open(reads2_file, 'rb') as f2:
    
            for line_counter, (r1_line, r2_line) in enumerate(zip(f1, f2)):
                if line_counter % 4 == 0:
                    try:
                        if read1 is None: ## starting the first read in the file
                            read1, read2 = SeqRead(line_counter, r1_line), SeqRead(line_counter, r2_line)
                            if read1.id != read2.id:
                                raise ReadProcessorException(
                                    f"""ERROR: The fastq files are not organized in the same 
                                        order, line {str(line_counter)}, read1 ID: {read1.id.decode("ascii")}, 
                                        read2 ID: {read2.id.decode("ascii")}.""")
                        else: ## starting a new read, process the previous one
                            self._add_previous_read(reads_buffer, read1, read2)
                            if len(reads_buffer) >= fastq_records_buffer_size:
                                self._process_buffer(reads_buffer)
                            read1, read2 = SeqRead(line_counter, r1_line), SeqRead(line_counter, r2_line)
                    except NotReadIDException:
                        raise ReadProcessorException(
                            f"""ERROR: Problem encountered while reading the FASTQ files,
                            read ID expected, not found: line {line_counter},
                            read1: {read1.record.decode("ascii")},\n\t\t\t\t
                            read2: {read2.record.decode("ascii")}.""")

                    if (line_counter / 4) % LINE_NR_PRINT == 0 and line_counter > 0:
                        print(f"{get_timestamp()}   Reading read nr. { '{:,}'.format(int(line_counter / 4) ) }...", flush=True)

                else: # add to the current read
                    read1.record += r1_line
                    read2.record += r2_line
            ## process last reads
            if line_counter > 0:
                print(f"{get_timestamp()}    Read { '{:,}'.format(int(line_counter / 4) ) } reads altogether...", flush=True)
            if line_counter < 1:
                raise ReadProcessorException("Input FASTQ file(s) is/are empty.")
            self._add_previous_read(reads_buffer, read1, read2)
            self._process_buffer(reads_buffer)
    
    def read_and_store(self, records_buffer_size):
        """Read input BAM file, extract read IDs and corresponding cell IDs and sample tag
            and store them in a database.
        """
        records = []
    
        try:
            self.storage.setup()
            for line_counter, line in enumerate(sys.stdin):
                line = line.rstrip()
                self._store(line, records)
                if line_counter % LINE_NR_PRINT == 0 and line_counter > 0:
                    print(f"{get_timestamp()}   { '{:,}'.format(line_counter) } reads collected.", flush=True)

                if line_counter >= records_buffer_size:
                    self.storage.store(records)

            print(f"{get_timestamp()}    { '{:,}'.format(line_counter) } reads collected.")
            print(f"{get_timestamp()}    Saving reads to a temporary local database.", flush=True)
            self.storage.store(records)
        except Exception as e:
            raise ReadProcessorException("Error while writing to the database.", e)
        else:
            pass
            self.storage.commit()
            #self.storage.create_indexes()
        finally:
            self.storage.close()
    
    def process_db(self, calc_stats):
        """Process the information stored in the database,
            reads from a cell (sharing the cell ID) might be assigned different sample tags,
            thus assign to each cell ID exactly one sample tag, using a threshold,
            produce final read ID -- sample tag association table.
        """
        self.storage.process_data(self.minimumSampleAssociationThreshold)
        total_cell_count, cell_count_per_sample = None, None
        if calc_stats:
            total_cell_count = self.storage.get_total_cell_count()
            cell_count_per_sample = self.storage.get_cell_count_per_sample()
            print("""\n\t\t\t**************************************************
                    \t**************  STATISTICAL REPORT  **************
                    \t**************************************************""")
            print(f"\t\t\tCell number per sample:")
            sample_cells_tab_string = "\n\t\t\t%15s%15s" % ("SAMPLE |", "COUNT") + "\n"
            sample_cells_tab_string += "\t\t\t" + "-" * 30 + "\n"
            sample_cells_tab_string += "\n".join([ "\t\t\t%15s%15s" % 
                                                       (sample +": |", "{:,d}".format(count)) 
                                                       for sample, count in cell_count_per_sample ])
            sample_cells_tab_string += "\n\t\t\t" + "-" * 30
            sample_cells_tab_string += "\n\t\t\t%15s%15s" % ("total: |", "{:,d}".format(total_cell_count))
            print(f"{sample_cells_tab_string}\n")
            print("""\t\t\t**************************************************
            \t\t**************************************************
            \t\t**************************************************\n""", flush=True)
        self.storage.cleanup()
        self.storage.close()
        return(total_cell_count, cell_count_per_sample)

    def cleanup(self):
        """Remove the temporary database"""
        print(f"{get_timestamp()}    Deleting temporary files.")
        self.storage.remove_db()

    def _write_reads(self, reads_buffer, id_sample_pairs):
        """Write reads in the buffer into the correct output files"""
        reads_to_write = {1: defaultdict(list), 2: defaultdict(list)}

        for read_id in reads_buffer.keys():
            read1, read2 = reads_buffer[read_id]
            sample = "UNDETERMINED"
            if read_id in id_sample_pairs:
                sample = id_sample_pairs[read_id]
            reads_to_write[1][sample].append(read1.record)
            reads_to_write[2][sample].append(read2.record)

        blocks_to_write = {}
        blocks_to_write[1] = { key : b"".join(val) for key, val in reads_to_write[1].items() }
        blocks_to_write[2] = { key : b"".join(val) for key, val in reads_to_write[2].items() }
        
        for sample in blocks_to_write[1].keys():
            ofile1, ofile2 = self.get_output_files(sample)
            ofile1.write(blocks_to_write[1][sample])
            ofile2.write(blocks_to_write[2][sample])

    def _add_previous_read(self, reads_buffer, read1, read2):
        """Upon finishing reading a read, add it to the buffer."""
        cropped_id = read1.cid
        reads_buffer[cropped_id] = (read1, read2)

    def _process_buffer(self, reads_buffer):
        """Find in the database sample corresponding for each read in the buffer"""
        keys = reads_buffer.keys()
        id_sample_pairs = self.storage.get_multiple_read_sample_pairs(keys)
        self._write_reads(reads_buffer, id_sample_pairs)
        reads_buffer.clear()
        
    def _store(self, line, records):
        """Read tags from the BAM file and puts them in the buffer"""
        read_id, cell_id, sample_name = line.split()
        if sample_name == "x":
            ## UNDETERMINED case
            ## ignore
            return
        crid = crop_read_id(read_id)
        records.append((crid, cell_id, sample_name))
    
