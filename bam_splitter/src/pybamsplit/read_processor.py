# ./main.py process ../output_test/.bamsplitter.db
# ./main.py -1 ../bam/test_read1.fastq.gz -2 ../bam/test_read2.fastq.gz "read" ../output_test/.bamsplitter.db

import os, sys
from collections import defaultdict
import gzip
from .read_storage import SQLReadStorage, DatabaseException, get_timestamp

LINE_NR_PRINT = 1000000

class NotReadIDException(Exception):
    pass

class ReadProcessorException(Exception):
    pass


def crop_read_id(read_id):
    cid = read_id.split(":", maxsplit=2)[2]
    return(cid)


class SeqRead:
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

    storage = None
    read_sample_lines_dict = defaultdict(list)
    out_dir = ""
    minimumSampleAssociationThreshold = None

    def __init__(self, out_dir, db_file, threshold = None):
        self.out_dir = out_dir
        self.storage = SQLReadStorage(db_file)
        self.minimumSampleAssociationThreshold = threshold

    def _create_read_pair(self, line_counter, r1_line, r2_line):
        read1, read2 = SeqRead(line_counter, r1_line), SeqRead(line_counter, r2_line)
        if read1.id != read2.id:
            raise ReadProcessorException(
                f"""ERROR: The fastq files are not organized in the same 
                       order, line {str(line_counter)}, read1 ID: {read1.id.decode("ascii")}, 
                       read2 ID: {read2.id.decode("ascii")}.""")
        return(read1, read2)

    def retrieve(self, reads1_file, reads2_file, fastq_records_buffer_size):
        reads_buffer = dict()
    
        read1, read2 = None, None
        with gzip.open(reads1_file, 'rb') as f1, gzip.open(reads2_file, 'rb') as f2:
    
            for line_counter, (r1_line, r2_line) in enumerate(zip(f1, f2)):
                if line_counter % 4 == 0:
                    try:
                        if read1 is None: ## starting the first read in the file
                            read1, read2 = self._create_read_pair(line_counter, r1_line, r2_line)
                        else: ## starting a new read, process the previous one
                            self._add_previous_read(reads_buffer, read1)
                            if len(reads_buffer) >= fastq_records_buffer_size:
                                self._process_buffer(reads_buffer)
                            read1, read2 = self._create_read_pair(line_counter, r1_line, r2_line)
                    except NotReadIDException:
                        raise ReadProcessorException(
                            f"""ERROR: Problem encountered while reading the FASTQ files,
                            read ID expected, not found: line {line_counter},
                            read1: {read1.record.decode("ascii")},\n\t\t\t\t
                            read2: {read2.record.decode("ascii")}.""")

                    if (line_counter / 4) % LINE_NR_PRINT == 0 and line_counter > 0:
                        print(f"{get_timestamp()}   Reading read nr. { '{:,}'.format(int(line_counter / 4) ) }...")

                else: # skip the rest of the read record
                    continue

            ## process last reads
            if line_counter < 1:
                raise ReadProcessorException("Input FASTQ file(s) is/are empty.")
            else:
                print(f"{get_timestamp()}   Read { '{:,}'.format(int(line_counter / 4) ) } reads altogether...")
            self._add_previous_read(reads_buffer, read1)
            self._process_buffer(reads_buffer)
            self._write_read_sample_lines(reads1_file)
            print("WARNING: NEED TO CLEAN UP!!!")
            #self.cleanup()
    
    def read_and_store(self, records_buffer_size):
        records = []
    
        try:
            self.storage.setup()
            for line_counter, line in enumerate(sys.stdin):
                line = line.rstrip()
                self._store(line, records)
                if line_counter % LINE_NR_PRINT == 0 and line_counter > 0:
                    print(f"{get_timestamp()}   { '{:,}'.format(line_counter) } reads collected.")

                if line_counter >= records_buffer_size:
                    self.storage.store(records)

            print(f"{get_timestamp()}    { '{:,}'.format(line_counter) } reads collected.")
            print("Saving reads to database.")
            self.storage.store(records)
        except Exception as e:
            raise ReadProcessorException("Error while writing to the database.", e)
        else:
            self.storage.commit()
            self.storage.create_indexes()
        finally:
            self.storage.close()
    
    def process_db(self):
        self.storage.process_data(self.minimumSampleAssociationThreshold)
        self.storage.cleanup()
        self.storage.close()

    def cleanup(self):
        self.storage.remove_db()

    def _save_read_sample_lines(self, reads_buffer, id_sample_pairs):
        def get_sample(read_id):
            sample = "UNDETERMINED"
            if read_id in id_sample_pairs:
                sample = id_sample_pairs[read_id]
            return(sample)
        for read_id, read in reads_buffer.items():
            self.read_sample_lines_dict[get_sample(read_id)].append(read.line_num)
        #tuples = [ (get_sample(read_id), read.line_num) for read_id, read in reads_buffer ]

    def _write_read_sample_lines(self, reads1_file):
        file_name = os.path.basename(reads1_file)
        for sample in self.read_sample_lines_dict.keys():
            block_to_write = b"\n".join([ b'%d' % (line_num+1) for line_num in self.read_sample_lines_dict[sample] ])
            with open(self.out_dir + "/linelist_" + sample + ".txt", 'wb') as ofile:
                ofile.write(block_to_write)

    def _add_previous_read(self, reads_buffer, read):
        cropped_id = read.cid
        reads_buffer[cropped_id] = read

    def _process_buffer(self, reads_buffer):
        keys = reads_buffer.keys()
        id_sample_pairs = self.storage.get_multiple_read_sample_pairs(keys)
        self._save_read_sample_lines(reads_buffer, id_sample_pairs)
        reads_buffer.clear()
        
    def _store(self, line, records):
        read_id, cell_id, sample_name = line.split()
        if sample_name == "x":
            ## UNDETERMINED case
            ## ignore
            return
            #sample_name = None
        crid = crop_read_id(read_id)
        records.append((crid, cell_id, sample_name))
    
