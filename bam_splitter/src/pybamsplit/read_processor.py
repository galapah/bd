# ./main.py process ../output_test/.bamsplitter.db
# ./main.py -1 ../bam/test_read1.fastq.gz -2 ../bam/test_read2.fastq.gz "read" ../output_test/.bamsplitter.db

import os, sys
from collections import defaultdict
import gzip
from .read_storage import SQLReadStorage, DatabaseException, get_timestamp


class NotReadIDException(Exception):
    pass

class ReadProcessorException(Exception):
    pass


class SeqRead:
    record = ""

    def __init__(self, record):
        self.record = record
    
    @property
    def id(self):
        if not self.record.startswith(b"@"):
            record_prefix = f"{str(self.record[:20])}..." 
            raise NotReadIDException("Error: Read ID was not found:", record_prefix) 
        read_id = self.record.split()[0][1:].decode("ascii").strip()
        return(read_id)


class ReadProcessor:

    storage = None
    output_files = {}
    out_dir = ""
    minimumSampleAssociationThreshold = None

    def __init__(self, out_dir, db_file, threshold = None):
        self.out_dir = out_dir
        self.storage = SQLReadStorage(db_file)
        self.minimumSampleAssociationThreshold = threshold

    def open_new_files(self, sample):
        out_file1 = gzip.open(self.out_dir + "/" + sample + "_reads1.fastq.gz", 'wb')
        out_file2 = gzip.open(self.out_dir + "/" + sample + "_reads2.fastq.gz", 'wb')
        self.output_files[sample] = (out_file1, out_file2)
    
    def get_output_files(self, sample):
        if sample not in self.output_files:
            self.open_new_files(sample)
        return(self.output_files[sample])
    
    def close_output_files(self):
        for (of1, of2) in self.output_files.values():
            of1.close()
            of2.close()
    
    def retrieve(self, reads1_file, reads2_file, read_id_leading_chars_ignore, 
                    fastq_records_buffer_size):
        reads_buffer = dict()
    
        read1, read2 = None, None
        with gzip.open(reads1_file, 'rb') as f1, gzip.open(reads2_file, 'rb') as f2:
    
            for line_counter, (r1_line, r2_line) in enumerate(zip(f1, f2)):
                if line_counter % 4 == 0:
                    try:
                        if read1 is None: ## starting the first read in the file
                            read1, read2 = SeqRead(r1_line), SeqRead(r2_line)
                            if read1.id != read2.id:
                                raise ReadProcessorException(
                                    f"""ERROR: The fastq files are not organized in the same 
                                        order, line {line_counter}, read1 ID: {read1.id}, 
                                        read2 ID: {read2.id}.""")
                        else: ## starting a new read, process the previous one
                            self._add_previous_read(reads_buffer, read1, read2, read_id_leading_chars_ignore)
                            if len(reads_buffer) >= fastq_records_buffer_size:
                                self._process_buffer(reads_buffer)
                            read1, read2 = SeqRead(r1_line), SeqRead(r2_line)
                    except NotReadIDException:
                        raise ReadProcessorException(
                            f"""ERROR: Problem encountered while reading the FASTQ files,
                            read ID expected, not found: line {line_counter},
                            read1: {read1.record.decode("ascii")},\n\t\t\t\t
                            read2: {read2.record.decode("ascii")}.""")

                    if (line_counter / 4) % 1000000 == 0 and line_counter > 0:
                        print(f"{get_timestamp()} Reading read nr. { '{:,}'.format(int(line_counter / 4) ) }...")

                else: # add to the current read
                    read1.record += r1_line
                    read2.record += r2_line
                ## process last reads
            if line_counter < 1:
                raise ReadProcessorException("Input FASTQ file(s) is/are empty.")
            self._add_previous_read(reads_buffer, read1, read2, read_id_leading_chars_ignore)
            self._process_buffer(reads_buffer)
            self.close_output_files()
            print("WARNING: NEED TO CLEAN UP!!!")
            #self.cleanup()
    
    def read_and_store(self, records_buffer_size):
        records = []
    
        try:
            self.storage.setup()
            for line in sys.stdin:
                line = line.rstrip()
                self._store(line, records)
                if len(records) >= records_buffer_size:
                    self.storage.store(records)
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

    def write_reads(self, reads_buffer, id_sample_pairs):
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

    def _add_previous_read(self, reads_buffer, read1, read2, read_id_leading_chars_ignore):
        cropped_id = read1.id[read_id_leading_chars_ignore:]
        reads_buffer[cropped_id] = (read1, read2)

    def _process_buffer(self, reads_buffer):
        keys = reads_buffer.keys()
        id_sample_pairs = self.storage.get_multiple_read_sample_pairs(keys)
        self.write_reads(reads_buffer, id_sample_pairs)
        reads_buffer.clear()
        
    def _new_read(self, r1_line, r2_line):
        read1_id = r1_line.split()[0][1:]
        read2_id = r2_line.split()[0][1:]
        return (read1_id, read2_id)
                
    def _store(self, line, records):
        read_id, cell_id, sample_name = line.split()
        records.append((read_id, cell_id, sample_name))
    
