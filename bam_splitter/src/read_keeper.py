#!/usr/bin/python3

import os, sys
import sqlite3
#from Bio import SeqIO

DB_FILENAME = ".bamsplitter.db"
RECORDS_IN_BUFFER = 10000000
RECORDS_IN_BUFFER_FASTQ = 1000

class SeqRead:
    record = ""

    def __init__(self, record):
        self.record = record

    def get_id(self):
        self.record.split()[0][1:]

class ReadKeeper:

    connection = None

    def __init__(self):
        pass

    def init(self):
        pass

    def get(self, key):
        pass

    def store(self, records):
        pass

    def commit(self):
        pass

    def close(self):
        pass

class SQLReadKeeper(ReadKeeper):

    cursor = None

    def __init__(self, file_name):
        self.connection = sqlite3.connect(file_name)
        self.cursor = self.connection.cursor()

    def init(self):
        # Create table
        self.cursor.execute("""CREATE TABLE reads ( read_id text PRIMARY_KEY, 
                                                    cell_id text NOT NULL,
                                                    sample_name text NOT NULL );""")
        self.cursor.execute("CREATE INDEX idx_cell_id ON reads(cell_id);")
        self.cursor.execute("CREATE INDEX idx_sample_name ON reads(sample_name);")
        self.commit()

    def calculate_stats_on_cells(self):
        """
        requires: reads TABLE filled with all values (self.store() )
        """
        self.cursor.execute("""CREATE TABLE cells_stat AS 
                                SELECT cell_id, sample_name, COUNT(*) abundance 
                                FROM reads GROUP BY cell_id, sample_name;""")
        self.commit()
    
    def assign_cells_to_samples(self):
        """
        requires: cells_stat TABLE (self.calculate_stats_on_cells() )
        """
        self.cursor.execute("""CREATE TABLE cells AS
                                    SELECT  cell_id,
                                            CASE
                                                WHEN max_abd >= sum_abd*0.75 THEN sample_name
                                                ELSE NULL
                                                END sample FROM
                                                (SELECT cell_id, sample_name,
                                                        MAX(abundance) max_abd, SUM(abundance) sum_abd
                                                        FROM cells_stat GROUP BY cell_id); """)
        self.commit()

    def cleanup(self):
        self.cursor.execute("DROP TABLE reads;")
        self.cursor.execute("DROP TABLE cells;")
        self.cursor.execute("DROP TABLE cells_stat;")
        self.commit()

    def create_final_table(self):
        self.cursor.execute("""CREATE TABLE read_sample_pairs AS
                                    SELECT read_id, sample FROM
                                        (SELECT read_id, reads.cell_id, sample FROM reads
                                            LEFT JOIN cells ON reads.cell_id = cells.cell_id);""")
        self.commit()


    #def get(self, key):
    #    self.cursor.execute(f"SELECT read_id, cell_id, sample_name FROM reads WHERE read_id='{key}';")
    #    tupl = self.cursor.fetchall()[0]
    #    return(tupl)

    def get_multiple_read_sample_pairs(self, key_list):
        return self.get_multiple("reads", ["read_id", "sample"], "read_id", key_list)

    def get_multiple(self, table, field_list, lookup_field, key_list):
        fields_string = ",".join([ field for field in field_list ])
        keys_string = ",".join("?"*len(key_list))

        self.cursor.execute(f"""SELECT {fields_string} FROM {table} 
                                WHERE {lookup_field} IN {keys_string};""", key_list)
        result = self.cursor.fetchall()
        return(result)

    def store(self, records):
        self.cursor.executemany('INSERT INTO reads VALUES(?,?,?);', records);
        
        last_rec = records[-1]
        records.clear()
        cell_id = self.get(last_rec[0])
        print(f"last recorded cell_id: {cell_id}, last_rec: {last_rec}")

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()


def retrieve(db_file, reads1_file, reads2_file, read_id_leading_chars_ignore):
    import gzip
    import os
    #print(os.environ['CONDA_DEFAULT_ENV'])
    #_ = os.system("conda env list | grep '*'")
    storage = SQLReadKeeper(db_file)
    reads_buffer = dict()

    read1, read2 = None, None
    with gzip.open(reads1_file, 'rt') as f1, gzip.open(reads2_file, 'rt') as f2:
        for r1_line, r2_line in zip(f1, f2):
            if r1_line.startswith("@"):
                if read1 is None: ## starting the first read in the file
                    read1, read2 = SeqRead(r1_line), SeqRead(r2_line)
                else: ## starting a new read, process the previous one
                    reads_buffer[read1.get_id()] = (read1, read2)
                    if len(reads_buffer) >= RECORDS_IN_BUFFER_FASTQ:
                        _process_buffer(reads_buffer)
                    read1, read2 = SeqRead(r1_line), SeqRead(r2_line)
            else: # add to the current read
                read1.record += r1_line
                read2.record += r2_line

def _process_buffer(reads_buffer):
    pass
    

def _new_read(r1_line, r2_line):
    read1_id = r1_line.split()[0][1:]
    read2_id = r2_line.split()[0][1:]
    return (read1_id, read2_id)
            
       # for record1, record2 in zip(SeqIO.parse(f1, "fastq"), SeqIO.parse(f2, "fastq")):
       #     read1_id = record1.id[read_id_leading_chars_ignore:]
       #     read2_id = record2.id[read_id_leading_chars_ignore:]
       #     print(read1_id, read2_id)
       #     print(record1.letter_annotations)

def _store(line, records):
    read_id, cell_id, sample_name = line.split()
    records.append((read_id, cell_id, sample_name))
    #print(records)

def read_and_store(db_file):
    storage = SQLReadKeeper(db_file)
    if os.path.exists(DB_FILENAME):
        os.remove(DB_FILENAME)
    storage = SQLReadKeeper(DB_FILENAME)
    records = []

    try:
        storage.init()
        _store(first_line, records)
        for line in sys.stdin:
            line = line.rstrip()
            _store(line, records)
            if len(records) >= RECORDS_IN_BUFFER:
                storage.store(records)
        storage.store(records)
    except Exception as e:
        print("Error while writing to the database.", e, file=sys.stderr)
    else:
        storage.commit()
    finally:
        storage.close()

def process_db(db_file):
    storage = SQLReadKeeper(db_file)
    print("stats")
    storage.calculate_stats_on_cells()
    print("assign")
    storage.assign_cells_to_samples()
    print("final")
    storage.create_final_table()
    print("cleanup")
    storage.cleanup()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('cmd', help='command to execute', choices=['build', 'process', 'read'])
    parser.add_argument('db_file', help='path to the database filename')
    parser.add_argument('-1', help='path to reads1 fastq file')
    parser.add_argument('-2', help='path to reads2 fastq file')
    parser.add_argument('-i', help='number of leading chars in the read ID to ignore')
    args = parser.parse_args()
    args = args.__dict__
    cmd = args["cmd"]
    db_file = args["db_file"]

    if cmd == "create":
        read_and_store(db_file)
    elif cmd == "process":
        process_db(db_file)
    elif cmd == "read":
        retrieve(db_file, args["1"], args["2"], int(args["i"]))
    else:
        raise Exception("ERROR: unknown command provided: ", cmd)
    sys.exit()


