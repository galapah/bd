#!/usr/bin/python3

import os, sys
import sqlite3

DB_FILENAME = ".bamsplitter.db"
RECORDS_IN_BUFFER = 10000000

class ReadIDsKeeper:

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

class SQLReadIDsKeeper(ReadIDsKeeper):

    cursor = None

    def __init__(self, file_name):
        self.connection = sqlite3.connect(file_name)
        self.cursor = self.connection.cursor()

    def init(self):
        # Create table
        self.cursor.execute("CREATE TABLE reads ( read_id text PRIMARY_KEY, cell_id text NOT NULL );")
        # Create index on the read_id column
        #self.cursor.execute("CREATE UNIQUE INDEX idx_reads ON reads(read_id);")

    def get(self, key):
        self.cursor.execute(f"SELECT read_id, cell_id FROM reads WHERE read_id='{key}';")
        tupl = self.cursor.fetchall()[0]
        return(tupl)

    def get_multiple(self, key_list):
        keys_string = "|".join([ k in key_list ])
        print(keys_string[:-1])
        self.cursor.execute(f"SELECT read_id, cell_id FROM reads WHERE read_id IN '{keys_string}';")
        result = self.cursor.fetchall()
        return(result)

    def store(self, records):
        self.cursor.executemany('INSERT INTO reads VALUES(?,?);', records);
        last_rec = records[-1]
        records.clear()
        cell_id = self.get(last_rec[0])
        print(f"last recorded cell_id: {cell_id}, last_rec: {last_rec}")

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()


def retrieve():
    storage = SQLReadIDsKeeper(DB_FILENAME)
    print("IMPLEMENT ME!!!")

def _store(line, records):
    read_id, cell_id = line.split()
    records.append((read_id, cell_id))
    #print(records)

def read_and_store(first_line):
    if os.path.exists(DB_FILENAME):
        os.remove(DB_FILENAME)
    storage = SQLReadIDsKeeper(DB_FILENAME)
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


if __name__ == "__main__":
    ## catch the first line - call retrieval or storage
    line = sys.stdin.readline()
    line = line.rstrip()
    if line.lower().startswith("retrieve"):
        retrieve()
    else:
        read_and_store(line)

