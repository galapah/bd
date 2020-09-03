"""read_storage.py: provides interface to the SQLite database"""

__author__ = "Jan Hapala"
__version__ = "1.0.0"
__maintainer__ = "Jan Hapala"
__email__ = "jan@hapala.cz"
__status__ = "Production"

import os
import sqlite3
from datetime import datetime


class DatabaseException(Exception):
    pass

def get_timestamp():
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%Y-%m-%d_%H:%M:%S")
    return(timestampStr)

class SQLReadStorage:
    """Interface for storing and retrieving the read information
        to/from the SQLite database.
    """

    connection = None
    cursor = None
    db_file_name = None 

    def __init__(self, file_name):
        self.db_file_name = file_name

    def _init_connect(self):
        """Initialize connection to the database,
            optimize the database.
        """
        if self.connection is None:
            try:
                self.connection = sqlite3.connect(self.db_file_name)
                #self.connection = apsw.Connection(self.db_file_name)
                self.cursor = self.connection.cursor()
                self.cursor.execute("PRAGMA synchronous = OFF;")
                self.cursor.execute("PRAGMA journal_mode = OFF;")
                self.cursor.execute("PRAGMA LOCKING_MODE = EXCLUSIVE;")
                self.cursor.execute("PRAGMA foreign_keys=off;")
            except Exception as e:
                raise DatabaseException("Error while connecting to the database.", e)

    def setup(self):
        """Create the initial table for collecting read information 
            from the BAM file.
        """
        if os.path.exists(self.db_file_name):
            os.remove(self.db_file_name)

        self._init_connect()
        try:
            self.cursor.execute("""CREATE TABLE reads ( read_id text NOT NULL, 
                                                        cell_id text NOT NULL,
                                                        sample_name text NOT NULL )
                                                         ;""")
        except Exception as e:
            raise DatabaseException("Error while initializing the database.", e)
        else:
            pass

    def create_indexes(self):
        """Try to speed up subsequent steps by creating indexes in the database."""
        self.cursor.execute("CREATE INDEX idx_reads_read_id ON reads(read_id);")
        #self.cursor.execute("CREATE INDEX idx_cell_id ON reads(cell_id);")
        #self.cursor.execute("CREATE INDEX idx_sample_name ON reads(sample_name);")
        self.cursor.execute("CREATE INDEX idx_reads_cell_sample ON reads(cell_id, sample_name);")

    def process_data(self, threshold):
        """Calculate final association table from the initial read table."""
        self._init_connect()
        print(f"{get_timestamp()}           * Calculating stats on cells")
        self._calculate_stats_on_cells()
        print(f"{get_timestamp()}           * Assigning cells to samples")
        self._assign_cells_to_samples(threshold)
        print(f"{get_timestamp()}           * Creating the association table")
        self._create_final_table()

    def get_total_cell_count(self):
        """Return total number of putative cells."""
        self.cursor.execute("SELECT COUNT(*) as count FROM cells;")
        count = self.cursor.fetchone()
        return(count[0])

    def get_cell_count_per_sample(self):
        """Return cell count per sample tag."""
        self.cursor.execute("SELECT sample, COUNT(*) as count FROM cells GROUP BY sample;")
        counts = self.cursor.fetchall()
        return(counts)

    def _calculate_stats_on_cells(self):
        """Calculate intermitten cells_stat table,
            requires: reads TABLE filled with all values (self.store() )
        """
        try:
            self.cursor.execute("""CREATE TABLE cells_stat AS 
                                SELECT cell_id, sample_name, COUNT(*) abundance 
                                FROM reads GROUP BY cell_id, sample_name;""")
            self.cursor.execute("CREATE INDEX idx_stat_cell_id ON cells_stat(cell_id);")
            self.cursor.execute("CREATE INDEX idx_stat_sample_name ON cells_stat(sample_name);")
        except Exception as e:
            raise DatabaseException("Error while calculating statistics on cell IDs.", e)
    
    def _assign_cells_to_samples(self, threshold = 0.75):
        """Create an intermitten table cells,
            requires: cells_stat TABLE (self.calculate_stats_on_cells() )
        """
        try:
            query = f"""CREATE TABLE cells AS
                          SELECT  cell_id,
                            CASE
                              WHEN max_abd >= sum_abd*{threshold} THEN sample_name
                                  ELSE 'MULTIPLE'
                              END sample FROM
                                (SELECT cell_id, sample_name,
                                     MAX(abundance) max_abd, SUM(abundance) sum_abd
                                         FROM cells_stat GROUP BY cell_id);"""
            self.cursor.execute(query)
            self.cursor.execute("CREATE INDEX idx_cells_cell_id ON cells(cell_id);")
            self.cursor.execute("CREATE INDEX idx_cells_sample_name ON cells(sample);")
        except Exception as e:
            raise DatabaseException("Error while calculating cell-sample relationships.", e)

    def cleanup(self):
        """Delete all but the final table from the database."""
        self._init_connect()
        try:
            self.cursor.execute("DROP TABLE IF EXISTS reads;")
            self.cursor.execute("DROP TABLE IF EXISTS cells;")
            self.cursor.execute("DROP TABLE IF EXISTS cells_stat;")
        except Exception as e:
            raise DatabaseException("Error while deleting the database.", e)

    def _create_final_table(self):
        """Calculate the final association table."""
        self._init_connect()
        try:
            self.cursor.execute("""CREATE TABLE read_sample_pairs AS
                                    SELECT read_id, sample FROM
                                      (SELECT read_id, reads.cell_id, sample FROM reads
                                        LEFT JOIN cells ON reads.cell_id = cells.cell_id);""")
            ## create a covering index
            self.cursor.execute("CREATE INDEX idx_pairs_cover ON read_sample_pairs(read_id, sample);")
        except Exception as e:
            raise DatabaseException("Error while assigning the dominant sample to cell IDs.", e)

    def get_multiple_read_sample_pairs(self, key_list):
        """Return sample for read IDs provided in the argument"""
        self._init_connect()
        return self.get_multiple("read_sample_pairs", ["read_id", "sample"], "read_id", key_list)

    def get_multiple(self, table, field_list, lookup_field, key_list):
        """generic method for retrieving information form the database."""
        if len(key_list) < 1:
            raise DatabaseException("No read IDs provided to look up.")
        fields_string = ",".join([ field for field in field_list ])
        keys_string = ",".join("?" * len(key_list))
        key_list = list(key_list)
        
        result = None
        try:
            self.cursor.execute(f"""SELECT {fields_string} FROM {table} 
                                    WHERE {lookup_field} IN ({keys_string});""", key_list)
            result = dict(self.cursor.fetchall())
        except Exception as e:
            raise DatabaseException("Error while retrieving data the database.", e)

        return(result)

    def store(self, records):
        """Insert read information into the database."""
        try:
            self.cursor.executemany('INSERT INTO reads VALUES(?,?,?);', records);
        except Exception as e:
            raise DatabaseException("Error while inserting into the database.", e)
        records.clear()

    def commit(self):
        """Commit the changes to the database."""
        try:
            self.cursor.execute("COMMIT;")
        except Exception as e:
            raise DatabaseException("Error while committing changes to the database.", e)

    def close(self):
        """Close the database connection."""
        if self.connection is not None:
            try:
                self.connection.close()
            except Exception as e:
                raise DatabaseException("Error while closing connection to the database.", e)
        
    def remove_db(self):
        """Delete the database file."""
        self.close()
        os.remove(self.db_file_name)

