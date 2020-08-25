import os, sqlite3
from datetime import datetime


class DatabaseException(Exception):
    pass

def get_timestamp():
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%Y-%m-%d_%H:%M:%S")
    return(timestampStr)

class SQLReadStorage:

    connection = None
    cursor = None
    db_file_name = None 

    def __init__(self, file_name):
        self.db_file_name = file_name

    def _init_connect(self):
        if self.connection is None:
            try:
                self.connection = sqlite3.connect(self.db_file_name)
                self.cursor = self.connection.cursor()
                self.cursor.execute("PRAGMA synchronous = OFF;")
                self.cursor.execute("PRAGMA journal_mode = OFF;")
                self.cursor.execute("PRAGMA LOCKING_MODE = EXCLUSIVE;")
                self.cursor.execute("PRAGMA foreign_keys=off;")
            except Exception as e:
                raise DatabaseException("Error while connecting to the database.", e)

    def setup(self):
        if os.path.exists(self.db_file_name):
            os.remove(self.db_file_name)

        self._init_connect()
        # Create table
        try:
            self.cursor.execute("""CREATE TABLE reads ( read_id text PRIMARY_KEY, 
                                                        cell_id text NOT NULL,
                                                        sample_name text NOT NULL );""")
        except Exception as e:
            raise DatabaseException("Error while initializing the database.", e)
        else:
            self.commit()

    def create_indexes(self):
        self.cursor.execute("CREATE INDEX idx_cell_id ON reads(cell_id);")
        self.cursor.execute("CREATE INDEX idx_sample_name ON reads(sample_name);")

    def process_data(self, threshold):
        self._init_connect()
        print(f"{get_timestamp()}       Calculating stats on cells")
        self._calculate_stats_on_cells()
        print(f"{get_timestamp()}       Assigning cells to samples")
        self._assign_cells_to_samples(threshold)
        print(f"{get_timestamp()}       Creating the final table")
        self._create_final_table()

    def _calculate_stats_on_cells(self):
        """
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
        else:
            self.commit()
    
    def _assign_cells_to_samples(self, threshold = 0.75):
        """
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
                                         FROM cells_stat GROUP BY cell_id); """
            self.cursor.execute("CREATE INDEX idx_cells_cell_id ON cells(cell_id);")
            self.cursor.execute("CREATE INDEX idx_cells_sample_name ON cells(sample_name);")
            self.cursor.execute(query)
        except Exception as e:
            raise DatabaseException("Error while calculating cell-sample relationships.", e)
        else:
            self.commit()

    def cleanup(self):
        self._init_connect()
        try:
            self.cursor.execute("DROP TABLE reads;")
            self.cursor.execute("DROP TABLE cells;")
            self.cursor.execute("DROP TABLE cells_stat;")
        except Exception as e:
            raise DatabaseException("Error while deleting the database.", e)
        else:
            self.commit()

    def _create_final_table(self):
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
        else:
            self.commit()

    def get_multiple_read_sample_pairs(self, key_list):
        self._init_connect()
        return self.get_multiple("read_sample_pairs", ["read_id", "sample"], "read_id", key_list)

    def get_multiple(self, table, field_list, lookup_field, key_list):
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
        print("Saving reads to database.")
        try:
            self.cursor.executemany('INSERT INTO reads VALUES(?,?,?);', records);
            self.commit()
        except Exception as e:
            raise DatabaseException("Error while inserting into the database.", e)
        records.clear()

    def commit(self):
        try:
            self.connection.commit()
        except Exception as e:
            raise DatabaseException("Error while committing changes to the database.", e)

    def close(self):
        if self.connection is not None:
            try:
                self.connection.close()
            except Exception as e:
                raise DatabaseException("Error while closing connection to the database.", e)
        
    def remove_db(self):
        self.close()
        os.remove(self.db_file_name)

