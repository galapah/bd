import os, sqlite3


class ReadStorage:

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


class SQLReadStorage(ReadStorage):

    cursor = None
    db_file_name = None 

    def __init__(self, file_name):
        self.db_file_name = file_name

    def _init_connect(self):
        if self.connection is None:
            self.connection = sqlite3.connect(self.db_file_name)
            self.cursor = self.connection.cursor()

    def setup(self):
        if os.path.exists(self.db_file_name):
            os.remove(self.db_file_name)

        self._init_connect()
        # Create table
        self.cursor.execute("""CREATE TABLE reads ( read_id text PRIMARY_KEY, 
                                                    cell_id text NOT NULL,
                                                    sample_name text NOT NULL );""")
        self.cursor.execute("CREATE INDEX idx_cell_id ON reads(cell_id);")
        self.cursor.execute("CREATE INDEX idx_sample_name ON reads(sample_name);")
        self.commit()

    def process_data(self):
        self._init_connect()
        self._calculate_stats_on_cells()
        self._assign_cells_to_samples()
        self._create_final_table()

    def _calculate_stats_on_cells(self):
        """
        requires: reads TABLE filled with all values (self.store() )
        """
        self.cursor.execute("""CREATE TABLE cells_stat AS 
                                SELECT cell_id, sample_name, COUNT(*) abundance 
                                FROM reads GROUP BY cell_id, sample_name;""")
        self.commit()
    
    def _assign_cells_to_samples(self):
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
        self._init_connect()
        self.cursor.execute("DROP TABLE reads;")
        self.cursor.execute("DROP TABLE cells;")
        self.cursor.execute("DROP TABLE cells_stat;")
        self.commit()

    def _create_final_table(self):
        self._init_connect()
        self.cursor.execute("""CREATE TABLE read_sample_pairs AS
                                    SELECT read_id, sample FROM
                                        (SELECT read_id, reads.cell_id, sample FROM reads
                                            LEFT JOIN cells ON reads.cell_id = cells.cell_id);""")
        self.commit()

    def get_multiple_read_sample_pairs(self, key_list):
        self._init_connect()
        return self.get_multiple("read_sample_pairs", ["read_id", "sample"], "read_id", key_list)

    def get_multiple(self, table, field_list, lookup_field, key_list):
        if len(key_list) < 1:
            raise Exception("No read IDs provided to look up.")
        fields_string = ",".join([ field for field in field_list ])
        keys_string = ",".join("?"*len(key_list))
        key_list = list(key_list)

        self.cursor.execute(f"""SELECT {fields_string} FROM {table} 
                                WHERE {lookup_field} IN ({keys_string});""", key_list)
        result = dict(self.cursor.fetchall())
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
        if self.connection is not None:
            self.connection.close()

    def remove_db(self):
        self.close()
        os.remove(self.db_file_name)

