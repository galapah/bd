

import sqlite3
connection = sqlite3.connect(".reads.db")
cursor = connection.cursor()
key_list = ['1:2634:28456:7185', '1:1103:15609:12649']

keys_string = "|".join([ f"'{k}'" for k in key_list ])
keys_string = "|".join([ k for k in key_list ])

print(keys_string)

cursor.execute(f"SELECT read_id, cell_id FROM reads WHERE read_id IN ({keys_string});")
result = cursor.fetchall()
