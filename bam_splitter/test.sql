import sqlite3
con = sqlite3.connect(".bamsplitter.db")
c = con.cursor()
c.execute("SELECT * FROM sqlite_master WHERE type='table';")
r = c.fetchall()
r
