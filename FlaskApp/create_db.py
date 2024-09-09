
import sqlite3

conn  =  sqlite3.connect('users.sqlite3')

cursor = conn.cursor()


cursor.execute("""CREATE TABLE user (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE NOT NULL,
  profile_pic TEXT NOT NULL
)
"""
)
               
conn.commit()
conn.close()
print('database created')
