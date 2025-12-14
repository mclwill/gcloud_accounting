import sqlite3
conn  =  sqlite3.connect('users.sqlite3', check_same_thread=False)
cursor = conn.cursor()
sql_query = """SELECT name FROM sqlite_master  WHERE type='table';"""
cursor.execute(sql_query)
print(cursor.fetchall())