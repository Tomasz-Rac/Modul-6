import sqlite3
import csv
from sqlite3 import Error

def create_connection(db_file):
   """ create a database connection to a SQLite database """
   conn = None
   try:
       conn = sqlite3.connect(db_file)
       return conn
   except Error as e:
       print(e)
   return conn

def create_connection_in_memory():
   """ create a database connection to a SQLite database """
   conn = None
   try:
       conn = sqlite3.connect(":memory:")
       print(f"Connected, sqlite version: {sqlite3.version}")
   except Error as e:
       print(e)
   finally:
       if conn:
           conn.close()

def execute_sql(conn, sql):
    """Execute a SQL script."""
    try:
        c = conn.cursor()
        c.execute(sql)
        conn.commit()
    except Error as e:
        print(e)

def add_project(conn, project):
    """Insert a new project into the projekty table"""
    sql = '''INSERT INTO projekty(nazwa, godzina, zadanie, data, ocena)
             VALUES(?,?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, project)
    conn.commit()
    return cur.lastrowid

def projekt(conn, projekt):
    sql = '''INSERT INTO projekty(nazwa, zadanie, "data", godzina, ocena)
         VALUES(?,?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, projekt)
    conn.commit()
    return cur.lastrowid

def print_projekty(conn):
    sql = "SELECT * FROM projekty"
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()

    print("\nTabela Praca:")
    print("-" * 120)
    print(f"{'ID':<5} {'Nazwa':<15} {'Zadanie':<15} {'Data':<15} {'Godzina':<15} {'Ocena':<15}")
    print("-" * 120)

    for row in rows:
        print(f"{row[0]:<5} {row[1]:<15} {row[2]:<15} {row[3]:<15} {row[4]:<15} {row[5]:<15}")

def download_data(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]

        with open(f"{table_name}.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(col_names)
            writer.writerows(rows)

        print(f"Data from '{table_name}' saved to {table_name}.csv")

def update_project(conn, project_id, nazwa=None, zadanie=None, data=None, godzina=None, ocena=None):
    """Update an existing project record with new values."""
    sql = "UPDATE projekty SET "
    updates = []
    params = []

    if nazwa:
        updates.append("nazwa = ?")
        params.append(nazwa)
    if zadanie:
        updates.append("zadanie = ?")
        params.append(zadanie)
    if data:
        updates.append('"data" = ?')
        params.append(data)
    if godzina:
        updates.append("godzina = ?")
        params.append(godzina)
    if ocena:
        updates.append("ocena = ?")
        params.append(ocena)

    if not updates:
        print("No fields provided for update.")
        return

    sql += ", ".join(updates) + " WHERE id = ?"
    params.append(project_id)

    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        print(f"Updated project with ID {project_id}")
    except Error as e:
        print(f"Error updating project: {e}")

def delete_all_data(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        for table in tables:
            table_name = table[0]
            cursor.execute(f"DELETE FROM {table_name};")
            conn.commit()
            print(f"All data deleted from '{table_name}'")

    except Error as e:
        print(f"Error deleting data: {e}")

if __name__=="__main__":
    create_projekty_sql = """
    CREATE TABLE IF NOT EXISTS projekty (
        id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Added AUTOINCREMENT
        nazwa TEXT NOT NULL,
        data TEXT,
        godzina TEXT
);
"""

    create_zadania_sql = """
    CREATE TABLE IF NOT EXISTS zadania (
        id INTEGER PRIMARY KEY AUTOINCREMENT,   -- Added AUTOINCREMENT
        projekt_id INTEGER NOT NULL,
        nazwa VARCHAR(250) NOT NULL,
        zadanie VARCHAR(250) NOT NULL,
        data TEXT NOT NULL,
        godzina TEXT NOT NULL,
        ocena TEXT NOT NULL,
        FOREIGN KEY (projekt_id) REFERENCES projekty (id)
);
"""

    db_file = "database.db"
    conn = create_connection(db_file)
    if conn is not None:
        execute_sql(conn, create_projekty_sql)
        execute_sql(conn, create_zadania_sql)

        projekty = ("Piekarnia", "04:45", "Pączki", "2025-02-27", "5.0")
        pr_id = projekt(conn, projekty)

        projekty2 = ("Rzeźnia", "05:20", "Steki", "2025-03-21", "6.0")
        pr_id2 = projekt(conn, projekty2)
        print(f"Dodano pracę z ID : {pr_id2}")

        projekty3 = ("Poczta", "10:44", "Sortowanie", "2025-03-19", "3.5")
        pr_id3 = projekt(conn, projekty3)
        print(f"Dorano pracę z ID : {pr_id3}")

        print_projekty(conn)

        download_data(conn)

        update_project(conn, pr_id3, nazwa="Szkoła", godzina="08:10", zadanie="Nauka", data="2025-03-03", ocena="4.5")

        delete_all_data(conn)

        conn.close()

