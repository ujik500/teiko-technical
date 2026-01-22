import pandas as pd
import sqlite3
import csv

'''
Load CSV data from input file into a table in an SQlite database
'''
def load_csv_to_sqlite(input_filename, db_name, table_name):
    file = open(input_filename, "r")
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # overwrite data if it is already present
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # define database schema
    cursor.execute(f"""
        CREATE TABLE {table_name} (
        project TEXT,
        subject TEXT,
        condition TEXT,
        age INTEGER,
        sex TEXT, 
        treatment TEXT,
        response TEXT,
        sample TEXT,
        sample_type TEXT,
        time_from_treatment_start INTEGER,
        b_cell INTEGER,
        cd8_t_cell INTEGER,
        cd4_t_cell INTEGER,
        nk_cell INTEGER,
        monocyte INTEGER
        )"""
    )

    columns = ['project','subject','condition','age','sex','treatment',
               'response','sample', 'sample_type','time_from_treatment_start',
               'b_cell','cd8_t_cell','cd4_t_cell','nk_cell',
               'monocyte']
    
    col_names = ",".join(columns)
    placeholders = ",".join(["?"] * len(columns))

    reader = csv.reader(file)
    next(reader) # skip header

    cursor.executemany(
        f"""
        INSERT INTO {table_name} ({col_names})
        VALUES ({placeholders})
        """
        , reader
    )

    conn.commit()
    conn.close()

    return None

'''
Given an SQlite db and table name, produce a new summary table of the samples 
in that dataset
'''
def overview(db_name, table_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS intermediate")
    cursor.execute(
        f"""
        CREATE TABLE intermediate AS
        SELECT sample, b_cell, cd8_t_cell, cd4_t_cell, nk_cell, monocyte
        FROM {table_name}
        """
    )

    # add total_count column
    cursor.execute("ALTER TABLE intermediate ADD COLUMN total_count INTEGER;")
    cursor.execute(
        f"""
        UPDATE intermediate SET total_count = b_cell + cd8_t_cell + 
        cd4_t_cell + nk_cell + monocyte;
        """
    )

    # unpivot to separate by sample, population pair
    cursor.execute(f"DROP TABLE IF EXISTS overview")
    cursor.execute(
        """
        CREATE TABLE overview AS
        SELECT sample, total_count, 'b_cell' AS population, b_cell, cd8_t_cell, cd4_t_cell, nk_cell, monocyte FROM intermediate
        UNION ALL
        SELECT sample, total_count, 'cd8_t_cell' AS population, b_cell, cd8_t_cell, cd4_t_cell, nk_cell, monocyte FROM intermediate
        UNION ALL 
        SELECT sample, total_count, 'cd4_t_cell' AS population, b_cell, cd8_t_cell, cd4_t_cell, nk_cell, monocyte FROM intermediate
        UNION ALL 
        SELECT sample, total_count, 'nk_cell' AS population, b_cell, cd8_t_cell, cd4_t_cell, nk_cell, monocyte FROM intermediate
        UNION ALL 
        SELECT sample, total_count, 'monocyte' AS population, b_cell, cd8_t_cell, cd4_t_cell, nk_cell, monocyte FROM intermediate
        """
    )
    cursor.execute(f"DROP TABLE intermediate")

    for row in cursor.execute(f"SELECT * FROM overview LIMIT 50 OFFSET 52450"):
        print(row)

    conn.commit()
    conn.close()
    pass

input_filename = "cell-count.csv"
db_name = "teiko_technical.db"
table_name = "sample_data"
data = load_csv_to_sqlite(input_filename, db_name, table_name)
overview(db_name, table_name)