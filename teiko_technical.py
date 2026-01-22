import pandas as pd
import sqlite3
import csv

'''
Load CSV data from input file into a table in an SQlite database
'''
def load_csv_to_sqlite(input_filename, db_name="teiko_technical.db", table_name="sample_data"):
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

    sql = f"""
    INSERT INTO {table_name} ({col_names})
    VALUES ({placeholders})
    """

    cursor.executemany(
        f"""
        INSERT INTO sample_data ({col_names})
        VALUES ({placeholders})
        """
        , reader
    )

    return None

'''
Given a SQlite db and table name, produce a summary table of the samples 
in that dataset
'''
def overview(df):
    pass
    #df['total_count'] = df[['b_cell','cd8_t_cell','cd4_t_cell','nk_cell','monocyte']].sum(axis=1)
    #print(df.head)


data = load_csv_to_sqlite("cell-count.csv")
overview(data)