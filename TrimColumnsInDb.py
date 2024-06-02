import psycopg2
import pyodbc
import json
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import ctypes
ctypes.windll.kernel32.SetConsoleTitleW("Trim Columns In DB")

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

db_type = config['db_type']

db_params = {
    'database': config['database'],
    'user': config['user'],
    'password': config['password'],
    'host': config['host'],
    'port': config['port']
}

max_workers = config['max_workers']

if db_type == 'postgresql':
    conn = psycopg2.connect(
        dbname=db_params['database'],
        user=db_params['user'],
        password=db_params['password'],
        host=db_params['host'],
        port=db_params['port']
    )
elif db_type == 'sqlserver':
    conn = pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={db_params['host']},{db_params['port']};"
        f"DATABASE={db_params['database']};UID={db_params['user']};PWD={db_params['password']}"
    )
else:
    raise ValueError("Unsupported database type")

cur = conn.cursor()

cur.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'public' AND data_type = 'character varying'")
table_columns = cur.fetchall()

cur.close()
conn.close()

def trim_table_columns(table, columns):
    try:
        progress_bar.set_description(f"{table}", False)
        if db_type == 'postgresql':
            conn = psycopg2.connect(
                dbname=db_params['database'],
                user=db_params['user'],
                password=db_params['password'],
                host=db_params['host'],
                port=db_params['port']
            )
        elif db_type == 'sqlserver':
            conn = pyodbc.connect(
                f"DRIVER={{SQL Server}};SERVER={db_params['host']},{db_params['port']};"
                f"DATABASE={db_params['database']};UID={db_params['user']};PWD={db_params['password']}"
            )
        else:
            raise ValueError("Unsupported database type")
        
        cur = conn.cursor()

        for column in columns:
            progress_bar.set_description(f"{table}.{column}", False)
            if db_type == 'postgresql':
                update_query = f"UPDATE {table} SET {column} = TRIM({column}) WHERE {column} LIKE '% '"
            elif db_type == 'sqlserver':
                update_query = f"UPDATE {table} SET {column} = LTRIM(RTRIM({column})) WHERE {column} LIKE '% '"
            
            cur.execute(update_query)
            conn.commit()
            progress_bar.update(1)

    except Exception as e:
        error_message = f"\nHata: {table} işlenirken;\n{e}"
        with open('errors.txt', 'a', encoding="utf-8") as error_file:
            error_file.write(error_message)

    finally:
        cur.close()
        conn.close()

table_grouped = {}
for table, column in table_columns:
    if table in table_grouped:
        table_grouped[table].append(column)
    else:
        table_grouped[table] = [column]

progress_bar = tqdm(total=len(table_columns), position=0, leave=True)

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for table, columns in table_grouped.items():
        executor.submit(trim_table_columns, table, columns)

progress_bar.close()
print("\n")
print("##########################################\n")
print("###      THE PROCESS IS COMPLETED       ##\n")
print("##########################################\n\n")

input("The process is over. You can close the window....")