import settings as s
import mysql.connector
from tkinter import Tk
from tkinter.filedialog import askdirectory
import os
import pandas as pd

Tk().withdraw()
files_path = askdirectory()

connection = mysql.connector.connect(
    host=s.DATABASE_HOSTNAME,
    user=s.ACTIVE_USERNAME,
    password=s.ACTIVE_USER_PWD,
    database=s.ACTIVE_DATABASE
)

try:
    if connection.is_connected():
        print("Connected to database successfully")
except Exception as e:
    print(f"Exception Occurred: {e}")

table_name = os.path.basename(files_path)
print(f"Processing data for table: Kaggle.{table_name}")

def pandas_to_mysql_type(pandas_type):
    """Convert Pandas dtype to MySQL data type"""
    if pd.api.types.is_integer_dtype(pandas_type):
        return "INT"
    elif pd.api.types.is_float_dtype(pandas_type):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(pandas_type):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(pandas_type):
        return "DATETIME"
    else:
        return "VARCHAR(255)"

try:
    all_files = [os.path.join(files_path, f) for f in os.listdir(files_path) if f.endswith('.csv')]
    
    if not all_files:
        raise FileNotFoundError("No CSV files found in directory")
    
    train_file = None
    for file_path in all_files:
        if "train.csv" in file_path.lower():
            train_file = file_path
            break
    
    if not train_file:
        raise FileNotFoundError("train.csv not found in directory")
    
    df_train = pd.read_csv(train_file)
    original_columns = df_train.columns.tolist()
    
    column_defs = []
    for col_name, col_type in df_train.dtypes.items():
        mysql_type = pandas_to_mysql_type(col_type)
        if col_name.lower() == 'id':
            column_defs.append(f"`{col_name}` {mysql_type}")
        else:
            column_defs.append(f"`{col_name}` {mysql_type}")
    
    column_defs.append("`FileCategory` VARCHAR(20)")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS Kaggle.{table_name} (
        {',\n        '.join(column_defs)}
    )
    """
    
    cursor = connection.cursor()
    cursor.execute(create_table_sql)
    print(f"Created table: Kaggle.{table_name}")

    for file_path in all_files:
        if "sample_submission" in file_path.lower():
            file_category = "sample"
        elif "train" in file_path.lower():
            file_category = "train"
        else:
            file_category = "test"
        print(f"\nProcessing {file_category} file: {os.path.basename(file_path)}")

        df = pd.read_csv(file_path)
        
        df['FileCategory'] = file_category
        
        expected_columns = original_columns + ['FileCategory']
        df = df.reindex(columns=expected_columns)
        
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)
                df[col] = df[col].replace('nan', None)
            elif pd.api.types.is_numeric_dtype(df[col]):
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].where(pd.notnull(df[col]), None)
        
        placeholders = ', '.join(['%s'] * len(df.columns))
        columns = ', '.join([f"`{col}`" for col in df.columns])
        
        insert_sql = f"""
        INSERT INTO Kaggle.{table_name} (
            {columns}
        ) VALUES (
            {placeholders}
        )
        """
        
        data_to_insert = []
        for row in df.itertuples(index=False):
            row_values = []
            for value in row:
                if pd.isna(value) or value == 'nan' or value == 'None':
                    row_values.append(None)
                else:
                    row_values.append(value)
            data_to_insert.append(tuple(row_values))
        
        try:
            cursor.executemany(insert_sql, data_to_insert)
            connection.commit()
            print(f"Successfully inserted {len(df)} rows from {file_category} file")
        except mysql.connector.Error as err:
            print(f"Error inserting data from {file_path}: {err}")
            print("Problematic row:", data_to_insert[len(data_to_insert)-1] if data_to_insert else "No rows to insert")
            connection.rollback()

except Exception as e:
    print(f"Exception Occurred: {e}")
    if connection.is_connected():
        connection.rollback()
finally:
    if connection.is_connected():
        if 'cursor' in locals():
            cursor.close()
        connection.close()
        print("Database connection closed")