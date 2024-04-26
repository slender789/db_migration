import pyodbc
import datetime
from sqlalchemy import create_engine
import pandas as pd
import pymysql
import warnings
warnings.filterwarnings("ignore")

# MySQL connection env. 
mysql_db_user = "dbeaver"
mysql_db_password = "dbeaver"
mysql_db_host = "localhost"
mysql_db_port = "3306"
mysql_db_name = "test"

# MySQL connection env. 
sql_db_user = "sa"
sql_db_password = "Password2"
sql_db_host = "localhost"

# Connect to MySQL database
cnx_mysql = pymysql.connect(
    user=mysql_db_user, password=mysql_db_password,
    host=mysql_db_host, port=int(mysql_db_port), db=mysql_db_name)

# Connect to SQL Server database
cnx_sql = pyodbc.connect(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_db_host};UID={sql_db_user};PWD={sql_db_password}")

# Set up SQLAlchemy engine for SQL Server connection
engine_sql = create_engine(f'mssql+pyodbc://{sql_db_user}:{sql_db_password}@{sql_db_host}/master?driver=ODBC+Driver+17+for+SQL+Server')

# Get all table names from MySQL
cursor_mysql = cnx_mysql.cursor()
cursor_mysql.execute("SHOW TABLES")
tables = cursor_mysql.fetchall()

for table in tables:
    table_name = table[0]

    # Check if table exists in SQL Server database
    cursor_sql = cnx_sql.cursor()
    cursor_sql.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
    table_exists = len(cursor_sql.fetchall()) > 0
    
    # if table_exists:
    #     # Delete the table
    #     cursor_sql.execute(f"DROP TABLE {table_name}")
    #     cursor_sql.commit()
    #     table_exists = False
    
    if not table_exists:
        # Fetch column names and data types from MySQL table
        cursor_mysql.execute(f"DESCRIBE {table_name}")
        columns = cursor_mysql.fetchall()
        columns_sql = [f"{col[0].replace('İ', 'I')} {'float' if 'double' in col[1] else col[1].replace('tinyint(1)', 'tinyint')}" for col in columns]
        
        # Update auto_increment columns to IDENTITY(1,1) in SQL Server
        auto_increment_cols = [col[0] for col in columns if col[5] == 'auto_increment']
        for cols in range(len(columns_sql)):
            for aut in auto_increment_cols:
                if aut in columns_sql[cols]:
                    columns_sql[cols] = columns_sql[cols] + " IDENTITY(1,1)"
                        

        cursor_mysql.execute(f"SHOW INDEX FROM {table_name}")
        indexes = cursor_mysql.fetchall()
        pk_cols = [idx[4] for idx in indexes if idx[2] == 'PRIMARY']
        if len(pk_cols) > 0:
            pk_constraint = f", CONSTRAINT PK_{table_name} PRIMARY KEY ({','.join(pk_cols)})"
        else:
            pk_constraint = ''
        
        index_cols = [idx[4] for idx in indexes if idx[2] != 'PRIMARY']
        if len(index_cols) > 0:
            index_constraints = [f"INDEX IX_{table_name}_{col} ({col})" for col in index_cols]
            index_constraint = f", {','.join(index_constraints)}"
        else:
            index_constraint = ''
        
        # Generate SQL statement to create table in SQL Server
        query = f"CREATE TABLE {table_name} ({','.join(columns_sql)}{pk_constraint}{index_constraint})"
        print(query) # print the generated query

        # Execute query to create table in SQL Server database
        try: 
            cursor_sql.execute(query)
            cursor_sql.commit()
            print(f"Creation of table {table_name} succesfull\n") 
        except Exception as e:
            print(f"Creation of table {table_name} failed with error: {str(e)}\n") 
        
    if table_exists:
        # Delete all data from the table
        cursor_sql.execute(f"DELETE FROM {table_name}")
        cnx_sql.commit()
        # Get the number of rows deleted
        rows_deleted = cursor_sql.rowcount
        print(f"Number of rows deleted: {rows_deleted}")
        cursor_sql.close()

    # Fetch from SQL Server table
    start_time = datetime.datetime.now()
    
    query = f"SELECT * FROM {table_name}"
    data = pd.read_sql(query, cnx_mysql)
    
    # Write data to SQL Server table
    try:
        data.to_sql(name=table_name, con=engine_sql, if_exists='append', index=False)
        end_time = datetime.datetime.now()
        elapsed_time = end_time - start_time
        elapsed_seconds = int(elapsed_time.total_seconds())
        minutes, seconds = divmod(elapsed_seconds, 60)
        print(f"Data transfer for table {table_name} ; {len(data)} rows was successful in: {minutes} minutes {seconds} seconds.")
    except Exception as e:
        print(f"Data transfer for table {table_name} failed with error: {str(e)}")

# Close database connections
cnx_mysql.close()
engine_sql.dispose()
