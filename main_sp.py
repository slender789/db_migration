import pyodbc
import datetime
from sqlalchemy import create_engine
import pandas as pd
import pymysql
import warnings
warnings.filterwarnings("ignore")

# conexion MySQL 
mysql_db_user = "dbeaver"
mysql_db_password = "dbeaver"
mysql_db_host = "localhost"
mysql_db_port = "3306"
mysql_db_name = "test"

# conexion SQL Server 
sql_db_user = "sa"
sql_db_password = "Password2"
sql_db_host = "localhost"

# Conexion a MySQL
cnx_mysql = pymysql.connect(
    user=mysql_db_user, password=mysql_db_password,
    host=mysql_db_host, port=int(mysql_db_port), db=mysql_db_name)

# Conexion a SQL Server
cnx_sql = pyodbc.connect(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_db_host};UID={sql_db_user};PWD={sql_db_password}")

# Motor SQLAlchemy para SQL Server
engine_sql = create_engine(f'mssql+pyodbc://{sql_db_user}:{sql_db_password}@{sql_db_host}/master?driver=ODBC+Driver+17+for+SQL+Server')

# Obtener todas las tablas de MySQL
cursor_mysql = cnx_mysql.cursor()
cursor_mysql.execute("SHOW TABLES")
tables = cursor_mysql.fetchall()

for table in tables:
    table_name = table[0]

    # Revisar si tabla ya existe en SQL Server
    cursor_sql = cnx_sql.cursor()
    cursor_sql.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
    table_exists = len(cursor_sql.fetchall()) > 0
    
    if not table_exists:
        # Obtenemos nombres de las columnas y tipos de tabla de MySQL 
        cursor_mysql.execute(f"DESCRIBE {table_name}")
        columns = cursor_mysql.fetchall()
        columns_sql = [f"{col[0].replace('İ', 'I')} {'float' if 'double' in col[1] else col[1].replace('tinyint(1)', 'tinyint')}" for col in columns]
        
        # Transformacion de auto_increment a IDENTITY(1,1) en SQL Server
        auto_increment_cols = [col[0] for col in columns if col[5] == 'auto_increment']
        for cols in range(len(columns_sql)):
            for aut in auto_increment_cols:
                if aut in columns_sql[cols]:
                    columns_sql[cols] = columns_sql[cols] + " IDENTITY(1,1)"
                        
        # Obtenemos los indices de la tabla para detectar llaves primarias
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
        
        # Generamos declaracion SQL para crear tabla en SQL Server
        query = f"CREATE TABLE {table_name} ({','.join(columns_sql)}{pk_constraint}{index_constraint})"
        print(query)

        # Generamos tabla en SQL Server
        try: 
            cursor_sql.execute(query)
            cursor_sql.commit()
            print(f"Creacion de tabla {table_name} exitosa\n") 
        except Exception as e:
            print(f"Creacion de tabla {table_name} fallo con error: {str(e)}\n") 
        
    if table_exists:
        # Si existe, borramos lo existente
        cursor_sql.execute(f"DELETE FROM {table_name}")
        cnx_sql.commit()
        # Imprimimos el numero de filas borradas
        rows_deleted = cursor_sql.rowcount
        print(f"Filas borradas: {rows_deleted}")
        cursor_sql.close()

    # Determinamos tiempo de inicio
    start_time = datetime.datetime.now()
    
    # Seleccionamos todos los datos de la tabla en MySQL
    query = f"SELECT * FROM {table_name}"
    data = pd.read_sql(query, cnx_mysql)
    
    # Escribimos los datos de Mysql a SQL Server mediante motor SQLAlchemy
    try:
        data.to_sql(name=table_name, con=engine_sql, if_exists='append', index=False)
        end_time = datetime.datetime.now()
        elapsed_time = end_time - start_time
        elapsed_seconds = int(elapsed_time.total_seconds())
        minutes, seconds = divmod(elapsed_seconds, 60)
        print(f"Migracion de datos de tabla {table_name} exitosa  \n{len(data)} filas fueron agredas en: {minutes} minutos {seconds} segundos.")
    except Exception as e:
        print(f"Migracion de datos de tabla {table_name} fallo con error: {str(e)}")

# Cerramos conexiones a bases de datos
cnx_mysql.close()
engine_sql.dispose()
