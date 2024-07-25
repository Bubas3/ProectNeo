import pandas as pd
import psycopg2
from datetime import datetime
import time
from psycopg2 import OperationalError
from settings import DBNAME, DBUSER, DBPASS, DBHOST, DBPORT

#функция подключения к бд
def create_connection(dbname, dbuser, dbpass, dbhost, dbport):
    conn = None
    try:
        conn = psycopg2.connect(database=dbname, user=dbuser, password=dbpass, host=dbhost, port=dbport)
        print('Успешное подключение')
    except OperationalError as e:
        print(f'Ошибка подключения: {e}')
    return conn

#функция добавления в логи
def insert_log_etl(connection, status, message):
    if connection is not None:
        cursor = connection.cursor()
        start_time = datetime.now()
        cursor.execute("INSERT INTO LOGS.ETL_LOG (start_time, status, message) VALUES (%s, %s, %s) RETURNING log_id",
                       (start_time, status, message))
        log_id = cursor.fetchone()[0]
        connection.commit()
        cursor.close()
        return log_id, start_time
    else:
        print('Нет подключения к БД')

# функция апдейта лога
def update_log_etl(connection, log_id, status, message):
    if connection is not None:
        cursor = connection.cursor()
        end_time = datetime.now()
        cursor.execute("UPDATE LOGS.ETL_LOG SET end_time = %s, status = %s, message = %s WHERE log_id = %s",
                       (end_time, status, message, log_id))
        connection.commit()
        cursor.close()
    else:
        print('Нет подключения к БД')


#функция загрузки данных бд
def load_csv_to_db(connection,file_path, table_name, conflict_columns):
    log_id, start_time = insert_log_etl(connection,'STARTED', f'Loading {file_path} into {table_name}')
    try:
        df = pd.read_csv(file_path, sep=';', header=0,encoding="cp65001", parse_dates=True,dtype=str)
        df = df.where(pd.notnull(df), None)#NaN -> None
        cursor = connection.cursor()
        if not conflict_columns:
            cursor.execute(f'TRUNCATE {table_name} RESTART IDENTITY')
        for i, row in df.iterrows():
            columns = ', '.join(row.index).replace(';',',')
            values = ', '.join(['%s'] * len(row))
            if conflict_columns:
                conflict_action = ', '.join([f"{col} = EXCLUDED.{col}" for col in row.index])
                query = f"""
                    INSERT INTO {table_name} ({columns}) VALUES ({values})
                    ON CONFLICT ({conflict_columns}) DO UPDATE SET {conflict_action}"""
            else:
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            cursor.execute(query, tuple(row))
        connection.commit()
        cursor.close()
        time.sleep(5)  # пауза на 5 секунд
        update_log_etl(connection,log_id, 'SUCCESS', f'Successfully loaded {file_path} into {table_name}')
        print(f'Таблица {table_name} успешно загружена')
    except Exception as e:
        print(e)
        connection.rollback()
        update_log_etl(connection,log_id, 'FAILED', str(e))

# функция выгрузки данных с бд
def unload_db_to_csv(connection):
    log_id, start_time = insert_log_etl(connection, 'STARTED', f'UnLoading dm.dm_f101_round_f')
    try:
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM dm.dm_f101_round_f')
        rows = cursor.fetchall()
        data = pd.DataFrame(rows)
        col_name = pd.read_sql("SELECT column_name FROM information_schema.columns where table_schema='dm' and table_name='dm_f101_round_f'",con=connection)
        data.columns = col_name['column_name'].values
        data = data.where(pd.notnull(data), None)#NaN -> None
        data.to_csv('CSVLOAD/md_f101_round_f.csv',index=False,sep=';',encoding='UTF-8')
        update_log_etl(connection, log_id, 'SUCCESS', f'Successfully unloaded dm.dm_f101_round_f')
        print('Файл успешно выгружен')
    except Exception as e:
        connection.rollback()
        update_log_etl(connection, log_id, 'FAILED', str(e))


#1.1
connection = create_connection(DBNAME, DBUSER, DBPASS, DBHOST, DBPORT)
#load_csv_to_db(connection, 'CSVLOAD/ft_balance_f.csv', 'ds.ft_balance_f','on_date, account_rk')
#load_csv_to_db(connection, 'CSVLOAD/ft_posting_f.csv', 'ds.ft_posting_f','')
#load_csv_to_db(connection, 'CSVLOAD/md_account_d.csv', 'ds.md_account_d','data_actual_date,account_rk')
#load_csv_to_db(connection, 'CSVLOAD/md_currency_d.csv', 'ds.md_currency_d','currency_rk,data_actual_date')
#load_csv_to_db(connection, 'CSVLOAD/md_exchange_rate_d.csv', 'ds.md_exchange_rate_d','data_actual_date,currency_rk')
#load_csv_to_db(connection, 'CSVLOAD/md_ledger_account_s.csv', 'ds.md_ledger_account_s','ledger_account,start_date')
#connection.close()

#1.4
#unload_db_to_csv(connection)
load_csv_to_db(connection, 'CSVLOAD/md_f101_round_f.csv', 'dm.dm_f101_round_f_v2','')
