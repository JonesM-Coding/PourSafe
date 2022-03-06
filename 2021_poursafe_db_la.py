import numpy as np
import pandas as pd
import datetime
import os
import psycopg2
import sqlalchemy
import glob
from config.py import postgresql

def getLatestDate(df):
    folder_path = df.rsplit("/", 1)[0]
    print(folder_path)
    files = glob.glob(folder_path + "/*.csv")
    times = [datetime.datetime.fromtimestamp(os.path.getctime(x)) for x in files]
    max_file = max(times)
    datepull = max_file.strftime("%Y%m%d")
    print(datepull)
    return datepull

def getSchema(df, name):
    engine = sqlalchemy.create_engine('postgresql://doadmin:yox1c9wy2onu8wtm@db-postgresql-sfo3-poursafe-do-user-9378326-0.b.db.ondigitalocean.com:25060/poursafe_data?sslmode=require', echo=True)
    check = engine.has_table(name)
    df = pd.read_csv(df, nrows=1)
    # if check == True:
    #     df.to_sql(name, con=engine, chunksize=50000, method='multi', if_exists='replace')
    schema = str(pd.io.sql.get_schema(df, name))
    schema = schema.replace('INTEGER', 'TEXT').replace('REAL', 'TEXT')
    print(schema)
    return schema


def newData(df, name):
    conn_string = 'postgresql://doadmin:yox1c9wy2onu8wtm@db-postgresql-sfo3-poursafe-do-user-9378326-0.b.db.ondigitalocean.com:25060/poursafe_data?sslmode=require'
    print(df)
    # perform to_sql test and print result
    drop_sql = "DROP TABLE IF EXISTS " + name
    sql = "COPY " + name + " FROM STDIN DELIMITER ',' CSV HEADER"

    print("SQL Statement Prepared: " + sql)

    schema = getSchema(df, name)
    pg_conn = psycopg2.connect(conn_string)
    cur = pg_conn.cursor()

    cur.execute(drop_sql)
    cur.execute(schema)
    cur.execute('TRUNCATE TABLE ' + name)
    cur.copy_expert(sql, open(df, "r"))

    pg_conn.commit()
    cur.close()

def upsertData(df, old_df, new_df):
    conn_string = 'postgresql://doadmin:yox1c9wy2onu8wtm@db-postgresql-sfo3-poursafe-do-user-9378326-0.b.db.ondigitalocean.com:25060/poursafe_data?sslmode=require'
    print(df)
    # perform to_sql test and print result

    insert_sql = "INSERT INTO " + old_df + "SELECT * FROM " + new_df + " WHERE UID NOT IN (SELECT UID FROM " + old_df
    # drop_sql = "DROP TABLE IF EXISTS " + old_df
    sql = "COPY " + old_df + " FROM STDIN DELIMITER ',' CSV HEADER"

    print("SQL Statement Prepared: " + sql)

    schema = getSchema(df, old_df)
    pg_conn = psycopg2.connect(conn_string)
    cur = pg_conn.cursor()

    cur.execute(insert_sql)
    cur.execute(schema)
    cur.execute('TRUNCATE TABLE ' + old_df)
    cur.copy_expert(sql, open(df, "r"))

    pg_conn.commit()
    cur.close()

if __name__ == "__main__":
    newData(r'C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc\abc_weekly_' + datetime.datetime.today().strftime("%Y%m%d") + '_final.csv', 'abc_daily_export')
    # newData(r'C:\Users\miche\Desktop\2021_PourSafe\data\city_of_la\crime_la_merged.csv', 'crime_la_merged')

