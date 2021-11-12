import psycopg2

# Redshift connection 함수
def get_Redshift_connection():
    host = "ssde.cnqux5xggmn5.us-east-2.redshift.amazonaws.com"
    redshift_user = "lsy141"
    redshift_pass = "Lsy141!1"
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=False)
    return conn
    
import requests

def extract(url):
    f = requests.get(url)
    return (f.text)
    
import json
import datetime
import pandas as pd
from pandas import json_normalize

def transform(json_data):
  info = json.loads(json_data)
  df = json_normalize(info['daily'])
  df['dt'] = pd.to_datetime(df['dt'], unit='s')
  result_df = pd.DataFrame(data={'Date' : df['dt'], 'Avg' : df['temp.day'], 'Min' : df['temp.min'], 'Max' : df['temp.max']})
  return result_df
  
from datetime import datetime

def init_origianl_table():
    table_sql = """
      CREATE TABLE IF NOT EXISTS lsy141.weather_incremental (
        w_date date,
        t_day float,
        t_min float,
        t_max float,
        updated timestamp
      );
      """
    return table_sql

def init_temp_table():
    table_sql = """
      CREATE TABLE IF NOT EXISTS lsy141.weather_incremental_temp (
        w_date date,
        t_day float,
        t_min float,
        t_max float,
        updated timestamp default GETDATE()
      );
      """
    return table_sql

def insert_from_original():
  copy_sql = "INSERT INTO lsy141.weather_incremental_temp select * from lsy141.weather_incremental;"
  return copy_sql

def delete_origianl():
  delete_sql = "DELETE FROM lsy141.weather_incremental;"
  return delete_sql

def insert_new(df):

    insert_sql = ""
    for i, daily_data in df.iterrows():
        insert_sql = insert_sql + "INSERT INTO lsy141.weather_incremental_temp (w_date, t_day, t_min, t_max) values('{w_date}',{t_day}, {t_min}, {t_max});\n".format(
             w_date = daily_data[0].strftime('%Y-%m-%d'),
             t_day = str(daily_data[1]),
             t_min = str(daily_data[2]),
             t_max = str(daily_data[3])
        )
    print(insert_sql)
    return insert_sql  

def insert_to_original():
  origianl_sql = """
      INSERT INTO lsy141.weather_incremental 
      SELECT w_date, t_day, t_min, t_max, updated 
      FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY w_date ORDER BY updated DESC) seq FROM lsy141.weather_incremental_temp) WHERE seq = 1;
      """
  return origianl_sql

def delete_temp():
  delete_sql = "DELETE FROM lsy141.weather_incremental_temp;"
  return delete_sql

def load(df):
    # cur = get_Redshift_connection()
    # cur.execute(sql)를 사용

    try:
      conn = get_Redshift_connection()
      cur = conn.cursor()
      cur.execute(init_origianl_table())
      cur.execute(init_temp_table())
      cur.execute(insert_from_original())
      cur.execute(delete_origianl())
      cur.execute(insert_new(df))
      cur.execute(insert_to_original())
      cur.execute(delete_temp())

      conn.commit()

    except conn.Error:
      conn.rollback()

    finally:
      # if conn.is_connected():
      cur.close()
      conn.close()
      
# Seoul
lat = '37.56'
lon = '126.98'
# API Key & URL
APIKey = '66047225f883ca8b14fa08321b414a2b'
url='https://api.openweathermap.org/data/2.5/onecall?lat=' + lat + '&lon=' + lon + '&appid=' + APIKey


json_data = extract(url)       
result_df = transform(json_data)
load(result_df)
