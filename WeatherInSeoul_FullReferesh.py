# Imports
import psycopg2
import json
import datetime
import pandas as pd
from pandas import json_normalize
from datetime import datetime

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
    

def transform(json_data):
  info = json.loads(json_data)
  df = json_normalize(info['daily'])
  df['dt'] = pd.to_datetime(df['dt'], unit='s')
  result_df = pd.DataFrame(data={'Date' : df['dt'], 'Avg' : df['temp.day'], 'Min' : df['temp.min'], 'Max' : df['temp.max']})
  return result_df



def load(df):
    # cur = get_Redshift_connection()
    # cur.execute(sql)를 사용
    print(df.head)

    table_sql = """
          CREATE TABLE IF NOT EXISTS lsy141.weather_full_refresh (
            w_date date,
            t_day float,
            t_min float,
            t_max float
          );
          """

    sql = ""
    for i, daily_data in df.iterrows():
        sql = sql + "INSERT INTO lsy141.weather_full_refresh (w_date, t_day, t_min, t_max) values('{w_date}',{t_day}, {t_min}, {t_max});\n".format(
             w_date = daily_data[0].strftime('%Y-%m-%d'),
             t_day = str(daily_data[1]),
             t_min = str(daily_data[2]),
             t_max = str(daily_data[3])
        )

    try:
      conn = get_Redshift_connection()
      cur = conn.cursor()
      print("create table");
      cur.execute(table_sql)
      print("full refresh");
      cur.execute("DELETE FROM lsy141.weather_full_refresh;")
      print("insert data");
      cur.execute(sql)

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
