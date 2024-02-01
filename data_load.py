import psycopg2
import os
import pandas as pd

def fetchDataFromDB(start_year,end_year, start_month,end_month, start_day, end_day):
    dbname = 'huq'
    user = 'fa71f'
    password = os.environ.get('CRED')
    host='172.20.67.43'
    port = '5432'

    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    cur = conn.cursor()

    
    
    start_date = f"{start_year}-{start_month:02d}-{start_day:02d} "
    end_date = f"{end_year}-{end_month:02d}-{end_day:02d} "



    cur.execute(
        f"""
        SELECT timestamp as datetime, device_iid_hash as uid, impression_lat as lat, impression_lng as lng, impression_acc
        FROM by_year.huq_gla_{start_year}_v1_2
        WHERE timestamp >= '{start_date}' and timestamp <'{end_date}'
        """
    )

    results = cur.fetchall()
    cols = [desc[0] for desc in cur.description]

    cur.close()
    conn.close()

    return pd.DataFrame(results, columns=cols)

def fetchData(start_year, end_year, start_month, end_month, start_day, end_day):
    return fetchDataFromDB(start_year,end_year, start_month, end_month,start_day, end_day)



