import psycopg2
import os
import pandas as pd
from datetime import datetime


def fetchData(query):

    dbname = "huq"
    user = "fa71f"
    password = os.environ.get('CRED')
    host = "172.20.67.57"
    port = "5432"




    try:
        print(f'{datetime.now()}: Connecting Database')
        connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        print(f'{datetime.now()}: Connection Established')
        cur = connection.cursor()

        cur.execute(query)
        result = cur.fetchall()
        print(f'{datetime.now()}: Batch Size={len(result)}')
        cols = [desc[0] for desc in cur.description]

        return pd.DataFrame(result,columns=cols)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if connection:
            connection.close()



