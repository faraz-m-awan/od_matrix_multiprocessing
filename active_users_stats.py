import pandas as pd
import multiprocessing
from multiprocessing import Pool, Manager
from datetime import datetime
import os
import psycopg2


def getQueries(year):

    queries = []
    for m in range(1, 13):
        queries.append(
            f"""
            SELECT hashed_user_id as uid,COUNT(DISTINCT date_trunc('day', sdk_ts)) AS m{m}
            FROM db_research_tamoco_by_year.tamoco_{year} 
            WHERE EXTRACT(MONTH FROM sdk_ts) = {m}
            GROUP BY uid
            """
            )
    return queries

def getData(query,_):
    
    dbname = "fa71f"
    username = "fa71f"
    password = os.environ.get('CRED')
    host = "172.20.67.58"
    port = "5432"
    
    print(f'{datetime.now()}: Connecting Database')
    conn= psycopg2.connect(
        dbname=dbname,
        host=host,
        user=username,
        password=password,
        port=port
    )

    cur=conn.cursor()

    
    cur.execute(query)
    result=cur.fetchall()
    print(f'{datetime.now()}: Batch Size={len(result)}')
    cols=[desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return pd.DataFrame(result,columns=cols)

if __name__ == '__main__':
    
    data_provider = 'Tamoco' # Tamoco
    year=2019



    print(f'{datetime.now()}: Starting Process')
    print(f'{datetime.now()}: Building Queries')
    args=getQueries(2019)
    args=[(query,1) for query in args]
    print(f'{datetime.now()}: Queries Built')
    print(f'{datetime.now()}: Fetching Data')
    with Pool(8) as p:
        results=p.starmap(getData, args)
    results=[*results]
    print(f'{datetime.now()}: Data Fetched')
    print(f'{datetime.now()}: Merging Data')
    df=results[0]
    for i in range(1,len(results)):
        df=pd.merge(df,results[i],on='uid',how='outer')
    print(f'{datetime.now()}: Data Merged')

    print(f'{datetime.now()}: Saving Data')
    
    path=f'U:\Projects\{data_provider}\Faraz\\final_OD_work\\{year}'
    fname=f'active_users_stats_{data_provider}_{year}.csv'

    if not os.path.exists(path):
        os.makedirs(path)
    df.to_csv(os.path.join(path,fname),index=False)

    