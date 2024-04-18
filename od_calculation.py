
# This is  Tamoco OD Calculation branch
import pandas as pd
import numpy as np
from skmob import TrajDataFrame,FlowDataFrame
from skmob.preprocessing import filtering, detection, clustering



import folium
from folium.plugins import FloatImage
from folium import plugins


import psycopg2
import os
from os.path import join, isfile
from tqdm import  tqdm
import json


import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt

from datetime import datetime
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from multiprocessing import Pool, Manager

####################################################
#                                                  #
#         Customized Modules                       #
#                                                  #
####################################################
from data_load import fetchData
from impression_filtering import getFilteredData,filter_data_process
from stop_node_detection import getStopNodes, stop_node_process
from flow_generation import generateFlow, processFlowGenration



class ODCalculation():

    year=2019
    month=[i for i in range(1,13)] #[i for i in range(1,13)] month number | ['all']
    radius=500
    time_th=5
    impr_acc=100
    cpu_cores=8 # Cores to be used for multiprocessing
    data_provider='Tamoco' # Huq | Tamoco

    def __init__(self_):
        return

    def getLoadBalancedBuckets(self_,tdf,bucket_size): # Multiprocessing is being used for processing the data for Stop node detection and flow generation. This Funcition devides the data based on the UID and Number of Impressions in a way that load on every process is well-balanced.
        
        print(f'{datetime.now()}: Getting unique users')
        unique_users=tdf['uid'].unique() # Getting Unique Users in the data
        
        print(f'{datetime.now()}: Creating sets')
        num_impr_df=pd.DataFrame(tdf.groupby('uid').size(),columns=['num_impressions']).reset_index().sort_values(by=['num_impressions'],ascending=False) # Creating a DataFrame containing Unique UID and Total number of impressions that Unique UID has in the data.
      

        buckets={} # A dictionary containing buckets of UIDs. Each bucket represent the CPU core. This dictionary tells how many users' data will be process on which core. For example, if bucket 1 contains 10 UIDs, data of those 10 UIDs will be processed on the core 1.
        bucket_sums={} # A flag dictionary to keep the track of load on each bucket.
        
        for i in range (1,bucket_size+1):
            buckets[i]=[] # Initializing empty buckets
            bucket_sums[i]=0 # Load in each bucket is zero initially

        
        # Allocate users to buckets
        for _, row in num_impr_df.iterrows():
            user, impressions = row['uid'], row['num_impressions'] # Getting the UID and the number of impressions of that UID
            # Find the bucket with the minimum sum of impressions
            min_bucket = min(bucket_sums, key=bucket_sums.get) # Getting the bucket with the minimum load. Initially, all the buckets have zero load.
            # Add user to this bucket
            buckets[min_bucket].append(user) # Adding UID to the minimum bucket
            # Update the sum of impressions for this bucket
            bucket_sums[min_bucket] += impressions # Updating the load value of the bucket. For example, UID 1 was added to Bucket 1 and UID 1 had 1000 impressions (records). So, load of bucket 1 is 1000 now. 


        print(f'{datetime.now()}: Creating seperate dataframes')

        tdf_collection=[] # List of collection of DataFrames. This list will contain the number of DataFrames=number of CPU Cores. Each DataFrame will be processed in a seperate core as a seperate process.
        for i in range (1, bucket_size+1):
            tdf_collection.append(tdf[tdf['uid'].isin(buckets[i])].copy())

        return tdf_collection
    

    

    def getQueriesForAllYearProcessing(self_,year): 

        query=[]

        for month in range(1,13,2): # This loop will run 6 times. Each loop will generate a query for fetching data for two months
            if month==11:
                query.append(
                    f"""
                    SELECT timestamp as datetime, device_iid_hash as uid, impression_lat as lat, impression_lng as lng, impression_acc
                    FROM by_year.huq_gla_{year}_v1_2
                    WHERE timestamp >= '{year}-{month:02d}-01' and timestamp <'{year+1}-{1:02d}-01'
                    """
                    )
            else:
                query.append(
                    f"""
                    SELECT timestamp as datetime, device_iid_hash as uid, impression_lat as lat, impression_lng as lng, impression_acc
                    FROM by_year.huq_gla_{year}_v1_2
                    WHERE timestamp >= '{year}-{month:02d}-01' and timestamp <'{year}-{month+2:02d}-01'
                    """
                    )
                    
        return query
    def getQueriesForMonthlyProcessing(self_,year,month):

        query=[]

        for day in range(1,31,5): # We are using 6 threads to fetch the data. Each month will be devided into 5 date windows. Each Thread will be responsible for fetching data for its own date window.

            if day==26:
                if month==12:
                    query.append(
                    f"""
                    SELECT sdk_ts as datetime, hashed_user_id as uid, latitude as lat, longitude as lng, accuracy as impression_acc
                    FROM db_research_tamoco_by_year.tamoco_{year}
                    WHERE sdk_ts >= '{year}-{month:02d}-{day:02d}' and sdk_ts <'{year+1}-01-01'
                    """
                    )
                else:
                    query.append(
                    f"""
                    SELECT sdk_ts as datetime, hashed_user_id as uid, latitude as lat, longitude as lng, accuracy as impression_acc
                    FROM db_research_tamoco_by_year.tamoco_{year}
                    WHERE sdk_ts >= '{year}-{month:02d}-{day:02d}' and sdk_ts <'{year}-{month+1:02d}-01'
                    """
                    )
            else:
                query.append(
                    f"""
                    SELECT sdk_ts as datetime, hashed_user_id as uid, latitude as lat, longitude as lng, accuracy as impression_acc
                    FROM db_research_tamoco_by_year.tamoco_{year}
                    WHERE sdk_ts >= '{year}-{month:02d}-{day:02d}' and sdk_ts <'{year}-{month:02d}-{day+5:02d}'
                    """
                    )

        return query

    def saveFile(self_,path,fname,df):

        if not os.path.exists(path):
            os.makedirs(path)
            
        
        df.to_csv(join(path,fname),index=False)

        return
   
        

if __name__=='__main__':
    
    
    obj=ODCalculation()
    
    for month in obj.month:

        print(f"""
        <OD Calculation Parameters>
        Year: {obj.year}
        Month: {month}
        Radius: {obj.radius}
        \n
        """)

        start_time=datetime.now()

        
        ##################################################################################
        #                                                                                #
        #                           Fetching Data From DB                                #
        #                                                                                #
        ##################################################################################

        print(f'{start_time}: Fetching data from Database')
        

        if month=='all':
            print('Yearly Processing')
            query=obj.getQueriesForAllYearProcessing(obj.year)
        else:
            print('Monthly Processing')
            query=obj.getQueriesForMonthlyProcessing(obj.year,month)
        
     
        with ThreadPoolExecutor(max_workers=6) as executor:
            results = list(executor.map(fetchData, query))

        print(f'{datetime.now()}: Data Concatination')
        #obj.raw_df=pd.concat(results)
        traj_df=pd.concat(results)
        print(f'{datetime.now()}: Data Concatination Completed')


        print(f'{datetime.now()}: Data fetching completed\n\n')
        print(f'Number of Records: {traj_df.shape[0]}')
    

        # Converting Raw DataFrame into a Trajectory DataFrame
        traj_df= TrajDataFrame(traj_df, latitude='lat',longitude='lng',user_id='uid',datetime='datetime') # Coverting raw data into a trajectory dataframe
        tdf_collection= obj.getLoadBalancedBuckets(traj_df,obj.cpu_cores)


        ##################################################################################
        #                                                                                #
        #                           Filtering Data Based on                              #
        #              Impression Accuracy and Speed Between GPS Points                  #
        #                                                                                #
        ##################################################################################

        print(f'{datetime.now()}: Filtering Started')
        args=[(tdf,obj.impr_acc) for tdf in tdf_collection]
        with multiprocessing.Pool(obj.cpu_cores) as pool:
            results = pool.starmap(filter_data_process, args)

        result1, result2, result3, result4,result5, result6, result7, result8 = results
        traj_df=pd.concat([result1,result2,result3,result4,result5,result6,result7,result8])
        print(f'{datetime.now()}: Filtering Finished\n\n\n')

        ##################################################################################
        #                                                                                #
        #                           Stope Node Detection                                 #
        #                                                                                #
        ##################################################################################

        print(f'{datetime.now()}: Stop Node Detection Started\n\n')
        print(f'Detecting stop nodes for the month: {traj_df.datetime.dt.month.unique().tolist()}')
        print(f'Radius: {obj.radius}\nTime Threshold: {obj.time_th}\nImpression Accuracy: {obj.impr_acc}')
        tdf_collection= obj.getLoadBalancedBuckets(traj_df,obj.cpu_cores)
        print(f'{datetime.now()}: Stop Node Detection Started')
        args=[(tdf,obj.time_th,obj.radius) for tdf in tdf_collection]
        with multiprocessing.Pool(obj.cpu_cores) as pool:
            results = pool.starmap(stop_node_process, args)

        del tdf_collection

        result1, result2, result3, result4,result5, result6, result7, result8 = results
        stdf=pd.DataFrame(pd.concat([result1,result2,result3,result4,result5,result6,result7,result8]))

        print(f'{datetime.now()} Stop Node Detection Completed\n')
        
        # Saving Stop Nodes
        obj.saveFile(
            path=f'U:\\Projects\\{obj.data_provider}\Faraz\\final_OD_work\\{obj.year}\\stop_nodes',
            fname=f'{obj.data_provider.lower()}_stop_nodes_{obj.year}_{month}_{obj.radius}m_{obj.time_th}min_{obj.impr_acc}m.csv',
            #path=f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\validation',
            #fname=f'new_code_val_stop_nodes_{obj.radius}m_{obj.year}.csv',
            df=stdf
        )
        
        #stdf=pd.read_csv(
        #    f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{obj.year}\stop_nodes\\huq_stop_nodes_{obj.year}_all_{obj.radius}m_5min_100m.csv',
        #    parse_dates=['datetime','leaving_datetime'])


        ##################################################################################
        #                                                                                #
        #                           Flow Generation                                      #
        #                                                                                #
        ##################################################################################
 

        stdf.rename(columns={'lat':'org_lat','lng':'org_lng'},inplace=True)
        stdf['dest_at']=stdf.groupby('uid')['datetime'].transform(lambda x: x.shift(-1))
        stdf['dest_lat']=stdf.groupby('uid')['org_lat'].transform(lambda x: x.shift(-1))
        stdf['dest_lng']=stdf.groupby('uid')['org_lng'].transform(lambda x: x.shift(-1))
        stdf=stdf.dropna(subset=['dest_lat'])

        #Indexing Raw Data
        #obj.raw_df.set_index(['uid','datetime'],inplace=True)
        #obj.raw_df.sort_index(inplace=True)      
        tdf_collection= obj.getLoadBalancedBuckets(stdf,obj.cpu_cores)
        
        print(f'{datetime.now()}: Generating args')
        args=[]
        #[(tdf,obj.raw_df[obj.raw_df['uid'].isin(tdf['uid'].unique())]) for tdf in tdf_collection]
        for tdf in tdf_collection:
            #temp_raw_df=obj.raw_df[obj.raw_df['uid'].isin(tdf['uid'].unique())].copy()
            #temp_raw_df.set_index(['uid','datetime'],inplace=True)
            #temp_raw_df.sort_index(inplace=True) 

            temp_raw_df=traj_df[traj_df['uid'].isin(tdf['uid'].unique())].copy()
            temp_raw_df.set_index(['uid','datetime'],inplace=True)
            temp_raw_df.sort_index(inplace=True) 
            args.append((tdf,temp_raw_df))

        
        
        del tdf_collection

        print(f'{datetime.now()}: args Generation Completed')
        print(f'{datetime.now()}: Flow Generation Started\n\n')
        with multiprocessing.Pool(obj.cpu_cores) as pool:
            results = pool.starmap(generateFlow, args)

        result1, result2, result3, result4,result5, result6, result7, result8 = results
        flow_df=pd.concat([result1,result2,result3,result4,result5,result6,result7,result8])
        print(f'{datetime.now()} Flow Generation Completed\n')

        # Saving Flow
        obj.saveFile(
            path=f'U:\\Projects\\{obj.data_provider}\Faraz\\final_OD_work\\{obj.year}\\trips',
            fname=f'{obj.data_provider.lower()}_trips_{obj.year}_{month}_{obj.radius}m_{obj.time_th}min_{obj.impr_acc}m.csv',
            #path=f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\validation',
            #fname=f'new_code_val_trips_{obj.radius}m_{obj.year}.csv',
            df=flow_df
        )

        end_time=datetime.now()
        print(f'{end_time}: Process Completed')
        print(f'\n\nTotal Time Taken: {(end_time-start_time).total_seconds()/60} minutes')
        ##################################################################################
        #                                                                                #
        #                           Trips Extrapolation                                  #
        #                                                                                #
        ##################################################################################
        