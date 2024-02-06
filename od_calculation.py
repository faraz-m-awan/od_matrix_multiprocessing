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
import multiprocessing
####################################################

#         Customized Modules                       #

####################################################
from data_load import fetchDataFromDB, fetchData
from impression_filtering import getFilteredData,filter_data_process
from stop_node_detection import getStopNodes, stop_node_process
from flow_generation import generateFlow, processFlowGenration

###########################################








class ODCalculation():

    year=2020
    month=[i for i in range(1,13)] #[i for i in range(1,13)] month number | ['all']
    radius=500
    time_th=5
    impr_acc=500
    cpu_cores=8 # Cores to be used for multiprocessing


    def __init__(self_):

        


        return

    def getLoadBalancedBuckets(self_,tdf,bucket_size):
        
        print(f'{datetime.now()}: Getting unique users')
        unique_users=tdf['uid'].unique()
        
        print(f'{datetime.now()}: Creating sets')
        num_impr_df=pd.DataFrame(tdf.groupby('uid').size(),columns=['num_impressions']).reset_index().sort_values(by=['num_impressions'],ascending=False)
      

        buckets={}
        bucket_sums={}
        
        for i in range (1,bucket_size+1):
            buckets[i]=[]
            bucket_sums[i]=0

        
        # Allocate users to buckets
        for _, row in num_impr_df.iterrows():
            user, impressions = row['uid'], row['num_impressions']
            # Find the bucket with the minimum sum of impressions
            min_bucket = min(bucket_sums, key=bucket_sums.get)
            # Add user to this bucket
            buckets[min_bucket].append(user)
            # Update the sum of impressions for this bucket
            bucket_sums[min_bucket] += impressions


        print(f'{datetime.now()}: Creating seperate dataframes')

        tdf_collection=[]
        for i in range (1, bucket_size+1):
            tdf_collection.append(tdf[tdf['uid'].isin(buckets[i])].copy())

        return tdf_collection
    

    def getDatesDivision(self_,month):

        args=[]
        window_size=3
        start_month=month
        end_month=month
        start_date=1
        end_date=start_date+window_size
        start_year=obj.year
        end_year=obj.year

        for i in range(0,self_.cpu_cores):
            
            args.append((start_year,end_year,start_month,end_month,start_date,end_date))

            if i==0:
                window_size=4

            if i==6:
                
                if start_month==12:

                    end_year=end_year+1
                    end_month=1

                    start_date=end_date
                    end_date=1
            
                else:    
                    
                    end_month+=1
                    start_date=end_date
                    end_date=1
            

            else:
                start_date=end_date
                end_date=start_date+window_size



        return args
    
    def getMonthlyDivsion(self_):


        args=[]
        window_size=2
        start_year=self_.year
        end_year=self_.year 
        start_month=1   # 2019-01-01
        end_month=start_month+window_size
        start_date=1
        end_date=1
        

        for i in range(1,7): # Using only 6 cores

            if i==1:
                start_year=self_.year
                end_year=self_.year 
                
                start_month=1  
                end_month=start_month+window_size

                start_date=1
                end_date=1
            elif i==6:
                
                start_month=end_month
                end_month=1
                end_year+=1
            else:
                start_month=end_month
                end_month=start_month+window_size
            
            args.append((start_year,end_year,start_month,end_month,start_date,end_date))

            


        return args

    def saveFile(self_,path,fname,df):
        
        #path=f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{year}\\na_flows'
        #fname=f'na_flows_{radius}_{year}.csv'

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

        print(f'{start_time}: Fetching data from Database')
        
            

        args=[]

        if month=='all':
            args=obj.getMonthlyDivsion()
            
            print(f'{args}')
            with multiprocessing.Pool(6) as pool:
                results = pool.starmap(fetchData, args)

            print(f'{datetime.now()}: Data Concatination')
            result1, result2, result3, result4,result5, result6 = results
            
            obj.raw_df=pd.concat([result1, result2, result3, result4,result5, result6])
            print(f'{datetime.now()}: Data Concatination Completed')
        else:
            args=obj.getDatesDivision(month)
            print(f'{args}')
            with multiprocessing.Pool(obj.cpu_cores) as pool:
                results = pool.starmap(fetchData, args)

            print(f'{datetime.now()}: Data Concatination')
            result1, result2, result3, result4,result5, result6, result7, result8 = results
            
            obj.raw_df=pd.concat([result1,result2,result3,result4,result5,result6,result7,result8])
            print(f'{datetime.now()}: Data Concatination Completed')


        

        
        
        #obj.raw_df=pd.concat([future1.result(),future2.result(),future3.result(),future1.result()])
        print(f'{datetime.now()}: Data fetching completed\n\n')
        print(f'Number of Records: {obj.raw_df.shape[0]}')

        # Converting Raw DataFrame into a Trajectory DataFrame
        
        traj_df= TrajDataFrame(obj.raw_df, latitude='lat',longitude='lng',user_id='uid',datetime='datetime') # Coverting raw data into a trajectory dataframe


        tdf_collection= obj.getLoadBalancedBuckets(traj_df,obj.cpu_cores)

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

        result1, result2, result3, result4,result5, result6, result7, result8 = results

        stdf=pd.DataFrame(pd.concat([result1,result2,result3,result4,result5,result6,result7,result8]))

        

        print(f'{datetime.now()} Stop Node Detection Completed\n')

        #fname=f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{obj.year}\\huq_stop_nodes_{obj.year}_{month}_{obj.radius}m_{obj.time_th}min_{obj.impr_acc}m.csv'

        #stdf.to_csv(fname,index=False)

        obj.saveFile(
            path=f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{obj.year}\\stop_nodes',
            fname=f'huq_stop_nodes_{obj.year}_{month}_{obj.radius}m_{obj.time_th}min_{obj.impr_acc}m.csv',
            df=stdf
        )
        


        ##################################################################################
        #                                                                                #
        #                           Flow Generation                                      #
        #                                                                                #
        ##################################################################################
        #print('Starting')
        #stdf=pd.read_csv(f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{obj.year}\\stop_nodes\\huq_stop_nodes_{obj.year}_{month}_{obj.radius}m_5min_100m.csv',parse_dates=['datetime','leaving_datetime']) #Delete
        #temp_users=list(pd.read_csv('D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\2022\\test_users.csv')['uid'].values) #Delete

        #stdf=stdf[stdf['uid'].isin(temp_users)] #Delete
        #obj.raw_df=obj.raw_df[obj.raw_df['uid'].isin(temp_users)] #Delete


        stdf.rename(columns={'lat':'org_lat','lng':'org_lng'},inplace=True)
        stdf['dest_at']=stdf.groupby('uid')['datetime'].transform(lambda x: x.shift(-1))
        stdf['dest_lat']=stdf.groupby('uid')['org_lat'].transform(lambda x: x.shift(-1))
        stdf['dest_lng']=stdf.groupby('uid')['org_lng'].transform(lambda x: x.shift(-1))
        stdf=stdf.dropna(subset=['dest_lat'])

        #Indexing Raw Data
        obj.raw_df.set_index(['uid','datetime'],inplace=True)
        obj.raw_df.sort_index(inplace=True)




        

        
        tdf_collection= obj.getLoadBalancedBuckets(stdf,obj.cpu_cores)

        print(f'{datetime.now()}: Generating args')
        args=[]
        #[(tdf,obj.raw_df[obj.raw_df['uid'].isin(tdf['uid'].unique())]) for tdf in tdf_collection]
        for tdf in tdf_collection:
            args.append(
                (
                    tdf,
                    obj.raw_df.copy()
                )
            )
        print(f'{datetime.now()}: args Generation Completed')


        print(f'{datetime.now()}: Flow Generation Started\n\n')
        with multiprocessing.Pool(obj.cpu_cores) as pool:
            results = pool.starmap(generateFlow, args)

        result1, result2, result3, result4,result5, result6, result7, result8 = results

        flow_df=pd.concat([result1,result2,result3,result4,result5,result6,result7,result8])

        print(f'{datetime.now()} Flow Generation Completed\n')

        #flow_df.to_csv(f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{obj.year}\\trips\huq_trips_{obj.year}_{month}_{obj.radius}m_{obj.time_th}min_{obj.impr_acc}m.csv',index=False)


        obj.saveFile(
            path=f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{obj.year}\\trips',
            fname=f'huq_trips_{obj.year}_{month}_{obj.radius}m_{obj.time_th}min_{obj.impr_acc}m.csv',
            df=flow_df
        )

        end_time=datetime.now()

        print(f'{end_time}: Processed Completed')
        print(f'\n\nTotal Time Taken: {(end_time-start_time).total_seconds()/60} minutes')
        ##################################################################################
        #                                                                                #
        #                           Trips Extrapolation                                  #
        #                                                                                #
        ##################################################################################





        

        