# This is the file in branch that contains the code for implementing OD Calculation at different time periods
import pandas as pd
import geopandas as gpd
from shapely.geometry import  Point
import os
from os.path import  join, isfile
import numpy as np
import json
from datetime import datetime
from spatial_join import performSpatialjoin, spatialJoin

import concurrent.futures
import multiprocessing
from tqdm import tqdm
import matplotlib.pyplot as plt



def getLoadBalancedBuckets(tdf,bucket_size):
        
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

def getQuarter(x):
    months=x.values
    quarters=[]
    for month in months:
        if month>=1 and month<=3:
            quarters.append(1)
        elif month>=4 and month<=6:
            quarters.append(2)
        elif month>=7 and month<=9:
            quarters.append(3)
        elif month>=10 and month<=12:
            quarters.append(4)

    return quarters

def saveFile(path,fname,df):

    #path=f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{year}\\na_flows'
    #fname=f'na_flows_{radius}m_{year}.csv'

    if not os.path.exists(path):
        os.makedirs(path)
        
    
    df.to_csv(join(path,fname),index=False)

    return

def getWeights(geo_df,hldf,adult_population,origin_col,destination_col,active_day_df):
    
    od_trip_df=pd.DataFrame(geo_df.groupby(['uid',origin_col,destination_col]).apply(lambda x: len(x)),columns=['trips']).reset_index() # Get number of Trips between orgins and destination for individual users
    od_trip_df=(
    od_trip_df.merge(active_day_df,how='left',left_on='uid',right_on='uid')
    .assign(tpad=lambda tdf: tdf['trips']/tdf['total_active_days'])
    )

    od_trip_df=pd.merge(od_trip_df,hldf[['Device_iid_hash','council','simd_quintile']],how='left',left_on='uid',right_on='Device_iid_hash').drop(columns=['Device_iid_hash'])
    od_trip_df


    

    od_trip_df.rename(columns={'council':'user_home_location'},inplace=True)

    # Calculating Weights Based in Adult Population and HUQ Population

    annual_users=(
    od_trip_df.dropna(subset=['simd_quintile'])
    .groupby(['user_home_location', 'simd_quintile'])
    .agg(users=('uid', 'nunique'))
    .reset_index()
    .merge(adult_population, left_on=['user_home_location', 'simd_quintile'], right_on=['council', 'simd_quintile'], how='left')
    .groupby('user_home_location',group_keys=True)
    .apply(lambda group: group.assign(Huq_percent=group['users'] / group['users'].sum()))
    .reset_index(drop=True)
    .assign(simd_weight=lambda df: df['percentage'] / df['Huq_percent'])
    .groupby('user_home_location',group_keys=True)
    .apply(lambda group: group.assign(total_pop=group['Total'].sum(), huq_pop=group['users'].sum()))
    .reset_index(drop=True)
    .assign(council_weight=lambda df: (df['total_pop'] / df['Total'].sum()) / (df['huq_pop'] / df['users'].sum()))
    
    )

    annual_users=annual_users[ # Rearranging Columns
        ['council', 
        'simd_quintile', 
        'users', 'Total', 
        'percentage',
        'Huq_percent', 
        'total_pop', 
        'huq_pop', 
        'simd_weight', 
        'council_weight']
        ]
    
    annual_users=annual_users.rename(columns={
        'users':'huq_user_simd_level',
        'Total':'adult_pop_simd_level',
        'percentage':'adult_pop_percentage_simd_level',
        'Huq_percent':'huq_users_percentage_simd_level',
        'total_pop':'adult_pop_council_level',
        'huq_pop':'huq_users_council_level',
    })


    

    od_trip_df=od_trip_df.merge(annual_users[['council','simd_quintile','simd_weight','council_weight']],how='left',left_on=['user_home_location','simd_quintile'],right_on=['council','simd_quintile'],suffixes=['_od','_anu'])
    od_trip_df['simd_weight']=od_trip_df['simd_weight'].fillna(1)
    od_trip_df['council_weight']=od_trip_df['council_weight'].fillna(1)
    od_trip_df['activity_weight']= total_days/od_trip_df['total_active_days']
    return od_trip_df



if __name__=='__main__':

    # Loading Glasgow City Region Shapefile

    start_time=datetime.now()
    print(f'{start_time}: Loading Shape File')

    shape_file=f'D:\Mobile Device Data\Boundries\latest_boundries\\all_processed_boundries\\all_boundaries_gcr.gpkg'
    shape=gpd.read_file(shape_file)

    
    shape=shape.to_crs('EPSG:4326')
    shape.sindex

    print(f'{datetime.now()}: Loading Shape File Finished')

    years=[2019]#,2020,2021,2022]
    rad=[500]

    for year in years:
        for radius in rad:
            #year=2019
            month= 'all'
            #radius=200
            cpu_cores=8
            geography_level='iz'   # oa= Ouput Area | dz= Data Zone | iz= Intermediate Zone | council= Council Level
            weighting_type='annual'     # annual | quarter
            total_days=365              # In terms of Annual Weighting=365 | Quarter weighting = total number of days in a quarter
            save_drived_products=False

            if geography_level=='council':
                origin_col=f'origin_{geography_level}_area_name'
                destination_col=f'destination_{geography_level}_area_name'
            else:
                origin_col=f'origin_{geography_level}_id'
                destination_col=f'destination_{geography_level}_id'


            print(f"""
            <OD Calculation Parameters>
            Year: {year}
            Month: {month}
            Radius: {radius}m
            Geography Level: {geography_level}
            Weighting Type: {weighting_type}
            Total Days: {total_days}
            \n
            """)

            

            


            # Loading Trip Data
            print(f'{datetime.now()}: Loading Trip Data')

            if year==2021:
                df=[]
                root=f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{year}\\trips'
                files=[f'{root}\\{f}' for f in os.listdir(root) if str(radius) in f ]
                print(f'{datetime.now()}: Combining and Loading Trip Data')
                for file in tqdm(files):
                    df.append(pd.read_csv(file,parse_dates=['org_arival_time','org_leaving_time','dest_arival_time']))
                
                df=pd.concat(df)
                print(f'{datetime.now()}: Trip Data Loading Completed')
            else:
                
                fname=f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{year}\\trips\\huq_trips_{year}_{month}_{radius}m_5min_100m.csv' #fname=f'U:\\Projects\\Huq\\Faraz\\final_OD\\{year}\\trips\\huq_trips_{year}_all_{radius}m_5min_100m.csv'  #
                
                #df=pd.read_csv(fname,parse_dates=['org_arival_time','org_leaving_time','dest_arival_time'])
                for tdf in pd.read_csv(fname,parse_dates=['org_arival_time','org_leaving_time','dest_arival_time'],chunksize=10_000):
                    df=tdf
                    break

                print(f'{datetime.now()}: Trip Data Loading Completed')
            
            

            df_collection= getLoadBalancedBuckets(df,cpu_cores)

            # Spatial Join for Origin

            print(f'{datetime.now()}: Spatial Join for Origin Started')

            args=[(tdf,shape,'org_lng','org_lat','origin') for tdf in df_collection]
            with multiprocessing.Pool(cpu_cores) as pool:
                results = pool.starmap(performSpatialjoin, args)

            result1, result2, result3, result4,result5, result6, result7, result8 = results
            
            #traj_df=pd.concat([result1,result2,result3,result4,result5,result6,result7,result8])
            df_collection=[result1,result2,result3,result4,result5,result6,result7,result8]

            print(f'{datetime.now()}: Spatial Join for Origin Finished')



            
            # Spatial Join for Destination
            
            print(f'{datetime.now()}: Spatial Join for Destination Started')

            args=[(tdf,shape,'dest_lng','dest_lat','destination') for tdf in df_collection]
            with multiprocessing.Pool(cpu_cores) as pool:
                results = pool.starmap(performSpatialjoin, args)

            result1, result2, result3, result4,result5, result6, result7, result8 = results
            
            geo_df=pd.concat([result1,result2,result3,result4,result5,result6,result7,result8])
            

            print(f'{datetime.now()}: Spatial Join for Destination Finished')


            # Filtering trips based on travel time and stay duration

            print(f'{datetime.now()}: Filtering on Travel Time and Stay Duration')

            geo_df=geo_df[(geo_df['dest_arival_time']-geo_df['org_leaving_time']).dt.total_seconds()/3600<=24]
            geo_df=geo_df[geo_df['stay_duration']<=3600]

            geo_df['origin_oa_id'].fillna('Others',inplace=True)
            geo_df['destination_oa_id'].fillna('Others',inplace=True)

            geo_df['origin_dz_id'].fillna('Others',inplace=True)
            geo_df['destination_dz_id'].fillna('Others',inplace=True)

            geo_df['origin_iz_id'].fillna('Others',inplace=True)
            geo_df['destination_iz_id'].fillna('Others',inplace=True)

            geo_df['origin_council_area_id'].fillna('Others',inplace=True)
            geo_df['destination_council_area_id'].fillna('Others',inplace=True)

            geo_df['origin_council_area_name'].fillna('Others',inplace=True)
            geo_df['destination_council_area_name'].fillna('Others',inplace=True)

            geo_df=geo_df[geo_df[origin_col]!='Others']
            geo_df=geo_df[geo_df[destination_col]!='Others']

            print(f'{datetime.now()}: Filtering Completed')


            #############################################################
            #                                                           #
            #                   Analysis                                #
            #                                                           #
            #############################################################

            # print(f'{datetime.now()}: Generating file for disclosure analysis')
            # analysis_df=geo_df.groupby([origin_col,destination_col]).agg(
            #    total_trips=pd.NamedAgg(column='uid',aggfunc='count'),
            #    num_users=pd.NamedAgg(column='uid',aggfunc='nunique')
            #    ).reset_index()

            # print(f'{datetime.now()}: Saving disclosure analysis file')
            # analysis_df.to_csv(f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{year}\\trip_analysis_{geography_level}_{radius}m_{year}.csv')
            # print(f'{datetime.now()}: Done')

            # Adding Trip ID

            print(f'{datetime.now()}: Adding Trip ID')

            geo_df=geo_df.assign(trip_id=lambda df: df.groupby(['uid'])['trip_time'].transform(lambda x: [i for i in range(1,len(x)+1)]))


            geo_df=geo_df[['uid','trip_id', 'org_lat', 'org_lng', 'org_arival_time', 'org_leaving_time',
            'dest_lat', 'dest_lng', 'dest_arival_time', 'stay_points',
            'trip_points', 'trip_time', 'stay_duration', 'observed_stay_duration', 
            'origin_oa_id','destination_oa_id', 'origin_dz_id','destination_dz_id', 'origin_iz_id', 'destination_iz_id',
            'origin_council_area_id','destination_council_area_id', 'origin_council_area_name','destination_council_area_name']]

                

            print(f'{datetime.now()}: Trip ID Added')


            # Calculate Total Trips/User

            print(f'{datetime.now()}: Calculating Total Trips/User')
            geo_df['month']=geo_df['org_leaving_time'].dt.month
            geo_df=geo_df.assign(total_trips=lambda df: df.groupby('uid')['trip_id'].transform(lambda x: len(x)))
            geo_df['quarter']=geo_df.groupby(['uid','month'])['month'].transform(getQuarter)

            geo_df.drop(columns=['month'],inplace=True)

            print(f'{datetime.now()}: Trips/User Calculated')

            # Add Trips/Active Day


            #active_day_df=pd.read_csv(f'U:\\Projects\\Huq\\Faraz\\latest_OD\\{year}\\active_days_stat_{year}.csv') #f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{year}\\active_days_stat_{year}.csv'

            print(f'{datetime.now()}: Calculating TPAD')



            active_day_df=pd.read_csv(f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{year}\\active_days_stat_{year}.csv')

            geo_df=(
                geo_df.merge(active_day_df,how='left',left_on='uid',right_on='uid')
                .assign(tpad=lambda tdf: tdf['total_trips']/tdf['total_active_days'])
                )
            print(f'{datetime.now()}: TPAD Calculated')

            # Add Year and Distance Threshold (radius)
            geo_df=geo_df.assign(year=year,distance_threshold=radius)
            print(f'{datetime.now()}: Year and Distance Threshold Added')

            # Add SIMD Level

            print(f'{datetime.now()}: Adding SIMD')

            hlfile=f'D:\Mobile Device Data\OD_calculation_latest_work\\aux_files\homelocations_huq_{year}_subset_joined.csv'
            hldf=pd.read_csv(hlfile)

            geo_df=(
                geo_df.merge(hldf[['Device_iid_hash','council','simd_quintile']],left_on='uid', right_on='Device_iid_hash',how='left')
                .drop(columns=['Device_iid_hash'])[[
                    'year',
                    'distance_threshold',
                    'uid',
                    'council', 
                    'simd_quintile',
                    'trip_id',
                    'org_lat',
                    'org_lng',
                    'org_arival_time',
                    'org_leaving_time', 
                    'dest_lat', 
                    'dest_lng',
                    'origin_oa_id','destination_oa_id', 
                    'origin_dz_id','destination_dz_id', 
                    'origin_iz_id', 'destination_iz_id',
                    'origin_council_area_id','destination_council_area_id', 
                    'origin_council_area_name','destination_council_area_name',
                    'dest_arival_time',
                    'stay_points',
                    'trip_points',
                    'trip_time',
                    'stay_duration',
                    'observed_stay_duration', 
                    'total_trips',
                    'total_active_days', 
                    'tpad'
                ]]
            )

        

            # # Add Travel Mode Placeholder
            # geo_df=geo_df.assign(travel_mode=np.nan)

            # if save_drived_products:

            #     # Save Non-Aggregated OD Flow
            #     print(f'{datetime.now()}: Saving Non-Aggregated OD Flow')
            #     saveFile(
            #         path=f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{year}\\na_flows',
            #         fname=f'na_flows_{radius}m_{year}.csv',
            #         df=geo_df[[
            #             'year',
            #             'distance_threshold',
            #             'uid', 
            #             'simd_quintile',
            #             'trip_id',
            #             'org_lat',
            #             'org_lng',
            #             'org_arival_time',
            #             'org_leaving_time', 
            #             'dest_lat', 
            #             'dest_lng',
            #             'dest_arival_time',
            #             'total_trips',
            #             'total_active_days',
            #             'tpad',
            #             'travel_mode']]
            #         )
            #     print(f'{datetime.now()}:  Non-Aggregated OD Flow Saved')


            #     # Save Output Area Aggregated Flow

            #     print(f'{datetime.now()}: Saving OA-Aggregated OD Flow')

            #     saveFile(
            #         path=f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{year}\\oa_agg_stay_points',
            #         fname=f'non_agg_stay_points_{radius}m_{year}.csv',
            #         df=geo_df[
            #             [
            #             'year', 
            #             'distance_threshold', 
            #             'origin_oa_id',
            #             'destination_oa_id',
            #             'org_arival_time', 
            #             'org_leaving_time', 
            #             'dest_arival_time',
            #             'travel_mode'
            #             ]
            #         ]
            #     )
            #     print(f'{datetime.now()}:  OA-Aggregated OD Flow Saved')

            #     # Save Non Aggregated Stay Points

            #     print(f'{datetime.now()}: Saving Non-Aggragated Stay Points')

            #     saveFile(
            #         path=f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{year}\\non_agg_stay_points',
            #         fname=f'non_agg_stay_points_{radius}m_{year}.csv',
            #         df=geo_df[
            #             [
            #                 'year', 
            #                 'distance_threshold', 
            #                 'uid',
            #                 'simd_quintile',
            #                 'stay_points',
            #                 'org_arival_time', 
            #                 'org_leaving_time', 
            #                 'stay_duration',
            #                 'org_lat',
            #                 'org_lng',
            #                 'total_active_days'
            #                 ]].rename(
            #                     columns={
            #                         'org_lat':'centroid_lat', # Changing the name to centroid because stay points don't have origin and destination
            #                         'org_lng':'centroid_lng',
            #                         'org_arival_time':'stop_node_arival_time',
            #                         'org_leaving_time':'stop_node_leaving_time'
            #                         }
            #                     )
            #     )

            #     print(f'{datetime.now()}: Non-Aggragated Stay Points Saved')


            #     # Save Aggregated Stay Points

            #     print(f'{datetime.now()}: Saving Aggragated Stay Points')

            #     saveFile(
            #         path=f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{year}\\agg_stay_points',
            #         fname=f'agg_stay_points_{radius}m_{year}.csv',
            #         df=geo_df[
            #             [
            #             'year', 
            #             'distance_threshold', 
            #             'simd_quintile',
            #             'origin_oa_id',
            #             'org_arival_time', 
            #             'org_leaving_time', 
            #             'stay_duration'
            #             ]].rename(
            #                 columns={
            #                     'org_arival_time':'stop_node_arival_time',
            #                     'org_leaving_time' :'stop_node_leaving_time',
            #                     'origin_oa_id' : 'stop_node_oa'
            #                     }
            #                 )
            #     )

            #     print(f'{datetime.now()}: Aggragated Stay Points Saved')


            #     # Save Trip Points
            #     print(f'{datetime.now()}: Saving Trips Points')

            #     saveFile(
            #         path=f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{year}\\trip_points',
            #         fname=f'trip_points_{radius}m_{year}.csv',
            #         df=geo_df[
            #             [
            #                 'year', 
            #                 'distance_threshold', 
            #                 'uid',
            #                 'simd_quintile',
            #                 'trip_id',
            #                 'trip_points',
            #                 'total_active_days',
            #                 'travel_mode'
            #             ]
            #         ]
            #     )

            #     print(f'{datetime.now()}: Trips Points Saved')



            ##################################################################################
            #                                                                                #
            #                           OD Generation                                        #
            #                                                                                #
            ##################################################################################

            print(f'{datetime.now()}: OD Calculation Started')
            geo_df=geo_df[(geo_df['total_active_days']>=7)&(geo_df['tpad']>=0.2)] # Filtering based on number of active days and trips/active day
            hlfile=f'D:\Mobile Device Data\OD_calculation_latest_work\\aux_files\homelocations_huq_{year}_subset_joined.csv'
            hldf=pd.read_csv(hlfile)
            adult_population = pd.read_csv(f"D:\Mobile Device Data\OD_calculation_latest_work\\aux_files\\adultpopulation.csv") # Reading adult population file

            weighted_trips=getWeights(geo_df,hldf,adult_population,origin_col,destination_col,active_day_df)

            weighted_trips.to_csv(f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\validation\\new_code_version_weighted_trips_{radius}m_{year}.csv',index=False)

            exit()
            t_cols=weighted_trips.columns
            print(geo_df.columns)
            print(weighted_trips.columns)
            od_trip_df=geo_df.merge(weighted_trips[['uid',origin_col,destination_col,'trips','activity_weight','simd_weight','council_weight']],how='left',left_on=['uid',origin_col,destination_col],right_on=['uid',origin_col,destination_col])
            t_cols=od_trip_df.columns

            

            ############################################################################################

            # od_trip_df=pd.DataFrame(geo_df.groupby(['uid',origin_col,destination_col]).apply(lambda x: len(x)),columns=['trips']).reset_index() # Get number of Trips between orgins and destination for individual users

            # od_trip_df=(
            # od_trip_df.merge(active_day_df,how='left',left_on='uid',right_on='uid')
            # .assign(tpad=lambda tdf: tdf['trips']/tdf['total_active_days'])
            # )

            # hlfile=f'D:\Mobile Device Data\OD_calculation_latest_work\\aux_files\homelocations_huq_{year}_subset_joined.csv'
            # hldf=pd.read_csv(hlfile)

            # od_trip_df=pd.merge(od_trip_df,hldf[['Device_iid_hash','council','simd_quintile']],how='left',left_on='uid',right_on='Device_iid_hash').drop(columns=['Device_iid_hash'])
            # od_trip_df


            

            # od_trip_df.rename(columns={'council':'user_home_location'},inplace=True)

            # # Calculating Weights Based in Adult Population and HUQ Population
            # adult_population = pd.read_csv(f"D:\Mobile Device Data\OD_calculation_latest_work\\aux_files\\adultpopulation.csv") # Reading adult population file


            # annual_users=(
            # od_trip_df.dropna(subset=['simd_quintile'])
            # .groupby(['user_home_location', 'simd_quintile'])
            # .agg(users=('uid', 'nunique'))
            # .reset_index()
            # .merge(adult_population, left_on=['user_home_location', 'simd_quintile'], right_on=['council', 'simd_quintile'], how='left')
            # .groupby('user_home_location',group_keys=True)
            # .apply(lambda group: group.assign(Huq_percent=group['users'] / group['users'].sum()))
            # .reset_index(drop=True)
            # .assign(simd_weight=lambda df: df['percentage'] / df['Huq_percent'])
            # .groupby('user_home_location',group_keys=True)
            # .apply(lambda group: group.assign(total_pop=group['Total'].sum(), huq_pop=group['users'].sum()))
            # .reset_index(drop=True)
            # .assign(council_weight=lambda df: (df['total_pop'] / df['Total'].sum()) / (df['huq_pop'] / df['users'].sum()))
            
            # )

            # annual_users=annual_users[ # Rearranging Columns
            #     ['council', 
            #     'simd_quintile', 
            #     'users', 'Total', 
            #     'percentage',
            #     'Huq_percent', 
            #     'total_pop', 
            #     'huq_pop', 
            #     'simd_weight', 
            #     'council_weight']
            #     ]
            
            # annual_users=annual_users.rename(columns={
            #     'users':'huq_user_simd_level',
            #     'Total':'adult_pop_simd_level',
            #     'percentage':'adult_pop_percentage_simd_level',
            #     'Huq_percent':'huq_users_percentage_simd_level',
            #     'total_pop':'adult_pop_council_level',
            #     'huq_pop':'huq_users_council_level',
            # })


            

            # od_trip_df=od_trip_df.merge(annual_users[['council','simd_quintile','simd_weight','council_weight']],how='left',left_on=['user_home_location','simd_quintile'],right_on=['council','simd_quintile'],suffixes=['_od','_anu'])
            # od_trip_df['simd_weight']=od_trip_df['simd_weight'].fillna(1)
            # od_trip_df['council_weight']=od_trip_df['council_weight'].fillna(1)
            # od_trip_df['activity_weight']= total_days/od_trip_df['total_active_days']

            ############################################################################################


            if weighting_type=='annual':
                cols=[
                    'uid', 'user_home_location', 'simd_quintile', origin_col,
                    destination_col, 'trips','activity_weight','simd_weight', 
                    'council_weight'
                    ]


            huq_population= len(od_trip_df['uid'].unique())
            adult_population= adult_population['Total'].sum()

            # Producing 5 Type of OD Matrices
            # Type 1: AM peak weekdays (7am-10am)
            # Type 2: PM peak weekdays (4 pm-7 pm)
            # Type 3: Interpeak weekdays (10 am–2 pm) 
            # Type 4: Interpeak weekends  (10am–2pm)
            # Type 5: Others

            od_type=['type5']#'type1','type2','type3','type4','type5']
            backup_od_trip_df=od_trip_df.copy()
            for typ in od_type:
                od_trip_df=backup_od_trip_df.copy()

                print(f'{datetime.now()}: Generating {typ} OD Matrix')
                
                if typ=='type1':
                    od_trip_df=od_trip_df[(od_trip_df['org_leaving_time'].dt.hour>=7)&(od_trip_df['org_leaving_time'].dt.hour<=10) & (od_trip_df['org_leaving_time'].dt.dayofweek<5)]
                elif typ=='type2':
                    od_trip_df=od_trip_df[(od_trip_df['org_leaving_time'].dt.hour>=16)&(od_trip_df['org_leaving_time'].dt.hour<=19) & (od_trip_df['org_leaving_time'].dt.dayofweek<5)]
                elif typ=='type3':
                    od_trip_df=od_trip_df[(od_trip_df['org_leaving_time'].dt.hour>=10)&(od_trip_df['org_leaving_time'].dt.hour<=14) & (od_trip_df['org_leaving_time'].dt.dayofweek<5)]
                elif typ=='type4':
                    od_trip_df=od_trip_df[(od_trip_df['org_leaving_time'].dt.hour>=10)&(od_trip_df['org_leaving_time'].dt.hour<=14)&(od_trip_df['org_leaving_time'].dt.dayofweek>=5)]





                agg_od_df=od_trip_df.groupby([origin_col,destination_col]).agg(

                    trips=('trips', 'sum'),
                    activity_weighted_trips= ('trips', lambda x: ((x * od_trip_df.loc[x.index,'activity_weight']).sum()/huq_population)*adult_population),
                    council_weighted_trips= ('trips', lambda x: ((x * od_trip_df.loc[x.index,'simd_weight'] * od_trip_df.loc[x.index,'council_weight']).sum()/huq_population)*adult_population),
                    act_cncl_weighted_trips= ('trips', lambda x: ((x * od_trip_df.loc[x.index,'activity_weight'] * od_trip_df.loc[x.index,'simd_weight'] * od_trip_df.loc[x.index,'council_weight']).sum()/huq_population)*adult_population)
                ).reset_index()

                agg_od_df=agg_od_df[agg_od_df[origin_col]!='Others']
                agg_od_df=agg_od_df[agg_od_df[destination_col]!='Others']

                print(f'{datetime.now()}: OD Generation Completed')
                print(f'{datetime.now()}: Saving OD')

                agg_od_df['year']=year
                agg_od_df['distance_threshold']=radius
                agg_od_df['geography_level']=geography_level
                agg_od_df['percentage']= (agg_od_df['act_cncl_weighted_trips']/agg_od_df['act_cncl_weighted_trips'].sum())*100

                agg_od_df=agg_od_df[[
                    'year',
                    'distance_threshold',
                    'geography_level',
                    origin_col,
                    destination_col,
                    'trips',
                    'activity_weighted_trips',
                    'council_weighted_trips',
                    'act_cncl_weighted_trips',
                    'percentage'
                ]]



                
                saveFile(
                    path=f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{year}\od_matrix',
                    fname=f'{typ}_od_{geography_level}_{radius}m_{year}.csv',
                    df=agg_od_df
                )

            end_time=datetime.now()

            print(f'{datetime.now()}: OD Saved')

        end_time=datetime.now()
        print(f'{end_time}: Processed Completed')
        print(f'\n\nTotal Time Taken: {(end_time-start_time).total_seconds()/60} minutes')
    


