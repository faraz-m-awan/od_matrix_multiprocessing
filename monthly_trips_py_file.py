import pandas as pd
import geopandas as gpd
from shapely.geometry import  Point
import os
from os.path import  join
import numpy as np
import json
from datetime import datetime

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

def getScallingDays(month):

    scaling_days=None
    for i in range(1,13):
        if i==2:
            if year%4==0:
                scaling_days=29
            else:
                scaling_days=28
        elif i==1 or i==3 or i==5 or i==7 or i==8 or i==10 or i==12:
            scaling_days=31
        else:
            scaling_days=30
        return scaling_days

shape_file=f'D:\Mobile Device Data\Boundries\latest_boundries\\all_processed_boundries\\all_boundaries_gcr.gpkg'
shape=gpd.read_file(shape_file)
shape=shape.to_crs('EPSG:4326')
shape.sindex

years=[2019,2020,2021]



dataset='tamoco' # 'tamoco' or 'huq'
radius=500
geography_level='council'

for year in years:
    if year==2019:
        start_month=1
    else:
        start_month=1
    months=[i for i in range(start_month,13)]
    for month in months:
        # calculate the value of quarter based on months value 
        if 1 <= month <= 3:
            quarter = 1
        elif 4 <= month <= 6:
            quarter = 2
        elif 7 <= month <= 9:
            quarter = 3
        elif 10 <= month <= 12:
            quarter = 4


        # gemerate scalling_days for each month
        scaling_days=getScallingDays(month)

        print(f'{datetime.now()} - Processing {year} - {month} - {quarter} - {scaling_days} - {dataset} - {radius} - {geography_level}')

        # Reading Trip Data
        print(f'{datetime.now()} - Reading Trip Data')
        if year==2021 or dataset=='tamoco':
            root=f'U:\\Projects\\Tamoco\Faraz\\final_OD_work\\{year}\\trips'
            files=[f'{root}\\{f}' for f in os.listdir(root) if str(radius) in f and f.split('_')[3]==f'{month}']
            df=[]
            for file in files:
                df.append(pd.read_csv(file,parse_dates=['org_arival_time','org_leaving_time','dest_arival_time']))

            df=pd.concat(df).reset_index(drop=True)
            
            df['org_arival_time']=pd.to_datetime(df['org_arival_time'],format='mixed')
            df['org_leaving_time']=pd.to_datetime(df['org_leaving_time'],format='mixed')
            df['dest_arival_time']=pd.to_datetime(df['dest_arival_time'],format='mixed')
            print(df.dtypes)
        else:
            fname=f'U:\\Projects\\Huq\\Faraz\\final_OD_work\\{year}\\trips\\huq_trips_{year}_all_{radius}m_5min_100m.csv'#f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{year}\\trips\\huq_trips_{year}_all_{radius}m_5min_100m.csv'
            df=pd.read_csv(fname,parse_dates=['org_arival_time','org_leaving_time','dest_arival_time'])

        print(df.dtypes)

        # Spatial join for Origin
        print(f'{datetime.now()} - Spatial join for Origin')
        
        geometry=[Point(xy) for xy in zip(df['org_lng'],df['org_lat'])]
        sp_joined_df=gpd.GeoDataFrame(df,geometry=geometry,crs='EPSG:4326')
        sp_joined_df.sindex
        sp_joined_df=gpd.sjoin(sp_joined_df,shape[['council_area_name','geometry']],how='left',predicate='intersects')
        sp_joined_df.rename(columns={'council_area_name':'origin'},inplace=True)
        sp_joined_df.drop(columns=['index_right','geometry'],inplace=True)
        sp_joined_df.reset_index(drop=True,inplace=True)
        sp_joined_df=pd.DataFrame(sp_joined_df)


        # Spatial join for Destination
        print(f'{datetime.now()} - Spatial join for Destination')
        geometry=[Point(xy) for xy in zip(sp_joined_df['dest_lng'],sp_joined_df['dest_lat'])]
        sp_joined_df=gpd.GeoDataFrame(sp_joined_df,geometry=geometry,crs='EPSG:4326')
        sp_joined_df.sindex
        sp_joined_df=gpd.sjoin(sp_joined_df,shape[['council_area_name','geometry']],how='left',predicate='intersects')
        sp_joined_df.rename(columns={'council_area_name':'destination'},inplace=True)
        sp_joined_df.drop(columns=['index_right','geometry'],inplace=True)
        sp_joined_df.reset_index(drop=True,inplace=True)
        sp_joined_df=pd.DataFrame(sp_joined_df)



        geo_df=sp_joined_df.copy()

        # Filtering trips based on travel time and stay duration
        print(f'{datetime.now()} - Filtering trips based on travel time and stay duration')
        geo_df=geo_df[(geo_df['dest_arival_time']-geo_df['org_leaving_time']).dt.total_seconds()/3600<=24]
        geo_df=geo_df[geo_df['stay_duration']<=3600]
        geo_df['origin'].fillna('Others',inplace=True)
        geo_df['destination'].fillna('Others',inplace=True)

        geo_df=geo_df[geo_df['origin']!='Others']
        geo_df=geo_df[geo_df['destination']!='Others']


        # Adding Trip ID
        geo_df=geo_df.assign(trip_id=lambda df: df.groupby(['uid'])['trip_time'].transform(lambda x: [i for i in range(1,len(x)+1)]))
        geo_df=geo_df[['uid', 'trip_id','org_lat', 'org_lng', 'org_arival_time', 'org_leaving_time',
            'dest_lat', 'dest_lng', 'dest_arival_time', 'stay_points',
            'trip_points', 'trip_time', 'stay_duration', 'observed_stay_duration',
            'origin', 'destination']]



        # Calculate Total Trips/User
        geo_df=geo_df.assign(total_trips=lambda df: df.groupby('uid')['trip_id'].transform(lambda x: len(x)))


        # Add Quarter
        geo_df['month']=geo_df['org_leaving_time'].dt.month
        geo_df['quarter']=geo_df.groupby(['uid','month'])['month'].transform(getQuarter)
        geo_df.drop(columns=['month'],inplace=True) 


        # Fetch Quarter Data (Monthly in case of Tamoco)
        geo_df=geo_df[geo_df['quarter']==quarter]


        # Add Trips/Active Day
        active_day_df=pd.read_csv(f'U:\\Projects\\{dataset}\\Faraz\\final_OD_work\\{year}\\active_days_stat_{year}.csv') #f'D:\Mobile Device Data\OD_calculation_latest_work\HUQ_OD\\{year}\\active_days_stat_{year}.csv'
        active_day_df=active_day_df[active_day_df[f'm{month}']>2]
        active_day_df=active_day_df[['uid',f'm{month}']]



        geo_df=geo_df[geo_df['uid'].isin(active_day_df['uid'])]
        geo_df=(
            geo_df.merge(active_day_df,how='left',left_on='uid',right_on='uid')
            #.assign(tpad=lambda tdf: tdf['total_quarter_trips']/tdf[f'active_days_q{quarter}'])
            .assign(tpad=lambda tdf: tdf['total_trips']/tdf[f'm{month}'])
            )


        # Add Year and Distance Threshold (radius)
        geo_df=geo_df.assign(year=year,distance_threshold=radius)


        # Add SIMD Level
        print(f'{datetime.now()} - Adding SIMD Level')
        hlfile=f'U:\\Projects\\{dataset.title()}\\Faraz\\final_OD_work\\homelocations\\homelocations_{dataset}_{year}_subset_joined.csv'
        hldf=pd.read_csv(hlfile)

        geo_df=(
            geo_df.merge(hldf[['Device_iid_hash','simd_quintile']],left_on='uid', right_on='Device_iid_hash',how='left')
            .drop(columns=['Device_iid_hash'])[[
                'year',
                'quarter',
                'distance_threshold',
                'uid', 
                'simd_quintile',
                'trip_id',
                'org_lat',
                'org_lng',
                'org_arival_time',
                'org_leaving_time', 
                'dest_lat', 
                'dest_lng',
                'origin',
                'destination',
                'dest_arival_time',
                'stay_points',
                'trip_points',
                'trip_time',
                'stay_duration',
            'observed_stay_duration', 
            'total_trips',
            #'total_quarter_trips', 
            #'active_days_q1', 
            #'active_days_q2',
            #'active_days_q3', 
            #'active_days_q4', 
            #'total_active_days', 
            f'm{month}',
            'tpad'
            ]]
        )


        # Add Travel Mode Placeholder
        geo_df=geo_df.assign(travel_mode=np.nan)

        # Filtering based on number of active days and trips/active day
        geo_df=geo_df[(geo_df[f'm{month}']>=3)&(geo_df['tpad']>=0.2)&(geo_df['tpad']<=7)]

        # Get number of Trips between orgins and destination for individual users
        od_trip_df=pd.DataFrame(geo_df.groupby(['uid','origin','destination']).apply(lambda x: len(x)),columns=['trips']).reset_index()

        od_trip_df=(
            od_trip_df.merge(active_day_df,how='left',left_on='uid',right_on='uid')
            #.assign(tpad=lambda tdf: tdf['trips']/tdf[f'active_days_q{quarter}'])
            .assign(tpad=lambda tdf: tdf['trips']/tdf[f'm{month}'])
            )



        # Weighting and Extrapolation Using Active Days Data
        # Calculate SIMD and Council Weights
        print(f'{datetime.now()} - Calculating Weights Based in Adult Population and Tamoco Population')

        hlfile=f'U:\\Projects\\{dataset.title()}\\Faraz\\final_OD_work\\homelocations\\homelocations_{dataset}_{year}_subset_joined.csv'
        hldf=pd.read_csv(hlfile)

        od_trip_df=pd.merge(od_trip_df,hldf[['Device_iid_hash','council','simd_quintile']],how='left',left_on='uid',right_on='Device_iid_hash').drop(columns=['Device_iid_hash'])

        # Calculating Weights Based in Adult Population and HUQ Population

        adult_population = pd.read_csv(f"D:\Mobile Device Data\OD_calculation_latest_work\\aux_files\\adultpopulation.csv")

        annual_users=(
            od_trip_df.dropna(subset=['simd_quintile'])
            .groupby(['council', 'simd_quintile'])
            .agg(users=('uid', 'nunique'))
            .reset_index()
            .merge(adult_population, left_on=['council', 'simd_quintile'], right_on=['council', 'simd_quintile'], how='left')
            .groupby('council')
            #.apply(lambda group: group.assign(Huq_percent=group['users'] / group['users'].sum()))
            .apply(lambda group: group.assign(tamoco_percent=group['users'] / group['users'].sum()))
            .reset_index(drop=True)
            .assign(simd_weight=lambda df: df['percentage'] / df['tamoco_percent'])
            .groupby('council')
            .apply(lambda group: group.assign(total_pop=group['Total'].sum(), tamoco_pop=group['users'].sum()))
            .reset_index(drop=True)
            .assign(council_weight=lambda df: (df['total_pop'] / df['Total'].sum()) / (df['tamoco_pop'] / df['users'].sum()))
            )



       
    
        annual_users=annual_users[
            ['council', 
            'simd_quintile', 
            'users', 'Total', 
            'percentage',
            #'Huq_percent', 
            'tamoco_percent',
            'total_pop', 
            #'huq_pop', 
            'tamoco_pop',
            'simd_weight', 
            'council_weight']
            ]


        annual_users=annual_users.rename(columns={
            'users':'tamoco_user_simd_level',
            'Total':'adult_pop_simd_level',
            'percentage':'adult_pop_percentage_simd_level',
            'tamoco_percent':'tamoco_users_percentage_simd_level',
            'total_pop':'adult_pop_council_level',
            'tamoco_pop':'tamoco_users_council_level',
        })

        print(annual_users.head())

        # Calculating Weighted and Extrapolated OD Trips
        od_trip_df=od_trip_df.merge(annual_users[['council','simd_quintile','simd_weight','council_weight']],how='left',on=['council','simd_quintile'])

        print(od_trip_df.head())

        od_trip_df['simd_weight']=od_trip_df['simd_weight'].fillna(1)
        od_trip_df['council_weight']=od_trip_df['council_weight'].fillna(1)

        ## Saving Weight File
        od_trip_df[['uid','simd_quintile','simd_weight','council_weight']].to_csv(f'U:\\Projects\\Tamoco\\Faraz\\final_OD_work\\{year}\\od_weights\\user_simd_council_od_weights_month{month}.csv',index=False)

        # Counting Trips with different Weightings
        od_trip_df=od_trip_df.assign(activity_weighted_trips=lambda d: ((d['trips'])/d[f'm{month}'])*scaling_days)
        od_trip_df=od_trip_df.assign(council_weighted_trips=lambda d: (d['trips']*d['simd_weight']*d['council_weight']))
        od_trip_df=od_trip_df.assign(act_cncl_weighted_trips=lambda d: ((d['trips']*d['simd_weight']*d['council_weight'])/d[f'm{month}'])*scaling_days)

        # Final Data Products
        # OD Data
        print(f'{datetime.now()} - Producing OD Data')

        tamoco_population=len(od_trip_df['uid'].unique())
        adult_population=adult_population['Total'].sum()

        od_tdf=od_trip_df.groupby(['origin','destination'])[['trips','activity_weighted_trips','council_weighted_trips','act_cncl_weighted_trips']].sum().round().reset_index()
        od_tdf=od_tdf.assign(year=lambda df: [year for i in range(0,df.shape[0])])
        od_tdf=od_tdf.assign(geography_level=lambda df: ['council_level' for i in range(0,df.shape[0])])
        od_tdf=od_tdf.assign(distance_threshold=lambda df: [radius for i in range(0,df.shape[0])])

        od_tdf=od_tdf[['year','geography_level','distance_threshold','origin','destination','trips','activity_weighted_trips','council_weighted_trips','act_cncl_weighted_trips']]

        # Applying Global Scaling

        trip_col='activity_weighted_trips'
        od_tdf[trip_col]=(od_tdf[trip_col]/tamoco_population)*adult_population

        trip_col='council_weighted_trips'
        od_tdf[trip_col]=(od_tdf[trip_col]/tamoco_population)*adult_population

        trip_col='act_cncl_weighted_trips'
        od_tdf[trip_col]=(od_tdf[trip_col]/tamoco_population)*adult_population

        # Saving OD
        print(f'{datetime.now()} - Saving OD Data')
        path=f'U:\\Projects\\{dataset.title()}\\Faraz\\final_OD_work\\{year}\\od_matrix\\with_tpad_cap_filter'
        fname=f'od_{geography_level}_{radius}m_{year}_m{month}.csv'

        if not os.path.exists(path):
            os.makedirs(path)


        od_tdf.to_csv(join(path,fname),index=False)