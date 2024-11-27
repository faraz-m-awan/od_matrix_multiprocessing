import pandas as pd
import gzip
import zipfile
import os
from multiprocessing import Pool
from ReadJson import readJsonFiles
import utils
from datetime import datetime
from pathlib import Path


def getLoadBalancedBuckets(tdf:pd.DataFrame,bucket_size:int)-> list: 
        """
        Description:
            Multiprocessing is being used for processing the data for Stop node detection and flow generation. 
            This Funcition devides the data based on the UID and Number of Impressions in a way that load on 
            every processor core being used is well-balanced.

        Parameters:
            tdf (pd.DataFrame): Trajectory DataFrame containing the data to be processed.
            bucket_size (int): Number of CPU Cores to be used for processing the data.

        Returns:
            list: List of Trajectory DataFrames. Each DataFrame will be processed in a seperate core as a seperate process.

        Example:
            >>> getLoadBalancedBuckets(tdf,bucket_size=8)
            [df1,df2,df3,df4,df5,df6,df7,df8]
        """

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

def getActiveDays(df:pd.DataFrame)->pd.DataFrame:
    """
    Description:
        This function calculates the number of active days of a user in a month.

    Parameters:
        df (pd.DataFrame): DataFrame containing the data of a user.

    Returns:
        pd.DataFrame: DataFrame containing the number of active days of a user in a month.

    Example:
        >>> getActiveDays(df)
        uid month total_active_days
        1   1     10
        2   1     15
        3   1     20
    """
    df['datetime']=pd.to_datetime(df['datetime'])
    df['month']=df['datetime'].dt.month
    df['day']=df['datetime'].dt.day
    df = (
        df.drop_duplicates(subset=['uid', 'month', 'day'])
        .groupby(['uid','month'])['day'].size()
        .reset_index()
        .rename(columns={'day': 'total_active_days'})
    )
    return df


if __name__=='__main__':

    root = utils.ROOT
    months = utils.MONTH

    for month in months:
        month_file=[f'{f}' for f in os.listdir(root) if f.split('_')[-1].split('.')[0]==str(month)]
        df=readJsonFiles(root, month_file[0])
        print(f'{datetime.now()}: Generating Activity Stats for month: {month}')
        tdf_collection=getLoadBalancedBuckets(df,utils.CPU_CORES)
        with Pool(utils.CPU_CORES) as p:
            df=p.map(getActiveDays,tdf_collection)
        
        df=pd.concat(df,ignore_index=True)
        df.reset_index(drop=True, inplace=True)
        print(f'{datetime.now()}: Activity Stats for month {month} generated.')
        print(f'{datetime.now()}: Saving Activity Stats for month: {month}')
        op_dir=Path(f'{utils.OUTPUT_DIR}/{utils.CITY}/{utils.YEAR}/activity_stats')
        op_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(f'{op_dir}/activity_stats_{utils.CITY}_{month}.csv', index=False)
        print(f'{datetime.now()}: Activity Stats for month {month} saved.\n\n')
       





