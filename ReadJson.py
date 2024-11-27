import os
import zipfile
import gzip
import json
import pandas as pd
from multiprocessing import Pool
from tqdm import tqdm
from datetime import datetime


def readJsonFiles(root:str, month_file:str)->pd.DataFrame:
    """
    Description:
        This function iterates over the zip files in the given directory and reads the gzip json files within zip files.
    Parameters:
        root (str): Root directory containing the zip files
        month_file (str): Name of the zip file
    Returns:
        pd.DataFrame: DataFrame containing the data from the json files
    Example:
        root = 'U:/Operations/SCO/Faraz/huq_compiled/Manchester/2021'
        month_file = 'huq_manchester_v13_20021_3_part0001.json.gz'
        readJsonFiles(root, month_file)

    """
    print(f'{datetime.now()}: Processing {month_file}')
    data=[]
    month_zip_file = f'{root}/{month_file}' 
    with zipfile.ZipFile(month_zip_file, 'r') as zf: #
        gz_files = zf.namelist()
        if gz_files:
            for gz_file in gz_files:
                print(f'{datetime.now()}: Processing {gz_file}')
                with zf.open(gz_file, 'r') as f:
                    with gzip.open(f, 'rt', encoding='utf-8') as g:
                        lines=g.readlines()
                        for line in lines:
                          
                            temp=json.loads(line.strip())
                            temp = {k: temp[k] for k in ['impression_acc', 'device_iid_hash', 'impression_lng', 'impression_lat', 'timestamp'] if k in temp}
                            try:
                                #temp=pd.DataFrame(temp,index=[0])
                                data.append(temp)
                            except Exception as e:
                                print(temp)
                                print(f'Error: {e}')
                                exit()
    
    del temp
    #data=pd.concat(data, ignore_index=True)
    data=pd.DataFrame(data)
    print(f'{datetime.now()}: {month_file} processed.')
    data.rename(columns={'timestamp': 'datetime', 'device_iid_hash':'uid', 'impression_lat': 'lat', 'impression_lng': 'lng'}, inplace=True)
    return data


if __name__ == '__main__':

    print(f'{datetime.now()}: Starting...')
    city = 'Manchester'
    year = '2021'
    root = f'U:/Operations/SCO/Faraz/huq_compiled/{city}/{year}'
    cores = 5#os.cpu_count()

    month_files=os.listdir(root)
    # pass root and month_files to the function
    args=[(root, mf) for mf in month_files]

    with Pool(cores) as p:
        df=p.starmap(readJsonFiles, args)
        df=pd.concat(df, ignore_index=True)
    print(f'{datetime.now()}: Finished')
    
