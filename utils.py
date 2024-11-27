
YEAR=2021
CITY='Bristol'
ROOT = f'U:/Operations/SCO/Faraz/huq_compiled/{CITY}/{YEAR}'
OUTPUT_DIR=f'U:\\Projects\\Huq\\Faraz\\final_OD_work_v2' # Output Directory
DB_TYPE='json' #'postgres' | 'json'
MONTH=[i for i in range(1,13)] #[i for i in range(1,13)] month number | ['all']
RADIUS=500 # Radius in meters for Stop Node Detection
TIME_THRESHOLD=5 # Time Threshold in minutes for Stop Node Detection
IMPRESSION_ACCURACY=100 # Impression Accuracy in meters for filtering the data
CPU_CORES=8 # Cores to be used for multiprocessing
