
from skmob.preprocessing import filtering, detection, clustering


def getFilteredData(traj_df,impr_acc):

    
    #traj_df=traj_df[traj_df['uid'].isin(user_set)]

    print(f"Filtering based on impression accuracy={impr_acc}")
    bf=traj_df.shape[0]
    traj_df=traj_df[traj_df.impression_acc<=impr_acc]
    af=traj_df.shape[0]

    print(f"""
    Records before accuracy filtering: {bf}
    Records after accuracy filtering: {af}
    Difference: {bf-af}
    Percentage of deleted record: {round(((bf-af)/bf)*100)}%
    """)

    # Filtering based on the speed

    print('Filtering based on the speed in between two consecutive GPS points...')

    traj_df=filtering.filter(traj_df,max_speed_kmh=200)

    print(f"""
    Records before speed filtering: {af}
    Records after speed filtering: {traj_df.shape[0]}
    Difference: {af-traj_df.shape[0]}
    Percentage of deleted record: {round(((af-traj_df.shape[0])/af)*100,2)}%
    """)
    return traj_df





def filter_data_process(traj_df, impr_acc):
    return getFilteredData(traj_df, impr_acc)