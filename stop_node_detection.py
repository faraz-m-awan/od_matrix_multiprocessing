
from skmob.preprocessing import filtering, detection, clustering


def getStopNodes(tdf, time_th,radius):

    return detection.stay_locations(tdf,  minutes_for_a_stop=time_th, spatial_radius_km=(radius/1000),leaving_time=True)

def stop_node_process(traj_df, time_th,radius):
    return getStopNodes(traj_df, time_th,radius)