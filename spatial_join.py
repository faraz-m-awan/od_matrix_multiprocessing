import pandas as pd
import geopandas as gpd
from shapely.geometry import  Point


def spatialJoin(df,shape,long_col,lat_col,loc_type):

    #geometry=[Point(xy) for xy in zip(df['org_lng'],df['org_lat'])]
    geometry=[Point(xy) for xy in zip(df[long_col],df[lat_col])]

    geo_df=gpd.GeoDataFrame(df,geometry=geometry,crs='EPSG:4326')

    geo_df.sindex
    #geo_df=gpd.sjoin(geo_df,shape[['Name','geometry']],how='left',predicate='intersects')
    geo_df=gpd.sjoin(geo_df,shape[['oa_id','dz_id','iz_id','council_area_id','council_area_name','geometry']],how='left',predicate='intersects')
    col_raname_dict={
        'oa_id':f'{loc_type.split("_")[0]}_oa_id',
        'dz_id':f'{loc_type.split("_")[0]}_dz_id',
        'iz_id':f'{loc_type.split("_")[0]}_iz_id',
        'council_area_id':f'{loc_type.split("_")[0]}_council_area_id',
        'council_area_name':f'{loc_type.split("_")[0]}_council_area_name',
    }
    geo_df.rename(columns=col_raname_dict,inplace=True)
    geo_df.drop(columns=['index_right','geometry'],inplace=True)
    geo_df.reset_index(drop=True,inplace=True)
    geo_df=pd.DataFrame(geo_df)
    

    return geo_df

def performSpatialjoin(df,shape,long_col,lat_col,loc_type):
    return spatialJoin(df,shape,long_col,lat_col,loc_type)