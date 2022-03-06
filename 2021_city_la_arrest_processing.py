import geopandas as gpd
import numpy as np
import pandas as pd
import datetime
import os


def incrementList(value):
    max = round(value/50000, 0) + 2
    print("{0} queries will be made to pull the dataset from socrata.".format(max))
    maxlist = list(np.arange(1, max))
    return maxlist

def pullSocrata(value,query):
    data = []
    partlist = incrementList(value)
    chunk = 0
    for part in partlist:
        print("{0} partition. Pulling next {1} chunk.".format(part, chunk))
        df = pd.read_csv(query.format(chunk))
        df.columns = df.columns.str.upper().str.replace("_", " ")
        data.append(df)
        chunk += 50000
    final_df = pd.concat(data, axis=0, sort=False)
    return final_df

def aggregateArrest():
    print("Pulling 2010 to 2019 LA Arrest Data")
    arrest10_19_value = pd.read_csv(
        'https://data.lacity.org/resource/yru6-6re4.csv?$select=count(rpt_id)&$$app_token=idaQ6LYbv34krBCmnbk8afKA2')[
        'count_rpt_id'][0]
    arrest10_19 = pullSocrata(arrest10_19_value,
        'https://data.lacity.org/resource/yru6-6re4.csv?$offset={0}&$limit=50000&$order=rpt_id&$$app_token=idaQ6LYbv34krBCmnbk8afKA2')

    print("Pulling 2020 to Current LA Arrest Data")
    arrest20_21_value = pd.read_csv(
        'https://data.lacity.org/resource/amvf-fr72.csv?$select=count(rpt_id)&$$app_token=idaQ6LYbv34krBCmnbk8afKA2')[
        'count_rpt_id'][0]
    arrest20_21 = pullSocrata(50000,
        'https://data.lacity.org/resource/amvf-fr72.csv?$offset={0}&$limit=50000&$order=rpt_id&$$app_token=idaQ6LYbv34krBCmnbk8afKA2')


    print("Merging together Arrest dataframes.")
    arrest_la = pd.concat([arrest10_19, arrest20_21], axis=0, sort=False)

    datepull = datetime.datetime.today().strftime("%Y%m%d")
    print("Saving Arrest File.")
    arrest_la.to_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\city_of_la\arrest_la_' + datepull +'.csv', index=False)

def prepareArrest():
    datepull = datetime.datetime.today().strftime("%Y%m%d")
    if os.path.isfile(r'C:\Users\miche\Desktop\2021_PourSafe\data\city_of_la\arrest_la_' + datepull + '.csv'):
        print("Loading Arrest Data")
        arrest_la = pd.read_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\city_of_la\arrest_la_' + datepull + '.csv')
    else:
        aggregateArrest()
        print("Loading Arrest Data")
        arrest_la = pd.read_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\city_of_la\arrest_la_' + datepull + '.csv')


    print("Converting Data to Spatial Format")
    arrest_geo = gpd.GeoDataFrame(arrest_la, geometry=gpd.points_from_xy(arrest_la.LON, arrest_la.LAT), crs='EPSG:4269')

    print("Reading in additional Shapefiles")
    ca_tract = gpd.read_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles\ca_tract_2020.shp')
    ca_council = gpd.read_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles\la_council.shp')
    ca_neighbor_council = gpd.read_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles\la_neighbor_council.shp')

    print("Executing Spatial Joins")
    arrest_la = gpd.sjoin(arrest_geo, ca_tract[['GEOID', 'NAME', 'NAMELSAD', 'geometry']], op='intersects').drop(['index_right'], axis=1)
    arrest_la = gpd.sjoin(arrest_la, ca_council[['district', 'name', 'geometry']], op='intersects').drop(['index_right'], axis=1)
    arrest_la = arrest_la.rename(columns={"district":"DISTNO", "name": "MEMBER"})
    arrest_la = gpd.sjoin(arrest_la, ca_neighbor_council[['name', 'service_re', 'waddress', 'geometry']], op='intersects').drop(['index_right'], axis=1)

    print("Saving Arrest data with Geo information.")
    arrest_la.to_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\city_of_la\arrest_la_merged.csv', index=False)

if __name__ == "__main__": 
    prepareArrest()