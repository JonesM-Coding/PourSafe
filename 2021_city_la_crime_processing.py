import geopandas as gpd
import numpy as np
import pandas as pd
import datetime
import os
from bs4 import BeautifulSoup
import requests
import pdb

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

def aggregateCrime():
    print("Pulling 2010 to 2019 LA Crime Data")
    # pdb.set_trace()
    crime10_19_value = pd.read_csv('https://data.lacity.org/resource/63jg-8b9z.csv?$select=count(dr_no)&$$app_token=idaQ6LYbv34krBCmnbk8afKA2')['count_dr_no'][0]
    crime10_19 = pullSocrata(crime10_19_value,
        'https://data.lacity.org/resource/63jg-8b9z.csv?$offset={0}&$limit=50000&$order=dr_no&$$app_token=idaQ6LYbv34krBCmnbk8afKA2'
    )
    print("Pulling 2020 to Current LA Crime Data")
    crime20_21_value = pd.read_csv('https://data.lacity.org/resource/2nrs-mtv8.csv?$select=count(dr_no)&$$app_token=idaQ6LYbv34krBCmnbk8afKA2')['count_dr_no'][0 ]
    crime20_21 = pullSocrata(crime20_21_value,
        'https://data.lacity.org/resource/2nrs-mtv8.csv?$offset={0}&$limit=50000&$order=dr_no&$$app_token=idaQ6LYbv34krBCmnbk8afKA2')


    print("Merging together Crime dataframes.")
    crime_la = pd.concat([crime10_19, crime20_21], axis=0, sort=False)

    print("Formatting Crime Fields.")
    crime_la['HOUR'] = crime_la['TIME OCC'].astype(str).str.zfill(4).str[:2]
    crime_la['MINUTE'] = crime_la['TIME OCC'].astype(str).str.zfill(4).str[2:]
    crime_la['CRM CD DESC'] = np.where(crime_la['CRM CD DESC'].isnull(), "Not Reported", crime_la['CRM CD DESC'])

    print("Saving Crime File.")
    datepull = datetime.datetime.today().strftime("%Y%m%d")
    crime_la.to_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\city_of_la\crime_la_' + datepull + '.csv', index=False)


def prepareCrime():
    datepull = datetime.datetime.today().strftime("%Y%m%d")
    if os.path.isfile(r'C:\Users\miche\Desktop\2021_PourSafe\data\city_of_la\crime_la_' + datepull + '.csv'):
        print("Loading Crime Data")
        crime_la = pd.read_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\city_of_la\crime_la_' + datepull + '.csv')
    else:
        aggregateCrime()
        print("Loading Crime Data")
        crime_la = pd.read_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\city_of_la\crime_la_' + datepull + '.csv')


    print("Converting Data to Spatial Format")
    crime_la_geo = gpd.GeoDataFrame(crime_la, geometry=gpd.points_from_xy(crime_la.LON, crime_la.LAT), crs='EPSG:4269')

    print("Reading in additional Shapefiles")
    ca_tract = gpd.read_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles\ca_tract_2020.shp')
    ca_council = gpd.read_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles\la_council.shp')
    ca_neighbor_council = gpd.read_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles\la_neighbor_council.shp')

    print("Executing Spatial Joins")
    crime_la = gpd.sjoin(crime_la_geo, ca_tract[['GEOID', 'NAME', 'NAMELSAD', 'geometry']], op='intersects').drop(['index_right'], axis=1)
    crime_la = gpd.sjoin(crime_la, ca_council[['district', 'name', 'geometry']], op='intersects').drop(['index_right'], axis=1)
    crime_la = crime_la.rename(columns={"district":"DISTNO", "name":"MEMBER"})
    crime_la = gpd.sjoin(crime_la, ca_neighbor_council[['name', 'service_re', 'waddress', 'geometry']], op='intersects').drop(['index_right'], axis=1)

    print("Saving Crime data with Geo information.")
    crime_la.to_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\city_of_la\crime_la_merged.csv', index=False)


if __name__ == "__main__":
    crime10_19_value = pd.read_csv('https://data.lacity.org/resource/63jg-8b9z.csv?$select=count(dr_no)&$$app_token=idaQ6LYbv34krBCmnbk8afKA2')['count_dr_no'][0]
    prepareCrime()