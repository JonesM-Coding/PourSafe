import pandas as pd
import numpy as np
import geopandas as gpd
from selenium.webdriver import FirefoxOptions
import datetime
import psycopg2
import sqlalchemy
import pdb
import requests
import json
# from geocodio import GeocodioClient
import time

def convertSHP(shapefile, name):
    shapefile = gpd.read_file(shapefile)
    shapefile = shapefile.to_crs('EPSG:4269')
    shapefile.to_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/'+ name + ".shp")

def makeLine(name):
    shapefile = gpd.read_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + name + ".shp")
    shapefile_line = gpd.GeoDataFrame(shapefile.boundary, columns=["geometry"])
    shapefile_line = shapefile.join(shapefile_line, lsuffix="_poly", rsuffix="_line")
    print(shapefile_line.columns)
    shapefile_line['geometry'] = shapefile_line['geometry_line']
    shapefile_line.drop(columns = ["geometry_poly", "geometry_line"], inplace=True)
    print(shapefile_line.columns)
    shapefile_line.to_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + name + "_line.shp")

def saveShapefile(df, name):
    shapefile = gpd.read_file(df)
    conn_string = 'postgresql://doadmin:yox1c9wy2onu8wtm@db-postgresql-sfo3-poursafe-do-user-9378326-0.b.db.ondigitalocean.com:25060/poursafe_data?sslmode=require'

    engine = sqlalchemy.create_engine(conn_string)
    print("Pushing {0} to poursafe database".format(name))
    shapefile.to_postgis(name, con=engine, if_exists="replace")


def getGeocodes(df, list, name):
    conn_string = 'postgresql://doadmin:yox1c9wy2onu8wtm@db-postgresql-sfo3-poursafe-do-user-9378326-0.b.db.ondigitalocean.com:25060/poursafe_data?sslmode=require'
    engine = sqlalchemy.create_engine(conn_string)
    query = "SELECT * from {0};".format(df)
    data = pd.read_sql(query, con=engine)
    data_setup = data[list]
    data_setup.columns = ['UID', 'street', 'city', 'state', 'zip']
    data_setup = data_setup[data_setup['street'] != " "]
    print("{0} total records. {1}  with identifiable street information.".format(str(len(data)), str(len(data_setup))))
    data_trunc = data_setup.groupby(["street", "city", "state", "zip"], as_index=False).agg({"UID": lambda x: str(x).to_list()})
    print("{0}  unique addresses in dataset. Will send these records to geocodio.".format(data_trunc))
    data_trunc['addresses'] = data_trunc['street'] + "," + data_trunc['city'] + "," + data_trunc['state'] + "," + data_trunc['zip']
    data_trunc["Coordinates"] = None
    data_trunc["Accuracy"] = None

    client = GeocodioClient("8124c8822111683321320ee18c06122180ce136")
    for index, row in data_trunc.iterrows():
        try:
            print(row["addresses"])
            geocode = client.geocode(row["addresses"])
            data_trunc.at[index, "Coordinates"] = geocode.coords
            data_trunc.at[index, "Accuracy"] = geocode.accuracy
        except:
            break

    data_trunc.to_csv(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + name + ".csv", index=False)

def getGeocodes_small(df, col, name):
    conn_string = 'postgresql://doadmin:yox1c9wy2onu8wtm@db-postgresql-sfo3-poursafe-do-user-9378326-0.b.db.ondigitalocean.com:25060/poursafe_df?sslmode=require'
    engine = sqlalchemy.create_engine(conn_string)
    df_setup = df_setup[df_setup[col] != " "]
    print("{0} total records. {1}  with identifiable street information.".format(str(len(df)), str(len(df_setup))))
    print("{0}  unique addresses in dfset. Will send these records to geocodio.".format(df_trunc))
    df_setup["Coordinates"] = None
    df_setup["Accuracy"] = None

    client = GeocodioClient("8124c8822111683321320ee18c06122180ce136")
    for index, row in df_setup.iterrows():
        try:
            print(row[col])
            geocode = client.geocode(row["addresses"])
            df_setup.at[index, "Coordinates"] = geocode.coords
            df_setup.at[index, "Accuracy"] = geocode.accuracy
        except:
            break

    df_setup.to_csv(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + name + ".csv", index=False)


if __name__ == "__main__":
    convertSHP(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles\geo_export_07d64309-6848-403a-9304-db973201e080.shp', 'la_council')
    convertSHP(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles\ElectionDistrict.shp', 'sr_council')
    makeLine(r'ca_bg_2020')
    makeLine(r'ca_tract_2020')
    makeLine(r'la_council')
    makeLine(r'la_neighbor_council')
    makeLine(r'lapd_districts')
    makeLine(r'sr_council')
    saveShapefile(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + "lapd_districts" + "_line.shp", "lapd_districts" + "_line")
    saveShapefile(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + "la_neighbor_council" + "_line.shp", "la_neighbor_council" + "_line")
    saveShapefile(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + "la_council" + "_line.shp", "la_council" + "_line")
    saveShapefile(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + "ca_bg_2020" + "_line.shp", "ca_bg_2020" + "_line")
    saveShapefile(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + "ca_tract_2020" + "_line.shp", "ca_tract_2020" + "_line")
    saveShapefile(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + "sr_council" + "_line.shp", "sr_council" + "_line")
    saveShapefile(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + "lapd_districts.shp" , "lapd_districts")
    saveShapefile(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + "la_neighbor_council.shp" , "la_neighbor_council")
    saveShapefile(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + "la_council.shp", "la_council")
    saveShapefile(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + "ca_bg_2020.shp", "ca_bg_2020")
    saveShapefile(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + "ca_tract_2020.shp", "ca_tract_2020")
    saveShapefile(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + "sr_council.shp", "sr_council")

    # getGeocodes("abc_daily_export", ['File Number','Prem Addr 1', 'Prem City', ' Prem State', 'Prem Zip'], "abc_address_geocodes")