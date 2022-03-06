import pandas as pd
import numpy as np
import requests
import zipfile
from selenium import webdriver
import io
from sqlalchemy import create_engine
import os
import datetime
from selenium.webdriver import ChromeOptions
import geopandas as gpd
import pdb
from ast import literal_eval
import time
import glob
import pantab
def getLatestDate(df):
    folder_path = df.rsplit("/", 1)[0]
    print(folder_path)
    files = glob.glob(folder_path + "/*.csv")
    times = [datetime.datetime.fromtimestamp(os.path.getctime(x)) for x in files]
    max_file = max(times)
    datepull = max_file.strftime("%Y%m%d")
    print(datepull)
    return datepull

def checkDriver(driver, url):
    opts = ChromeOptions()
    opts.add_argument("--headless")
    driver = webdriver.Chrome(executable_path = driver, chrome_options=opts)
    try:
        driver.get(url)
        title = driver.title
        content = driver.page_source

        print(driver.title)

    finally:

        driver.quit()

def initialGeocodes(df, name):
    prev_df = pd.read_csv(r"C:\Users\miche\Desktop\2021_PourSafe\data\past_data/2021-04-22_all_geocoded_updated.csv")
    prev_df["Dup Counts"] = prev_df["Dup Counts"].fillna(0)
    prev_df['Type Orig Iss Date'] = prev_df['Type Orig Iss Date'].replace(r'^\s*$', np.nan, regex=True)
    prev_df["UID"] = prev_df["File Number"].astype(str) + "_" + prev_df["Lic or App"] + "_" + prev_df["License Type"].astype(str) + "_" + prev_df["Type Status"].astype(str) + "_" + prev_df["Dup Counts"].astype(str) + "_" + prev_df['Type Orig Iss Date'].astype(str)
    prev_df = prev_df[["UID", "File Number","Prem Addr 1", " Prem Addr 2", "Prem City", " Prem State", "Prem Zip", "Latitude", "Longitude", "geo_score", "geo_data_source","geo_provider", "prem_st_1_clean"]]
    summ_df = prev_df
    summ_df["UID_count"] = summ_df["UID"]
    summ = summ_df.groupby("UID", as_index=False)["UID_count"].nunique()
    engine = create_engine('postgresql://doadmin:yox1c9wy2onu8wtm@db-postgresql-sfo3-poursafe-do-user-9378326-0.b.db.ondigitalocean.com:25060/poursafe_data?sslmode=require', echo=True)
    prev_df.to_sql(name, con=engine, if_exists='replace')
    prev_df.to_csv(r"C:\Users\miche\Desktop\2021_PourSafe\data\past_data/abc_geocodes.csv", index=False)


def getDaily():
    abc_folder = requests.get(r'https://www.abc.ca.gov/wp-content/uploads/WeeklyExport_CSV.zip')
    abc_zip = zipfile.ZipFile(io.BytesIO(abc_folder.content))
    abc_zip.extractall(r'C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc')
    if os.path.isfile(r"C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc\ABC_WeeklyDataExport.csv") == True:
        print("The ABC Daily Report has been downloaded successfully.")
    else:
        print("The file has not been downloaded successfully.")
    datepull = datetime.datetime.today().strftime("%Y%m%d")
    df = pd.read_csv(r"C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc\ABC_WeeklyDataExport.csv", skiprows=1)
    # df = df.drop_duplicates(keep='first')
    prev_df = pd.read_csv(r"C:\Users\miche\Desktop\2021_PourSafe\data\past_data/2021-04-22_all_geocoded_updated.csv")
    prev_df.drop(
        ["geography", "geom", "abc_main_id", "class", "action", "conditions", "escrow", "trans_orig_type",
         "trans_orig_lic", "trans_new_type", "trans_new_lic", "abc_rpt_src_type", "abc_rpt_src_url", "abc_rpt_date",
         "todays_date", "sys_period_start", "class_label", "class_parent", "prem_city_ps", "prem_council",
         "prem_neighborhood", "geoid", "prem_neighborhood_council", "geo_score", "geo_data_source", "geo_provider",
         "prem_st_1_clean"], axis=1, inplace=True)
    # prev_df.drop_duplicates(keep='first')
    df["Dup Counts"] = df["Dup Counts"].replace(r'^\s*$', np.nan, regex=True)
    df["Dup Counts"] = df["Dup Counts"].str.replace('  ', '').str.replace(' ', '')
    df["Dup Counts"] = df["Dup Counts"].fillna(0)
    df["Dup Counts"] = df["Dup Counts"].astype(float).astype(int)
    df['Type Orig Iss Date'] = df['Type Orig Iss Date'].replace(r'^\s*$', np.nan, regex=True)
    df['Type Orig Iss Date'] = pd.to_datetime(df['Type Orig Iss Date'])
    df['Type Orig Iss Date'] = df['Type Orig Iss Date'].dt.strftime('%m/%d/%Y')

    prev_df["Dup Counts"] = prev_df["Dup Counts"].replace(r'^\s*$', np.nan, regex=True)
    prev_df["Dup Counts"] = prev_df["Dup Counts"].fillna(0)
    prev_df["Dup Counts"] = prev_df["Dup Counts"].astype(float).astype(int)
    prev_df['Type Orig Iss Date'] = prev_df['Type Orig Iss Date'].replace(r'^\s*$', np.nan, regex=True)
    prev_df['Type Orig Iss Date'] = pd.to_datetime(prev_df['Type Orig Iss Date'])
    prev_df['Type Orig Iss Date'] = prev_df['Type Orig Iss Date'].dt.strftime('%m/%d/%Y')


    df["UID"] = df["File Number"].astype(str) + "_" + df["Lic or App"] + "_" + df["License Type"].astype(str) + "_" + df["Type Status"].astype(str) + "_" + df["Dup Counts"].astype(str) + "_" + df['Type Orig Iss Date'].astype(str)
    df.to_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\past_data/abc_duplicate_check.csv', index=False)
    prev_df["UID"] = prev_df["File Number"].astype(str) + "_" + prev_df["Lic or App"] + "_" + prev_df["License Type"].astype(str) + "_" + prev_df["Type Status"].astype(str) + "_" + prev_df["Dup Counts"].astype(str) + "_" + prev_df['Type Orig Iss Date'].astype(str)
    prev_df.to_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\past_data/abc_duplicate_check_previous.csv', index=False)
    # df = df.merge(prev_df[["UID", "Latitude", "Longitude"]], how='left', on="UID")
    # prev_df_add = prev_df[~prev_df["UID"].isin(df["UID"])]
    df_add = df[~df['UID'].isin(prev_df["UID"])]
    print("The original dataset has {0} records".format(str(len(prev_df))))
    print("There are {0} records being added to the previous dataset.".format(str(len(df_add))))
    cols = np.intersect1d(prev_df.columns, df.columns)
    cols = cols.tolist()
    # df = pd.concat([df, prev_df_add[cols]], axis=0,sort=False).reset_index()
    prev_df = pd.concat([prev_df, df_add[cols]], axis=0,sort=False).reset_index()
    print("The new dataset has {0} total records".format(str(len(prev_df))))
    prev_df = prev_df.replace(r'^\s*$', np.nan, regex=True)
    print("Formatting file to remove additional lines and rename with current date: {0}.".format(datepull))
    prev_df.to_csv(r"C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc\ABC_WeeklyDataExport_" + datepull + ".csv", index=False)
    prev_df.drop_duplicates(subset="UID", keep='first', inplace=True)
    print("Pulling records that require geocoding.")
    prev_df_geo = prev_df[prev_df["Latitude"].isnull()]
    print("{0} records need to be geocoded. Saving data".format(len(prev_df_geo)))
    prev_df_geo[["UID", "File Number","Prem Addr 1", " Prem Addr 2", "Prem City", " Prem State", "Prem Zip"]].to_csv(r"C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc\ABC_WeeklyDataExport_" + datepull + "_togeocode.csv", index=False)

# def getGeocodes(list, name):
#     datepull = datetime.datetime.today().strftime("%Y%m%d")
#     data = pd.read_csv(r"C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc\ABC_WeeklyDataExport_" + datepull + "_togeocode.csv")
#     data_setup = data[list]
#     data_setup = data_setup[data_setup['Prem Addr 1'] != " "]
#     data_setup['addresses'] = data_setup['Prem Addr 1'] + "," + data_setup['Prem City'] + "," + data_setup[' Prem State'] + "," + data_setup['Prem Zip']
#     data_setup["Coordinates"] = None
#     data_setup["geo_score"] = None
#     data_setup["prem_st_1_clean"] = None
#     print("{0} total records. {1}  with identifiable street information.".format(str(len(data)), str(len(data_setup))))
#     client = GeocodioClient("8124c8822111683321320ee18c06122180ce136")
#
#     for index, row in data_setup.iterrows():
#         try:
#             print(row["addresses"])
#             geocode = client.geocode(row["addresses"])
#             data_setup.at[index, "Coordinates"] = geocode.coords
#             data_setup.at[index, "geo_score"] = geocode.accuracy
#             data_setup.at[index, "prem_st_1_clean"] = geocode.formatted_address
#             time.sleep(.25)
#         except:
#             break
#
#     data_setup.to_csv(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + name + ".csv", index=False)

def processGeocode(name):
    new = pd.read_csv(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + name + ".csv")
    new = new[(new["Coordinates"].notnull()) & (new["geo_score"] >= .7)]
    new["Latitude"] = new["Coordinates"].str.split(",").str[0].str.replace("(", "")
    new["Longitude"] = new["Coordinates"].str.split(",").str[-1].str.replace(")", "")
    engine = create_engine('postgresql://doadmin:yox1c9wy2onu8wtm@db-postgresql-sfo3-poursafe-do-user-9378326-0.b.db.ondigitalocean.com:25060/poursafe_data?sslmode=require', echo=True)
    geocodes = pd.read_sql("SELECT * from abc_geocodes", con=engine)
    new_geocodes =  pd.concat([new, geocodes], axis=0, sort=False)
    new_geocodes.to_sql(name, con=engine, if_exists='replace')


def mergeGeocode():
    engine = create_engine('postgresql://doadmin:yox1c9wy2onu8wtm@db-postgresql-sfo3-poursafe-do-user-9378326-0.b.db.ondigitalocean.com:25060/poursafe_data?sslmode=require', echo=True)

    datepull = datetime.datetime.today().strftime("%Y%m%d")
    abc = pd.read_csv(r"C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc\ABC_WeeklyDataExport_" + datepull + ".csv")
    geocodes = pd.read_sql("SELECT * from abc_geocodes", con=engine)
    abc_merge = abc.merge(geocodes[["UID", "Prem City", " Prem State", "Latitude", "Longitude"]], how="left", on="UID", suffixes=("_data", "_geocode"))
    abc_merge["Latitude"] = np.where(abc_merge["Latitude_data"].isnull(), abc_merge["Latitude_geocode"], abc_merge["Latitude_data"])
    abc_merge["Longitude"] = np.where(abc_merge["Longitude_data"].isnull(), abc_merge["Longitude_geocode"], abc_merge["Longitude_data"])
    #abc_merge = abc.merge(geocodes, how="left", left_on=["File Number"], right_on=["UID"])

    abc_merge["Prem City"] = np.where(abc_merge["Prem City_data"].isnull(), abc_merge["Prem City_geocode"], abc_merge["Prem City_data"])
    abc_merge[" Prem State"] = np.where(abc_merge[" Prem State_data"].isnull(), abc_merge[" Prem State_geocode"], abc_merge[" Prem State_data"])
    abc_merge.drop(columns=["Latitude_data", "Longitude_data", "Latitude_geocode", "Longitude_geocode", "Prem City_data", " Prem State_data", "Prem City_geocode", " Prem State_geocode"], inplace=True)
    abc_lisc = abc_merge[abc_merge["Lic or App"] == "LIC"]
    abc_app = abc_merge[abc_merge["Lic or App"] == "APP"]
    abc_lisc.drop_duplicates(subset="UID", keep='first', inplace=True)
    abc_merge = pd.concat([abc_lisc, abc_app], axis=0, sort=False)
    abc_merge.to_csv(r"C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc\ABC_WeeklyDataExport_" + datepull + "_geocoded.csv", index=False)

def prepareLicense():
    datepull = datetime.datetime.today().strftime("%Y%m%d")
    abc = pd.read_csv(r"C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc\ABC_WeeklyDataExport_" + datepull + "_geocoded.csv")
    abc_coord = abc[abc["Latitude"].notnull()]
    abc_blank = abc[abc["Latitude"].isnull()]

    print("Converting Data to Spatial Format")
    abc_geo = gpd.GeoDataFrame(abc_coord, geometry=gpd.points_from_xy(abc_coord.Longitude, abc_coord.Latitude), crs='EPSG:4269')

    print("Reading in additional files")
    ca_tract = gpd.read_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/ca_tract_2020.shp')
    ca_council = gpd.read_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/la_council.shp')
    ca_neighbor_council = gpd.read_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/la_neighbor_council.shp')
    sr_council = gpd.read_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/sr_council.shp')
    print("Executing Spatial Joins")
    sr_council = sr_council.rename(columns={"Name": "San Rafael Council District"})
    abc = gpd.sjoin(abc_geo, ca_tract[['GEOID', 'NAME', 'NAMELSAD', 'geometry']], op='intersects', how='left').drop(['index_right'], axis=1)
    abc = gpd.sjoin(abc, ca_council[['district', 'name', 'geometry']], op='intersects', how='left').drop(['index_right'], axis=1)
    abc = abc.rename(columns={"district": "DISTNO", "name":"MEMBER"})
    abc = gpd.sjoin(abc, ca_neighbor_council[['name', 'service_re', 'waddress', 'geometry']], op='intersects', how='left').drop(['index_right'], axis=1)
    abc = gpd.sjoin(abc, sr_council[["San Rafael Council District", 'geometry']], op='intersects', how='left').drop(['index_right'], axis=1)

    abc = pd.concat([abc, abc_blank], axis=0, sort=False)
    print("Saving ABC Weekly Data with Geo information.")

    print("Removing Duplicates.")
    # abc = abc.drop_duplicates(subset="UID", keep='first')
    abc.to_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc\abc_weekly_' + datepull + "_final.csv" , index=False)

def save_hyper():
    datepull = datetime.datetime.today().strftime("%Y%m%d")
    df = pd.read_csv(r"C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc\abc_weekly_" + datepull + '_final.csv')
    pantab.frame_to_hyper(df, r"C:\Users\miche\Desktop\2021_PourSafe\data\abc_licensing_test.hyper", table = 'license')

if __name__ == "__main__":
    checkDriver(r"C:\Users\miche\Desktop\2021_PourSafe\chromedriver.exe","https://www.abc.ca.gov/licensing/license-lookup/license-number/")
    getDaily()
    initialGeocodes("", "abc_geocodes")
    # getGeocodes(['UID','Prem Addr 1', 'Prem City', ' Prem State', 'Prem Zip'], "abc_address_geocodes")
    # processGeocode("abc_address_geocodes")
    mergeGeocode()
    prepareLicense()
    # save_hyper()