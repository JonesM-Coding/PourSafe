import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.support.ui import Select
import re
import time
import bs4
import psycopg2
import geopandas as gpd
from selenium.webdriver import ChromeOptions
import datetime
import pdb
import glob
import os
from sqlalchemy import create_engine
import psycopg2
import sqlalchemy

def getGeocodes_small(df, col, name):
    conn_string = 'postgresql://doadmin:yox1c9wy2onu8wtm@db-postgresql-sfo3-poursafe-do-user-9378326-0.b.db.ondigitalocean.com:25060/poursafe_df?sslmode=require'
    engine = sqlalchemy.create_engine(conn_string)
    df_setup = df[df[col] != " "]
    print("{0} total records. {1}  with identifiable street information.".format(str(len(df)), str(len(df_setup))))
    print("{0}  unique addresses in dfset. Will send these records to geocodio.".format(df_setup))
    df_setup["Coordinates"] = None
    df_setup["Accuracy"] = None

    client = GeocodioClient("8124c8822111683321320ee18c06122180ce136")
    for index, row in df_setup.iterrows():
        try:
            print(row[col])
            geocode = client.geocode(row[col])
            df_setup.at[index, "Coordinates"] = geocode.coords
            df_setup.at[index, "Accuracy"] = geocode.accuracy
            time.sleep(.25)
        except:
            break

    df_setup.to_csv(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + name + ".csv", index=False)
    return df_setup

def getLatestDate(df):
    folder_path = df.rsplit("/", 1)[0]
    print(folder_path)
    files = glob.glob(folder_path + "/*.csv")
    times = [datetime.datetime.fromtimestamp(os.path.getctime(x)) for x in files]
    max_file = max(times)
    datepull = max_file.strftime("%Y%m%d")
    print(datepull)
    return datepull

def getDate():
    datepull = datetime.datetime.today().strftime("%Y%m%d")
    return datepull

def getColon(string):
    matches = re.finditer(":", string)
    match_positions = [match.start() for match in matches]
    return match_positions

def incrementList(value):
    max = round(value/50000, 0) + 2
    print("{0} queries will be made to pull the dataset from socrata.".format(max))
    maxlist = list(np.arange(1, max))
    return maxlist

def modifyString(string):
    """Inserts "None" placeholder for records with no data"""
    insert_str = '\n(none)\n'
    string_sp = string.split(":")
    newString = []
    for piece in string_sp:
        if piece[:2] == '\n\n' and piece[:3] != "\n\n\n":
            piece_new = insert_str + piece
            newString.append(piece_new)
        elif piece == '\n\n\n\n' or piece[:4] == '\n\n\n\n':
            piece_new = insert_str + piece
            newString.append(piece_new)
        else:
            newString.append(piece)
    finalString = "".join(newString)
    return finalString


def list_slice(lis):
    data = []
    min = 0
    stop = 7
    while stop <= len(lis):
        spl_list = lis[min:stop]
        data.append(spl_list)
        min += 7
        stop += 7
    return data


def setDriver(driver, url):
    opts = ChromeOptions()
    opts.add_argument("--headless")
    opts.add_argument("--start-maximized")
    prefs = {"profile.default_content_settings.popups": 0,
             "download.default_directory": r"C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc\\",  # IMPORTANT - ENDING SLASH V IMPORTANT
             "directory_upgrade": True}
    opts.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(executable_path = driver, options=opts)
    try:
        driver.get(url)
        title = driver.title
        content = driver.page_source

        print(driver.title)

    finally:

        return driver

def getPage(driver, url):
    driver.get(url)


def abcScrape(driver,url):
    getPage(driver, url)
    dates = driver.find_elements_by_xpath('//select[@name="week_ending"]')[0]
    dates = dates.text.strip().split("\n")
    select = Select(driver.find_elements_by_xpath('//select[@name="week_ending"]')[0])
    button = driver.find_elements_by_xpath('//button[@id="surr-lic-submit"]')[0]
    return dates, select, button


def getNumbers(driver):
    data = driver.find_element_by_xpath('//div[@class="lqs"]')
    file_numbers = data.find_elements_by_xpath('//h2')
    file_numbers = [f.text for f in file_numbers]
    return file_numbers


def getLicenseText(driver):
    soup = bs4.BeautifulSoup(driver.page_source, 'html.parser')
    global enforcement_data
    enforcement_data = {}
    for header in soup.find_all('h2'):
        nextNode = header
        info = []
        interm_data = []
        while True:
            nextNode = nextNode.nextSibling
            if nextNode is None:
                break
            if isinstance(nextNode, bs4.NavigableString):
                info.append(nextNode)
            if isinstance(nextNode, bs4.Tag):
                if nextNode.name == "h2":
                    break
                info.append(nextNode.get_text())
        enforcement_data.update({header.get_text(): info})
    return enforcement_data


def processPageData(file_numbers):
    file_list = []
    for number in file_numbers:
        print("Data collection process starting for {0}.".format(number))

        print("Formatting License Types Data.")
        lisc_data = enforcement_data[number][3]
        lisc_data = lisc_data.split("\n")
        lisc_data = [i for i in lisc_data if i != '']
        lisc_data = list_slice(lisc_data)
        lisc_df = pd.DataFrame(lisc_data[1:], columns=lisc_data[0])
        print("Formatting File Information Data.")

        file_data = enforcement_data[number][9]
        file_data = modifyString(file_data)
        file_data = file_data.split("\n")
        file_data = [i for i in file_data if i != '']
        file_data = [i.replace(":", "") for i in file_data if i != '']
        file_data = [file_data[0::2], file_data[1::2]]
        file_df = pd.DataFrame(file_data[1:], columns=file_data[0])

        print("Formatting Licensee Details and Proceedings Data.")
        licensee_data = enforcement_data[number][13]
        licensee_data = modifyString(licensee_data)
        licensee_data = licensee_data.split("\n")
        licensee_data = [i for i in licensee_data if i != '']
        licensee_data = [i.replace(":", "") for i in licensee_data if
                         i != '' and i != "Licensee Details" and i != 'Proceedings']
        licensee_data = [licensee_data[0::2], licensee_data[1::2]]
        licensee_df = pd.DataFrame(licensee_data[1:], columns=licensee_data[0])

        enforcement_df = pd.concat([lisc_df, file_df, licensee_df], axis=1, sort=False)
        enforcement_df["License Number"] = number
        enforcement_df = enforcement_df.fillna(method='ffill')
        file_list.append(enforcement_df)
    enforcement_df = pd.concat(file_list, axis=0, sort=False)
    return enforcement_df


def runScrape(driver, url, name):
    driver = setDriver(driver, url)
    dates, select, button = abcScrape(driver,url)
    final_data = []
    for date in dates:
        select.select_by_visible_text(date)
        button.click()
        time.sleep(5)
        file_list = []

        file_numbers = getNumbers(driver)
        enforcement_data = getLicenseText(driver)

        processed_data = processPageData(file_numbers)
        file_list.append(processed_data)

        interim_data = pd.concat(file_list, axis=0, sort=False)
        interim_data["Week Ending"] = date
        final_data.append(interim_data)
    datepull = getDate()
    final_enforcement = pd.concat(final_data, axis=0, sort=False)
    final_enforcement.to_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc/abc_enforcements_' + name + "_" + datepull + '.csv')
    print("{0} Enforcement Data has been pulled from {1} to {2}".format(name,str(date), datepull))

def getRestrictions(driver, license):
    getPage(driver, "https://www.abc.ca.gov/licensing/license-lookup/license-number/")
    searchbox = driver.find_elements_by_xpath('//input[@name="LICENSE"]')[0]
    searchbox.send_keys(license)
    searchbox.submit()
    soup = bs4.BeautifulSoup(driver.page_source, 'html.parser')
    results = soup.find_all('div', {'class':'col-sm-12'})
    restrictions = results[1]
    restriction_result = restrictions.find_all('p')
    restriction_result = [p.text for p in restriction_result]
    return restriction_result


def mergeEnforcement():
    datepull = getLatestDate(r'C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc/abc_enforcements_finalized_')
    final = pd.read_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc/abc_enforcements_finalized_'+ datepull + '.csv')
    filed = pd.read_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc/abc_enforcements_filed_' + datepull + '.csv')
    filed.drop(filed.filter(regex='Unnamed').columns, axis=1, inplace=True)
    final.drop(final.filter(regex='Unnamed').columns, axis=1, inplace=True)
    final["Dataset"] = "Final"
    filed["Dataset"] = "Filed"
    filed["File Number"] = filed["License Number"].str.replace("File Number ", "").astype(int)
    final["File Number"] = final["License Number"].str.replace("File Number ", "").astype(int)

    final["Issued"] = pd.to_datetime(final["Issued"]).dt.strftime("%m/%d/%Y")
    filed["Issued"] = pd.to_datetime(filed["Issued"]).dt.strftime("%m/%d/%Y")

    final["Expires"] = np.where(final["Expires"] == "0", "",final["Expires"])
    filed["Expires"] = np.where(filed["Expires"] == "0", "", filed["Expires"])

    final["Expires"] = pd.to_datetime(final["Expires"]).dt.strftime("%m/%d/%Y")
    filed["Expires"] = pd.to_datetime(filed["Expires"]).dt.strftime("%m/%d/%Y")

    final["Status Date"] = pd.to_datetime(final["Status Date"]).dt.strftime("%m/%d/%Y")
    filed["Status Date"] = pd.to_datetime(filed["Status Date"]).dt.strftime("%m/%d/%Y")

    final["Registration Date"] = np.where(final["Registration Date"] == "(none)", "", final["Registration Date"])
    filed["Registration Date"] = np.where(filed["Registration Date"] == "(none)", "", filed["Registration Date"])
    final["Registration Date"] = pd.to_datetime(final["Registration Date"]).dt.strftime("%m/%d/%Y")
    filed["Registration Date"] = pd.to_datetime(filed["Registration Date"]).dt.strftime("%m/%d/%Y")

    final["Date Cleared"] = np.where(final["Date Cleared"] == "(none)", "", final["Date Cleared"])
    filed["Date Cleared"] = np.where(filed["Date Cleared"] == "(none)", "", filed["Date Cleared"])
    final["Date Cleared"] = pd.to_datetime(final["Date Cleared"]).dt.strftime("%m/%d/%Y")
    filed["Date Cleared"] = pd.to_datetime(filed["Date Cleared"]).dt.strftime("%m/%d/%Y")

    final["Date Received"] = pd.to_datetime(final["Date Received"]).dt.strftime("%m/%d/%Y")
    filed["Date Received"] = pd.to_datetime(filed["Date Received"]).dt.strftime("%m/%d/%Y")

    final["Dups"] = final["Dups"].fillna(0)
    filed["Dups"] = filed["Dups"].fillna(0)
    final = final.rename(columns = {"Status.1":"Proceedings Status", "Status Date.1": "Proceedings Status Date"})

    final["Proceedings Date"] = np.where(np.logical_or(final["Proceedings Date"] == "(none)", final["Proceedings Date"] == "DD-MON-YYYY"), "", final["Proceedings Date"])
    final["Proceedings Date"] = pd.to_datetime(final["Proceedings Date"]).dt.strftime("%m/%d/%Y")

    final["UID"] = final["File Number"].astype(str) + "_" + "LIC" + "_" + final["Type"].astype(
        str) + "_" + final["License Status"].astype(str) + "_" + final["Dups"].astype(str)

    filed["UID"] = filed["File Number"].astype(str) + "_" + "LIC" + "_" + filed["Type"].astype(
        str) + "_" + filed["License Status"].astype(str) + "_" + filed["Dups"].astype(str)

    # final_check = final.merge(filed, how="left", on="UID", suffixes=("_final", "_filed"))
    # filed = filed.add_suffix("_filed")
    filed_remains = filed[~filed["UID"].isin(final["UID"])]
    # filed = filed.rename(columns={"UID_filed": "UID"})
    final_enforce = pd.concat([final, filed_remains], axis=0, sort=False)

    engine = create_engine('postgresql://doadmin:yox1c9wy2onu8wtm@db-postgresql-sfo3-poursafe-do-user-9378326-0.b.db.ondigitalocean.com:25060/poursafe_data?sslmode=require', echo=True)
    # abc_daily = pd.read_sql("SELECT * from abc_daily_export", con=engine)
    #
    # addr = abc_daily.drop_duplicates(subset="File Number", keep="first")
    # addr["File Number"] = addr["File Number"].astype(int)
    # addr.drop_duplicates()
    # final_enforce_geo = final_enforce.merge(addr[["File Number", "Latitude", "Longitude", "Prem City", "Prem County"]],
    #                                         how="left", on="File Number")
    # # final_geocode = getGeocodes_small(final_enforce_geo, "Premises Address", "abc_weekly_geocoded.")
    # final_geocode = pd.read_csv(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/' + "abc_weekly_geocoded." + ".csv")
    # final_geocode = final_geocode.drop_duplicates(subset="File Number", keep="first")
    # final_enforce_geo = final_enforce_geo.merge(final_geocode[["File Number", "Coordinates", "Accuracy"]], how='left',
    #                                             on="File Number")
    # final_enforce_geo["Latitude"] = np.where(final_enforce_geo["Latitude"].isnull(),
    #                                          final_enforce_geo["Coordinates"].str.split(",").str[0].str.replace("(",
    #                                                                                                             ""),
    #                                          final_enforce_geo["Latitude"])
    # final_enforce_geo["Longitude"] = np.where(final_enforce_geo["Longitude"].isnull(),
    #                                          final_enforce_geo["Coordinates"].str.split(",").str[0].str.replace("(",
    #                                                                                                             ""),
    #                                          final_enforce_geo["Longitude"])
    # pdb.set_trace()
    final_enforce.to_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc/abc_enforcements_' + "all" + "_" + datepull + '.csv', index=False)

def referenceLicense():
    datepull = getLatestDate(r'C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc/abc_enforcements_finalized_')
    enforce = pd.read_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc/abc_enforcements_' + "all" + "_" + datepull + '.csv')
    license = pd.read_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc\abc_weekly_' + datepull + "_final.csv")
    license["enforcement_UID"] = license["File Number"].astype(str) + "_" + license["Lic or App"] + "_" + license["License Type"].astype(str)+ "_" + license["Type Status"].astype(str) + "_" + license["Dup Counts"].astype(str)
    enforce['enforcement_UID'] = enforce["File Number"].astype(str) + "_" + "LIC" + "_" + enforce["Type"].astype(
        str) + "_" + enforce["License Status"].astype(str) + "_" + enforce["Dups"].astype(str)
    enforce = enforce.merge(license[["enforcement_UID", "Latitude", "Longitude", "Prem City", " Prem State", "Prem County"]], how='left', on='enforcement_UID')
    enforce = enforce.rename(columns = {"Prem State ": "State", "Prem County":"County", "Prem City":"City"})
    enforce.drop_duplicates(subset=['Registration Number', "License Type"], inplace=True, keep='first')
    enforce_coord = enforce[enforce["Latitude"].notnull()]
    enforce_blank = enforce[enforce["Latitude"].isnull()]
    enforce_geo = gpd.GeoDataFrame(enforce_coord, geometry=gpd.points_from_xy(enforce_coord.Longitude, enforce_coord.Latitude), crs='EPSG:4269')

    print("Reading in additional files")
    ca_tract = gpd.read_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/ca_tract_2020.shp')
    ca_council = gpd.read_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/la_council.shp')
    ca_neighbor_council = gpd.read_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/la_neighbor_council.shp')
    sr_council = gpd.read_file(r'C:\Users\miche\Desktop\2021_PourSafe\shapefiles/sr_council.shp')
    print("Executing Spatial Joins")
    sr_council = sr_council.rename(columns={"Name": "San Rafael Council District"})
    enforce = gpd.sjoin(enforce_geo, ca_tract[['GEOID', 'NAME', 'NAMELSAD', 'geometry']], op='intersects', how='left').drop(['index_right'], axis=1)
    enforce = gpd.sjoin(enforce, ca_council[['district', 'name', 'geometry']], op='intersects', how='left').drop(['index_right'], axis=1)
    enforce = enforce.rename(columns={"district": "DISTNO", "name":"MEMBER"})
    enforce = gpd.sjoin(enforce, ca_neighbor_council[['name', 'service_re', 'waddress', 'geometry']], op='intersects', how='left').drop(['index_right'], axis=1)
    abc = gpd.sjoin(abc, sr_council[["San Rafael Council District", 'geometry']], op='intersects', how='left').drop(['index_right'], axis=1)
    enforce = pd.concat([enforce, enforce_blank], axis=0, sort=False)

    enforce.to_csv(r'C:\Users\miche\Desktop\2021_PourSafe\data\ca_abc\abc_enforcement_coords_' + datepull + ".csv",
                   index=False)

    return


if __name__ == "__main__":
    runScrape(r"C:\Users\miche\Desktop\2021_PourSafe\chromedriver.exe","https://www.abc.ca.gov/licensing/licensing-reports/legal-actions-finalized/", "finalized")
    runScrape(r"C:\Users\miche\Desktop\2021_PourSafe\chromedriver.exe","https://www.abc.ca.gov/licensing/licensing-reports/legal-actions-filed/", "filed")
    mergeEnforcement()
    referenceLicense()