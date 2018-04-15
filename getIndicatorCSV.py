# pylint: skip-file
# use http://www.racketracer.com/2016/07/06/pandas-in-parallel/
"""
Take a original data file and extract info if the dropoff was suburban
and if the pickup was o hare. Also extract the week. Output to file.
"""
import datetime as dt
import time
from multiprocessing import Pool

import numpy as np
import pandas as pd
import shapely.wkt
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

DATATYPES = {
    "Taxi ID": object, "Trip Start Timestamp": object,
    "Dropoff Centroid  Location": object, "Pickup O'Hare Community Area": float
}


def parallelize_dataframe(df, func):
    num_partitions = 100
    num_cores = 4
    df_split = np.array_split(df, num_partitions)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def addWeeks(df):
    df["week"] = df["Trip Start Timestamp"].map(getwknum)
    return df


def getwknum(string):
    month, day, year = map(int, [string[:2], string[3:5], string[6:10]])
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday // 7

def getDowntownBoundary():
    x1 = (-87.668046, 41.925163)
    x2 = (-87.632791, 41.925714)
    x3 = (-87.613650, 41.892051)
    x4 = (-87.611171, 41.852618)
    x5 = (-87.660700, 41.851470)
    return Polygon([x1, x2, x3, x4, x5])

def getSuburbBoundary():
    x1 = (-88.040402, 42.072566)
    x2 = (-87.697086, 42.078652)
    x3 = (-87.532285, 41.746548)
    x4 = (-87.528165, 41.603970)
    x5 = (-87.686093, 41.623477)
    x6 = (-87.804883, 41.729640)
    return Polygon([x1, x2, x3, x4, x5, x6])


def getInPolygonIndicators(wktdata, polygon):
    t0 = time.time()
    points = wktdata.map(shapely.wkt.loads)
    return points.map(lambda x: not polygon.contains(x)) * 1


def getInSuburbIndicators(df):
    boundary = getSuburbBoundary()
    df["iDropoffSuburb"] = getInPolygonIndicators(
        df["Dropoff Centroid  Location"], boundary)
    return df

def addInDowntownIndicators(df):
    boundary = getDowntownBoundary()
    df["iDropoffDowntown"] = getInPolygonIndicators(
        df["Dropoff Centroid  Location"], boundary)
    return df

def readWrite(year):
    filename = f"original/Chicago_taxi_trips{year}.csv"

    t0 = time.time()
    df = pd.read_csv(filename, usecols=DATATYPES.keys(),
                     dtype=DATATYPES, nrows=100000).dropna(axis=0, how="any")
    print(f"{filename} read in {round(time.time()-t0)} sec.")

    df = parallelize_dataframe(df, addWeeks)
    print(f"Weeks added in {round(time.time()-t0)} sec.")

    df = parallelize_dataframe(df, getInSuburbIndicators)
    print(f"Indicators in {round(time.time()-t0)} sec.")

    df = parallelize_dataframe(df, addInDowntownIndicators)
    print(f"Indicators in {round(time.time()-t0)} sec.")

    df.to_csv(f"{year}_iRegions.csv", index=False)
    print(f"csv written in {round(time.time()-t0)} sec.")


if __name__ == "__main__":
    readWrite(2016)
    # test()
