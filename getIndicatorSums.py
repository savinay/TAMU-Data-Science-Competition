# pylint: skip-file
# use http://www.racketracer.com/2016/07/06/pandas-in-parallel/
import datetime as dt
import time
from multiprocessing import Pool

import numpy as np
import pandas as pd
import shapely.wkt
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

DATATYPES = {
    "Taxi ID": object, "Trip Start Timestamp": object, "Pickup Centroid Location": object
}


def addWeeks(df):
    df["week"] = df["Trip Start Timestamp"].map(getwknum)
    return df


def getDowntownBoundary():
    x1 = (-87.668046, 41.925163)
    x2 = (-87.632791, 41.925714)
    x3 = (-87.613650, 41.892051)
    x4 = (-87.611171, 41.852618)
    x5 = (-87.660700, 41.851470)
    return Polygon([x1, x2, x3, x4, x5])


def parallelize_dataframe(df, func):
    num_partitions = 100
    num_cores = 4
    df_split = np.array_split(df, num_partitions)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def getSuburbBoundary():
    x1 = c(-88.040402, 42.072566)
    x2 = c(-87.697086, 42.078652)
    x3 = c(-87.532285, 41.746548)
    x4 = c(-87.528165, 41.603970)
    x5 = c(-87.686093, 41.623477)
    x6 = c(-87.804883, 41.729640)
    return Polygon([x1, x2, x3, x4, x5, x6])


def getInPolygonIndicators(wktdata, polygon):
    """
    wktdata: ["POINT (-87.64 41.88)", ..., "POINT (1 1)"]
    returns: [1, ..., 0]
    """
    t0 = time.time()
    points = wktdata.map(shapely.wkt.loads)
    return points.map(polygon.contains) * 1


def getInDowntownIndicators(df):
    downtown = getDowntownBoundary()
    df["iPickupDowntown"] = getInPolygonIndicators(
        df["Pickup Centroid Location"], downtown)
    return df


def getwknum(string):
    month, day, year = map(int, [string[:2], string[3:5], string[6:10]])
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday // 7


def test():
    df = pd.DataFrame({"Taxi ID": pd.Series(["A", "A", "B", "B", "C", "A"]),
                       "Trip Start Timestamp": pd.Series(
                           ["01/01/2013", "01/01/2013", "01/08/2013", "01/08/2013", "01/16/2013", "01/01/2013"]),
                       "Pickup Centroid Location": pd.Series(
                           ["POINT (-87.64 41.88)", "POINT (1 1)", "POINT (1 1)", "POINT (1 1)", "POINT (-87.64 41.88)", "POINT (1 1)"])})
    downtown = getDowntownBoundary()
    df["iPickupDowntown"] = getInPolygonIndicators(
        df["Pickup Centroid Location"], downtown)
    groups = df.groupby(["Taxi ID", lambda idx: getwknum(
        df, idx, "Trip Start Timestamp")])["iPickupDowntown"]
    print(df)
    downtowns = groups.sum().unstack(level=-1)
    print(downtowns)
    totals = groups.count().unstack(level=-1)
    print(totals)
    proportions = downtowns / totals
    print(proportions)
    medians = proportions.median()
    print(medians)


def readWrite(year):
    filename = f"Chicago_taxi_trips{year}.csv"

    t0 = time.time()
    df = pd.read_csv(filename,
                     usecols=["Taxi ID", "Trip Start Timestamp",
                              "Pickup Centroid Location"],
                     dtype=DATATYPES).dropna(axis=0, how="any")
    print(f"{filename} read in {round(time.time()-t0)} sec.")

    df = parallelize_dataframe(df, getInDowntownIndicators)
    print(f"Indicators in {round(time.time()-t0)} sec.")

    df = parallelize_dataframe(df, addWeeks)
    print(f"Weeks added in {round(time.time()-t0)} sec.")

    groups = df.groupby(["Taxi ID", "week"])["iPickupDowntown"]
    print(f"Group by in {round(time.time()-t0)} sec.")

    proportions = (groups.sum() / groups.count()).unstack(level=-1)
    proportions.to_csv(f"{year}_iPickupDowntown.csv", index=False)
    medians = proportions.median()
    medians.to_csv(f"{year}_iPickupDowntown_median.csv", index=False)
    print(f"{year}_iPickupDowntown.csv written in {round(time.time()-t0)} sec.")


if __name__ == "__main__":
    readWrite(2015)
    # test()
