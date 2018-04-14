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
    "Taxi ID": object, "week": object, "iDropoffSuburb": float, "Pickup O'Hare Community Area": float
}


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


def getNAtoSIndicators(df):
    df["NotAirToSub"] = (1 - df["iDropoffSuburb"]) * df["Pickup O'Hare Community Area"]
    return df


def getAtoSIndicators(df):
    df["AirToSub"] = df["iDropoffSuburb"] * df["Pickup O'Hare Community Area"]
    return df


def readWrite(year):
    filename = f"{year}_iRegions.csv"

    t0 = time.time()
    df = pd.read_csv(filename,
                     usecols=DATATYPES.keys(),
                     dtype=DATATYPES).dropna(axis=0, how="any")
    print(f"{filename} read in {round(time.time()-t0)} sec.")

    df = parallelize_dataframe(df, getNAtoSIndicators)
    print(f"getNAtoSIndicators in {round(time.time()-t0)} sec.")

    df = parallelize_dataframe(df, getAtoSIndicators)
    print(f"getAtoSIndicators in {round(time.time()-t0)} sec.")

    groups = df.groupby(["Taxi ID", "week"])
    print(f"Group by in {round(time.time()-t0)} sec.")

    proportions = (groups["NotAirToSub"].sum() / groups["NotAirToSub"].count()).unstack(level=-1)
    proportions.to_csv(f"{year}_iNotAirToSub.csv", index=False)
    medians = proportions.median()
    medians.to_csv(f"{year}_iNotAirToSub_median.csv", index=False)
    print(f"csv written in {round(time.time()-t0)} sec.")

    proportions = (groups["AirToSub"].sum() / groups["AirToSub"].count()).unstack(level=-1)
    proportions.to_csv(f"{year}_iAirToSub.csv", index=False)
    medians = proportions.median()
    medians.to_csv(f"{year}_iAirToSub_median.csv", index=False)
    print(f"csv written in {round(time.time()-t0)} sec.")

if __name__ == "__main__":
    readWrite(2017)
    # test()
