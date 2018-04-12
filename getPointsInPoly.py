# pylint: skip-file

import pandas as pd
import time
import datetime as dt
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import shapely.wkt

DATATYPES = {
    "Taxi ID": object, "week": int, "Pickup Centroid Location": object
}


def getDowntownBoundary():
    x1 = (-87.668046, 41.925163)
    x2 = (-87.632791, 41.925714)
    x3 = (-87.613650, 41.892051)
    x4 = (-87.611171, 41.852618)
    x5 = (-87.660700, 41.851470)
    return Polygon([x1, x2, x3, x4, x5])


def getInPolygonIndicators(wktdata, polygon):
    """
    wktdata: ["POINT (-87.64 41.88)", ..., "POINT (1 1)"]
    returns: [1, ..., 0]
    """
    points = wktdata.map(shapely.wkt.loads)
    return points.map(polygon.contains) * 1


def getIndicatorProportions(groups):
    return (groups.sum() / groups.count()).unstack(level=-1)


def test():
    df = pd.DataFrame({'Taxi ID': pd.Series(["A", "A", "B", "B", "C", "A"]),
                       'week': pd.Series([1, 2, 1, 2, 2, 1]),
                       'Pickup Centroid Location': pd.Series(["POINT (-87.64 41.88)", "POINT (1 1)", "POINT (1 1)", "POINT (1 1)", "POINT (-87.64 41.88)", "POINT (1 1)"])})
    downtown = getDowntownBoundary()
    df["iPickupDowntown"] = getInPolygonIndicators(
        df["Pickup Centroid Location"], downtown)
    groups = df.groupby(["Taxi ID", "week"])["iPickupDowntown"]
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
    filename = f"original/Chicago_taxi_trips{year}_weeks.csv"

    t0 = time.time()
    df = pd.read_csv(filename, usecols=["Taxi ID", "week", "Pickup Centroid Location"], dtype=DATATYPES, nrows=100000).dropna(axis=0, how='any')
    print(df.head())
    print(f"{filename} read in {round(time.time()-t0)} sec.")

    t0 = time.time()
    downtown = getDowntownBoundary()
    df["iPickupDowntown"] = getInPolygonIndicators(df["Pickup Centroid Location"], downtown)
    groups = df.groupby(["Taxi ID", "week"])["iPickupDowntown"]
    proportions = getIndicatorProportions(groups)
    print(proportions.head())
    medians = proportions.median()
    print(medians.head())
    print(f"Indicator medians calculated in {round(time.time()-t0)} sec.")

    t0 = time.time()
    medians.to_csv(f"{year}_iPickupDowntown.csv", index=False)
    print(f"{year}_iPickupDowntown.csv written in {round(time.time()-t0)} sec.")


if __name__ == "__main__":
    readWrite(2014)
    # test()
