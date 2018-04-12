# pylint: skip-file
import pandas as pd
from collections import Counter
import numpy as np
import datetime as dt
import time

HEADERS = ['Taxi ID', *[f'week{i}' for i in range(1, 54)]]
DATATYPES = {
    "Taxi ID": object, "Trip Start Timestamp": object,
    "Trip Miles": float, "Trip Total": object, "Trip Seconds": float,
    "Tolls": object, "Fare": object, "Tips": object, "Tolls": object, "Extras": object,
    "Pickup Centroid Latitude": float, "Pickup Centroid Longitude": float,
    "Dropoff Centroid Latitude": float, "Dropoff Centroid Longitude": float,
    "Pickup Centroid Location": object
}


def getwknum(string):
    month, day, year = map(int, [string[:2], string[2:4], string[6:]])
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday // 7


def addWeeks(df):
    df['week'] = df['Trip Start Timestamp'].transform(lambda x: getwknum(x))
    return df


def prepareForCSV(data):
    trip_miles = {}
    for k, v in data.items():
        taxiID = k[0]
        if taxiID not in trip_miles:
            trip_miles[taxiID] = [0] * 53
        wknum = k[1]
        trip_miles[taxiID][wknum] += float(v)
    return pd.DataFrame(
        [[key, *val] for key, val in trip_miles.items()], columns=HEADERS, index=None)


def getSums(filename, column, df, year):
    if DATATYPES[column] == object:
        t0 = time.time()
        df[column] = df[column].map(
            lambda x: x if type(x) == float else float(x[1:]))
        t1 = time.time()
        print(f"map in {t1-t0} sec.")
    t0 = time.time()
    total_count = df.groupby(["Taxi ID", "week"])[
        column].sum().unstack(level=-1)
    t1 = time.time()
    print(f"groupby in {t1-t0} sec.")
    print(f"prep in {t1-t0} sec.")
    return total_count


if __name__ == "__main__":
    year = 2013
    filename = f"original/Chicago_taxi_trips{year}_weeks.csv"
    t0 = time.time()
    readcols = ["Trip Total"]
    # reading csv takes about 2 minutes
    df = pd.read_csv(filename,
                     usecols=readcols +
                     ["Taxi ID", "week"],
                     dtype=DATATYPES)
    t1 = time.time()
    print(f"{filename} read (and weeks added) in {t1-t0} sec.")

    for column in readcols:
        result = getSums(filename, column, df, year)
        result.to_csv(f"{year}_{column}_sums.csv", index=False)
        print(f"{year}_{column}_sums.csv written.")
