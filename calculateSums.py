# pylint: skip-file
"""Get sums of things like trip totals and trip seconds."""

import datetime as dt
import time

import pandas as pd

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


def getSums(column, df):
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
    return total_count


def readWrite(year):
    datapath = "/Users/josiahcoad/Desktop/Coding/dataScienceComp2017/TAMU-Data-Science-Competition/original/"
    filename = f"{datapath}Chicago_taxi_trips{year}_weeks.csv"
    t0 = time.time()
    readcols = ["Trip Total", "Trip Miles"]
    df = pd.read_csv(filename,
                     usecols=readcols +
                     ["Taxi ID", "week"],
                     dtype=DATATYPES,
                     nrows=10000)
    print(f"{filename} read in {time.time()-t0} sec.")

    medians = pd.DataFrame()
    for column in readcols:
        result = getSums(column, df)
        column = column.replace(" ", "_")
        medians[column] = result.median()
        result.to_csv(f"{column}_{year}_sums.csv", index=False)
        print(f"csv written.")
    medians.to_csv(f"medians_{year}.csv", index_label="week", header=readcols)


if __name__ == "__main__":
    readWrite(2013)
