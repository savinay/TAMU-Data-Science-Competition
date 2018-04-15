# pylint: disable=missing-docstring, invalid-name
"""Get sums of things like trip totals and trip seconds."""

import datetime as dt
import time
from multiprocessing import Pool
import numpy as np
import pandas as pd


HEADERS = ['Taxi ID', *[f'week{i}' for i in range(1, 54)]]
DATATYPES = {
    "Taxi ID": object, "Trip Start Timestamp": object,
    "Trip Miles": float, "Trip Total": object, "Trip Seconds": float,
    "Tolls": object, "Fare": object, "Tips": object, "Extras": object
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


def getdaynum(string):
    month, day, year = map(int, [string[:2], string[3:5], string[6:10]])
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday


def addDays(df):
    df["day"] = df["Trip Start Timestamp"].map(getdaynum)
    return df


def getSums(column, df):
    if DATATYPES[column] == object:
        t0 = time.time()
        df[column] = df[column].map(
            lambda x: x if type(x) == float else float(x[1:]))
        t1 = time.time()
        print(f"map in {t1-t0} sec.")
    t0 = time.time()
    total_count = df.groupby(["Taxi ID", "day"])[
        column].sum().unstack(level=-1)
    t1 = time.time()
    print(f"groupby in {t1-t0} sec.")
    return total_count


def readWrite():
    year = 2013
    # basepath = "/Users/josiahcoad/Desktop/Coding/dataScienceComp2017/TAMU-Data-Science-Competition"
    basepath = "."
    datapath = f"{basepath}/original"
    outsumspath = f"{basepath}/daily/{year}/sums"
    outmedianpath = f"{basepath}/daily/medians"

    filename = f"{datapath}/Chicago_taxi_trips{year}.csv"
    t0 = time.time()
    readcols = ["Trip Total", "Trip Miles",
                "Trip Seconds", "Fare", "Tolls", "Extras", "Tips"]
    df = pd.read_csv(filename,
                     usecols=readcols +
                     ["Taxi ID", "Trip Start Timestamp"],
                     dtype=DATATYPES,
                     nrows=10000)
    print(f"{filename} read in {round(time.time()-t0)} sec.")

    df = parallelize_dataframe(df, addDays)
    print(f"Days added in {round(time.time()-t0)} sec.")

    medians = pd.DataFrame()
    for column in readcols:
        result = getSums(column, df)
        column = column.replace(" ", "_")
        medians[column] = result.median()
        result.to_csv(f"{column}_{year}_sums.csv", index=False)
        print(f"{outsumspath}/{column}_{year}_sums.csv in {round(time.time()-t0)}")
    medians.to_csv(f"{outmedianpath}/medians_{year}.csv", index_label="day", header=readcols)
    print(f"Done in {round(time.time()-t0)}")


if __name__ == "__main__":
    readWrite()
