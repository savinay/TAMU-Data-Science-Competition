"""Calculate the number of taxis in a time period per week.
ex: week 1 had 56 unique taxis driving."""
import time
import datetime as dt
import pandas as pd
from multiprocessing import Pool
import numpy as np

DATATYPES = {
    "Trip Start Timestamp": object, "Trip ID": object, "Taxi ID": object
}


def parallelize_dataframe(df, func):
    num_partitions = 10
    num_cores = 2
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


def readWrite(year):
    filename = f"original/Chicago_taxi_trips{year}.csv"
    t0 = time.time()
    df = pd.read_csv(filename,
                     usecols=DATATYPES.keys(),
                     dtype=DATATYPES)
    print(f"Read in {round(time.time()-t0)} sec.")

    df = parallelize_dataframe(df, addWeeks)
    print(f"Weeks added in {round(time.time()-t0)} sec.")

    result = df.groupby(["week", "Taxi ID"]).size()
    print(f"Groupby in {round(time.time()-t0)} sec.")

    result.to_csv(f"{year}_numTaxisPerWeek.csv")


if __name__ == "__main__":
    readWrite(2017)
