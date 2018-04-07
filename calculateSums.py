# pylint: skip-file
import pandas as pd
import os
import subprocess
import time
import datetime as dt
import sys
from collections import Counter

filedir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(filedir, os.pardir))
os.chdir(filedir)


def getwknum(string):
    # this could be sped up since split is O(n)
    # if we could work under assumption timestamps are all exact same,
    # then we can split on index
    month, day, year = map(int, string.split()[0].split("/"))
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday // 7


def addWeeks(df):
    df['week'] = df['Trip Start Timestamp'].transform(lambda x: getwknum(x))
    return df


def sumsByTaxiID(column, new_df):
    # trying to speed this up using pandas vectorization
    k = new_df.groupby(["Taxi ID", "week"])["Trip Miles"].sum().to_dict()
    k = Counter(k)
    return k


def convert(data):
    trip_miles = {}
    for k, v in data.items():
        taxiID = k[0]
        if taxiID not in trip_miles:
            trip_miles[taxiID] = [0] * 53
        wknum = k[1]
        trip_miles[taxiID][wknum] += float(v)
    return trip_miles


def readAllRows(filename, chunksize, column, nrows=None):
    if not nrows:
        print(f"Reading total number of lines in {filename}.")
        test = subprocess.Popen(["wc", "-l", filename], stdout=subprocess.PIPE)
        output = test.communicate()[0]
        nrows = int(str(output).split()[1])
        print(f"{nrows} read.")
    total_count = Counter()
    print("Start reading.")
    t0 = time.time()
    starttime = time.time()
    count = 1
    for df in pd.read_csv(filename,
                          usecols=["Taxi ID", "Trip Miles",
                                   "Trip Start Timestamp"],
                          dtype={
                              "Taxi ID": object,
                              "Trip Miles": float,
                              "Trip Start Timestamp": object
                          },
                          chunksize=chunksize,
                          iterator=True,
                          nrows=nrows):
        total_count += sumsByTaxiID(column, addWeeks(df))
        t1 = time.time()
        # print(f"Time for this loop is {t1 - t0} and average {(t1 - start) / count}")
        print(f"Rows: {chunksize * count}")
        count += 1
        t0 = t1
    print(f"Done in total time {(t1 - starttime) / 60} min.")
    headers = ['Taxi ID', *[f'week{i}' for i in range(1, 54)]]
    return pd.DataFrame([[key, *val] for key, val in convert(total_count).items()], columns=headers, index=None)


if __name__ == "__main__":
    result = readAllRows(
        "testdata.csv", 1, "Trip Miles")
    result.to_csv("testout.csv", index=False)
    # get num of taxis: df['Taxi ID'].nunique()
