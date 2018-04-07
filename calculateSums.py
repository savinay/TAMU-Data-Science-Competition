# pylint: skip-file
import pandas as pd
import os
import subprocess
import time
import datetime as dt
import sys
from gitversion import strip_gitcommit
from collections import Counter

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

def sumsByTaxiID_old(column, data, trip_miles):
    for _, row in data.iterrows():
        taxiID = row["Taxi ID"]
        if not trip_miles.get(taxiID):
            trip_miles[taxiID] = [0] * 53
        wknum = getwknum(row["Trip Start Timestamp"])
        trip_miles[taxiID][wknum] += float(row[column])
    # trip_miles # {taxiId1: [sum-week1, ... , sum-week53], ... , taxiIdN: [sum-week1, ... , sum-week53]}
    return trip_miles

def convert(data):
    trip_miles = {}
    for k, v in data.items():
        taxiID = k[0]
        if taxiID not in trip_miles:
            trip_miles[taxiID] = [0] * 53
        wknum = k[1]
        trip_miles[taxiID][wknum] += float(v)
    return trip_miles


def readAllRows(filename, column, chunksize, nrows=None):
    if not nrows:
        if sys.platform == "win32":
            raise Exception("Must pass nrows if on windows.")
        print(f"Reading total number of lines in {filename}.")
        test = subprocess.Popen(["wc", "-l", filename], stdout=subprocess.PIPE)
        output = test.communicate()[0]
        nrows = int(str(output).split()[1])
        print(f"{nrows} rows in file.")

    # total_count = Counter()
    trip_miles = {}
    count = 1
    print("Start reading.")
    t0 = time.time()
    starttime = time.time()

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
        # total_count += sumsByTaxiID(column, addWeeks(df))
        trip_miles = sumsByTaxiID_old(column, df, trip_miles)
        t1 = time.time()
        # print(f"Time for this loop is {t1 - t0} and average {(t1 - start) / count}")
        print(f"Rows processed: {chunksize * count}")
        count += 1
        t0 = t1
    print(f"Done in total time {(t1 - starttime) / 60} min.")
    headers = ['Taxi ID', *[f'week{i}' for i in range(1, 54)]]
    # return pd.DataFrame([[key, *val] for key, val in convert(total_count).items()], columns=headers, index=None)
    return pd.DataFrame([[key, *val] for key, val in trip_miles.items()], columns=headers, index=None)


if __name__ == "__main__":
    # set the cwd to where this file is.
    filedir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, os.path.join(filedir, os.pardir))
    os.chdir(filedir)
    
    result = readAllRows(
        "testdata.csv", "Trip Miles", chunksize=1)
    result.to_csv(f"testout{strip_gitcommit()}.csv", index=False)
    # get num of taxis: df['Taxi ID'].nunique()
