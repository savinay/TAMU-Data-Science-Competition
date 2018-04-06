# pylint: skip-file
from addWeeks import addWeeks
import pandas as pd
import os
import subprocess
import time
import datetime as dt
import sys

filedir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(filedir, os.pardir))
os.chdir(filedir)


def getwknum(string):
    month, day, year = map(int, string.split()[0].split("/"))
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday // 7


def sumsByTaxiID(column, data, trip_miles):
    for _, row in data.iterrows():
        taxiID = row["Taxi ID"]
        if not trip_miles.get(taxiID):
            trip_miles[taxiID] = [0] * 53
        wknum = getwknum(row["Trip Start Timestamp"])
        trip_miles[taxiID][wknum] += float(row[column])
    # trip_miles # {taxiId1: [sum-week1, ... , sum-week53], ... , taxiIdN: [sum-week1, ... , sum-week53]}
    return trip_miles


def readAllRows(filename, chunksize, column):
    test = subprocess.Popen(["wc", "-l", filename], stdout=subprocess.PIPE)
    output = test.communicate()[0]
    total = int(str(output).split()[1])
    # total = 1000000
    trip_miles = {}
    # t0 = time.time()
    # t1 = time.time()
    # print(f"Time for read is {t1 - t0}")
    print("Start.")
    for df in pd.read_csv("../data/Chicago_taxi_trips2017.csv",
                         usecols=["Taxi ID", "Trip Miles",
                                  "Trip Start Timestamp"],
                         dtype={
                             "Taxi ID": object,
                             "Trip Miles": float,
                             "Trip Start Timestamp": object
                         },
                         chunksize=chunksize,
                         iterator=True):
        print(f"read.")
        t0 = time.time()
        trip_miles = sumsByTaxiID(column, df, trip_miles)
        t1 = time.time()
        print(f"Time for sumsByTaxiID is {t1 - t0}")
    headers = ['Taxi ID', *[f'week{i}' for i in range(1, 54)]]
    return pd.DataFrame([[key, *val] for key, val in trip_miles.items()], columns=headers, index=None)


if __name__ == "__main__":
    result = readAllRows(
        "../data/Chicago_taxi_trips2017.csv", 10000, "Trip Miles")
    result.to_csv("out.csv", index=False)
