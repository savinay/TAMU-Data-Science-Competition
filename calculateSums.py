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


def readAllRows(filename, chunksize, column):
    # test = subprocess.Popen(["wc", "-l", filename], stdout=subprocess.PIPE)
    # output = test.communicate()[0]
    # total = int(str(output).split()[1])
    total = 100000
    total_count = Counter()
    print("Start.")
    t0 = time.time()
    start = time.time()
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
                          nrows=total):
        total_count += sumsByTaxiID(column, addWeeks(df))
        t1 = time.time()
        # print(f"Time for this loop is {t1 - t0} and average {(t1 - start) / count}")
        print(f"Rows: {chunksize * count}")
        count += 1
        t0 = t1
    print(f"Done in total time {t1 - start}")
    headers = ['Taxi ID', *[f'week{i}' for i in range(1, 54)]]
    # return pd.DataFrame([[key, *val] for key, val in total_df.items()], columns=headers, index=None)
    return total_count


if __name__ == "__main__":
    result = readAllRows(
        "Chicago_taxi_trips2017.csv", 10000, "Trip Miles")
    # print(result)
    # result.to_csv("out.csv", index=False)
    # get num of taxis: cars['Manufacturer'].nunique()
