# pylint: skip-file
from addWeeks import addWeeks
import pandas as pd
import os
import subprocess
import time

import sys

filedir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(filedir, os.pardir))
os.chdir(filedir)


def sumsByTaxiID(column, df, trip_miles):
    weeks = range(1, 54)
    for week in weeks:
        df_week = df[(df['week'] == week)]
        for _, val in df_week[['Taxi ID', column]].iterrows():
            if val[0] not in trip_miles:
                trip_miles[val[0]] = [0] * 53
            else:
                trip_miles[val[0]][week - 1] += val[1]
    # trip_miles # {taxiId1: [sum-week1, ... , sum-week53], ... , taxiIdN: [sum-week1, ... , sum-week53]}
    return trip_miles


def readAllRows(filename, step, column):
    test = subprocess.Popen(["wc", "-l", filename], stdout=subprocess.PIPE)
    # output = test.communicate()[0]
    # total = int(str(output).split()[1])
    total = 1000000
    trip_miles = {}
    for rowNum in range(0, total, step):
        t0 = time.time()
        df = pd.read_csv("../data/Chicago_taxi_trips2017.csv",
                         skiprows=range(1, rowNum), nrows=step,
                         usecols=["Taxi ID", "Trip Miles",
                                  "Trip Start Timestamp"],
                         dtype={
                             "Taxi ID": object,
                             "Trip Miles": float,
                             "Trip Start Timestamp": object
                         })
        t1 = time.time()
        print(f"Time for read is {t1 - t0}")

        t0 = time.time()
        df = addWeeks(df)
        t1 = time.time()
        print(f"Time for add week is {t1 - t0}")
        print(f"Up to {rowNum} read.")
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
