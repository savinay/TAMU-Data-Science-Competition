# pylint: skip-file
from addWeeks import addWeeks
import pandas as pd
import os
import subprocess


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
    total = 1000
    trip_miles = {}
    for rowNum in range(0, total, step):
        df = pd.read_csv(filename, skiprows=rowNum, nrows=step)
        df = addWeeks(df)
        trip_miles = sumsByTaxiID(column, df, trip_miles)
    headers = ['Taxi ID', *[f'week{i}' for i in range(1, 54)]]
    return pd.DataFrame([[key, *val] for key, val in trip_miles.items()], columns=headers, index=False)


if __name__ == "__main__":

    result = readAllRows(
        "../data/Chicago_taxi_trips2017.csv", 100, "Trip Miles")
    result.to_csv("out.csv")
