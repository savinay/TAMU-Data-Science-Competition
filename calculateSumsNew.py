# pylint: skip-file
import pandas as pd
from collections import Counter
import numpy as np
import datetime as dt
import time


def getwknum(string):
    month, day, year = map(int, string.split()[0].split("/"))
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday // 7


def addWeeks(df):
    df['week'] = df['Trip Start Timestamp'].transform(lambda x: getwknum(x))
    return df


def getwknum(string):
    month, day, year = map(int, string.split()[0].split("/"))
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday // 7


def addWeeks(df):
    df['week'] = df['Trip Start Timestamp'].transform(lambda x: getwknum(x))
    return df


def convert(data):
    trip_miles = {}
    for k, v in data.items():
        taxiID = k[0]
        if taxiID not in trip_miles:
            trip_miles[taxiID] = [0] * 53
        wknum = k[1]
        trip_miles[taxiID][wknum] += float(v)
    return trip_miles


def getSums(filename, column, dictionary, df, year):
    if dictionary[column] == object:
        df[column] = df[column].map(
            lambda x: x if type(x) == float else float(x[1:]))
    total_count = df.groupby(["Taxi ID", "week"])[column].sum().to_dict()
    headers = ['Taxi ID', *[f'week{i}' for i in range(1, 54)]]
    result = pd.DataFrame(
        [[key, *val] for key, val in convert(total_count).items()], columns=headers, index=None)
    result.to_csv(f"{filename}_{column}_sums.csv", index=False)
    print(f"{year}_{column}_sums.csv written.")


if __name__ == "__main__":
    year = 2017
    filename = f"Chicago_taxi_trips{i}.csv"
    datatypes = {
        "Taxi ID": object, "Trip Start Timestamp": object,
        "Trip Miles": float, "Trip Total": object, "Trip Seconds": float,
        "Tolls": object, "Fare": object, "Tips": object, "Tolls": object, "Extras": object}
    t0 = time.time()
    df = pd.read_csv(filename,
                     usecols=["Taxi ID", "Trip Miles", "Trip Total", "Trip Seconds",
                              "Tolls", "Fare", "Tips", "Tolls", "Extras", "Trip Start Timestamp"],
                     dtype=datatypes)
    t1 = time.time()
    print(f"{filename} read in {t1-t0} sec.")

    for column in ["Trip Total", "Trip Miles", "Trip Seconds", "Tolls", "Fare", "Tips", "Tolls", "Extras"]:
        getSums(filename, column, datatypes, addWeeks(df), year)
