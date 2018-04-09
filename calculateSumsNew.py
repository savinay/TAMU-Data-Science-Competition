# pylint: skip-file
import pandas as pd
from collections import Counter
import numpy as np
import datetime as dt
import time


def convert(data):
    trip_miles = {}
    for k, v in data.items():
        taxiID = k[0]
        if taxiID not in trip_miles:
            trip_miles[taxiID] = [0] * 53
        wknum = k[1]
        trip_miles[taxiID][wknum] += float(v)
    return trip_miles


def getSums(filename, column, dictionary):
    t0 = time.time()
    df = pd.read_csv(filename,
                     usecols=["Taxi ID", column, "Trip Start Timestamp"],
                     dtype={
                         "Taxi ID": object,
                         column: dictionary[column],
                         "Trip Start Timestamp": object
                     })
    t1 = time.time()
    print(f"{filename} done in {t1-t0} sec.")
    total_count = df.groupby(["Taxi ID", "week"])[column].sum().to_dict()
    headers = ['Taxi ID', *[f'week{i}' for i in range(1, 54)]]
    result = pd.DataFrame(
        [[key, *val] for key, val in convert(total_count).items()], columns=headers, index=None)
    result.to_csv(f"{filename}_{column}_sums.csv", index=False)
    print(f"{filename}_{column}_sums.csv written.")

if __name__ == "__main__":
    for i in range(4, 5):
        filename = f"Chicago_taxi_trips201{i}.csv"
        dictionary = {"Trip Total": object, "Trip Seconds": float, "Tolls": object, "Fare": object, "Tips": object, "Tolls": object, "Extras": object}
        for column in ["Trip Total", "Trip Seconds", "Tolls", "Fare", "Tips", "Tolls", "Extras"]:
            getSums(filename, column, dictionary)