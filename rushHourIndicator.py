import pandas as pd
from collections import Counter
import numpy as np
import datetime as dt
import time

HEADERS = ['Taxi ID', *[f'week{i}' for i in range(1, 54)]]
DATATYPES = {
    "Taxi ID": object, "Trip Start Timestamp": object,
    "Trip Miles": float, "Trip Total": object, "Trip Seconds": float,
    "Tolls": object, "Fare": object, "Tips": object, "Tolls": object, "Extras": object,
    "Pickup Centroid Latitude": float, "Pickup Centroid Longitude": float,
    "Dropoff Centroid Latitude": float, "Dropoff Centroid Longitude": float
}

def checkIfRushHour(timestamp):
    hour = int(timestamp.split()[1].split(":")[0])
    isAM = False
    isAM = timestamp.split()[2] == "AM"
    if not isAM:
        hour += 12
    if hour >=6 and hour <= 10:
        return 1
    if hour >=15 and hour <= 19:
        return 1
    return 0

def prepareForCsv(res, final_res):
    final_res = {}
    for key in res:
        if key[0] not in final_res:
            final_res[key[0]] = [0] * 53
        final_res[key[0]][key[1]] = res[key]
    return pd.DataFrame([[key, *val] for key, val in final_res.items()], columns=HEADERS, index=None)

def rushHourindicator():
    t4 = time.time()
    df = pd.read_csv("Chicago_taxi_trips2017.csv", usecols=["Taxi ID", "Trip Start Timestamp", "week"], dtype={"Taxi ID": object, "Trip Start Timestamp": object, "week": int})
    print("read csv in " + str(time.time() - t4))
    t0 = time.time()
    # df["hour"] = pd.to_datetime(df["Trip Start Timestamp"]).dt.hour
    # print("Finished adding hour in : " + str(time.time() - t0))
    t1 = time.time()
    df["rush_hour"] = df["Trip Start Timestamp"].transform(lambda x: checkIfRushHour(x))
    print("Finished adding rush hour in : " + str(time.time() - t1))
    t2 = time.time()
    group_rush_hour = df.groupby(["Taxi ID", "week"])["rush_hour"].sum().to_dict()
    group_count = df.groupby(["Taxi ID", "week"]).agg(['count']).to_dict()
    print("Finished grouping  in : " + str(time.time() - t2))
    res = {}
    t3 = time.time()
    for key in group_count[('Trip Start Timestamp','count')]:
        if key not in group_rush_hour:
            continue
        else:
            res[key] = group_rush_hour[key]/group_count[('Trip Start Timestamp','count')][key]
    
    final_res = {}
    final_res = prepareForCsv(res, final_res)
    final_res.to_csv("rush_hour_indicator_2017.csv", index=False)
    print("added to csv in " + str(time.time() - t3))
    print("Total time: " + str(time.time() - t4))

if __name__ == "__main__":
    rushHourindicator()
    
