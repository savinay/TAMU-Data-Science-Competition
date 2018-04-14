import pandas as pd
from collections import Counter
import numpy as np
import datetime as dt
from multiprocessing import Pool
import time

HEADERS = ['Taxi ID', *[f'week{i}' for i in range(1, 54)]]
DATATYPES = {
    "Taxi ID": object, "Trip Start Timestamp": object,
    "Trip Miles": float, "Trip Total": object, "Trip Seconds": float,
    "Tolls": object, "Fare": object, "Tips": object, "Tolls": object, "Extras": object,
    "Pickup Centroid Latitude": float, "Pickup Centroid Longitude": float,
    "Dropoff Centroid Latitude": float, "Dropoff Centroid Longitude": float
}

def getwknum(string):
    month, day, year = map(int, [string[:2], string[3:5], string[6:10]])
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday // 7

def addWeeks(df):
    df["week"] = df["Trip Start Timestamp"].map(getwknum)
    return df

def parallelize_dataframe(df, func):
    num_partitions = 10
    num_cores = 2
    df_split = np.array_split(df, num_partitions)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

def prepareForCsv(res, final_res):
    final_res = {}
    for key in res:
        if key[0] not in final_res:
            final_res[key[0]] = [0] * 53
        final_res[key[0]][key[1]] = res[key]
    return pd.DataFrame([[key, *val] for key, val in final_res.items()], columns=HEADERS, index=None)

def rushHourindicator():
    t0 = time.time()
    df = pd.read_csv("Chicago_taxi_trips2016.csv", usecols=["Taxi ID", "Trip Start Timestamp"], dtype={"Taxi ID": object, "Trip Start Timestamp": object, "week": int})
    print("read csv in " + str(time.time() - t0))
    df = parallelize_dataframe(df, addWeeks)
    t1 = time.time()
    group_count = df.groupby(["Taxi ID", "week"]).agg(['count']).to_dict()
    print("Finished grouping  in : " + str(time.time() - t1))
    res = {}
    t2 = time.time()
    for key in group_count[('Trip Start Timestamp','count')]:
        res[key] = group_count[('Trip Start Timestamp','count')][key]
    
    final_res = {}
    final_res = prepareForCsv(res, final_res)
    final_res.to_csv("number_trips_2017.csv", index=False)
    print("added to csv in " + str(time.time() - t2))
    print("Total time: " + str(time.time() - t0))

if __name__ == "__main__":
    rushHourindicator()
    
