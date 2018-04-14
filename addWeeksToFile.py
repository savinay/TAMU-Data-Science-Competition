# pylint: skip-file
import pandas as pd
from collections import Counter
import numpy as np
import datetime as dt
import time


DATATYPES = {
    "Taxi ID": object, "Trip Start Timestamp": object,
    "Trip Miles": float, "Trip Total": object, "Trip Seconds": float,
    "Fare": object, "Tips": object,
    "Pickup Centroid Location": object, "Dropoff Centroid  Location": object
}


def getwknum(string):
    month, day, year = map(int, string.split()[0].split("/"))
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday // 7


def readWrite(year):
    filename = f"original/Chicago_taxi_trips{year}.csv"
    chunksize = 100000
    t0 = time.time()
    for i, chunk in enumerate(pd.read_csv(filename,
                                        usecols=DATATYPES.keys(),
                                        dtype=DATATYPES,
                                        chunksize=chunksize,
                                        iterator=True)):
        print("Chunk read.")
        df["weeks"] = df["Trip Start Timestamp"].transform(getwknum)
        print("Weeks added.")
        if i == 0:
            chunk.to_csv(filename.split(".")[0] + "_weeks.csv", index=False)
        else:
            chunk.to_csv(filename.split(".")[
                        0] + "_weeks.csv", index=False, mode='a', header=None)
        t1 = time.time()
        print(f"{round(((chunksize * (i+1)) / 31100000)*100)}% written in {round(time.time()-t0)}.")
    print(f"Total: {round(time.time()-t0)}")

if __name__ == "__main__":
    readWrite(2017)
