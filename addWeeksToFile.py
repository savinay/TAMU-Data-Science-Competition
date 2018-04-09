# pylint: skip-file
import pandas as pd
from collections import Counter
import numpy as np
import datetime as dt
import time


DATATYPES = {
    "Taxi ID": object, "Trip Start Timestamp": object,
    "Trip Miles": float, "Trip Total": object, "Trip Seconds": float,
    "Tolls": object, "Fare": object, "Tips": object, "Tolls": object, "Extras": object,
    "Pickup Centroid Latitude": float, "Pickup Centroid Longitude": float,
    "Dropoff Centroid Latitude": float, "Dropoff Centroid Longitude": float
}


def getwknum(string):
    month, day, year = map(int, [string[:2], string[2:4], string[6:]])
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday // 7


def addWeeks(df):
    df['week'] = df['Trip Start Timestamp'].transform(lambda x: getwknum(x))
    return df


filename = "original/Chicago_taxi_trips2014.csv"
df = pd.read_csv(filename,
                 usecols=DATATYPES.keys(),
                 dtype=DATATYPES)
df = addWeeks(df)
df.to_csv(filename, index=False)
