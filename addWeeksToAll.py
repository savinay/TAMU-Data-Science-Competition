import pandas as pd
from collections import Counter
import numpy as np
import datetime as dt
import time

def getwknum(string):
    # this could be sped up since split is O(n)
    # if we could work under assumption timestamps are all exact same,
    # then we can split on index
    month, day, year = map(int, string.split()[0].split("/"))
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday // 7


def addWeeks(df):
    df['week'] = df['Trip Start Timestamp'].transform(lambda x: getwknum(x))
    return df

for i in range(3, 8):
    filename = f"Chicago_taxi_trips201{i}.csv"
    df = pd.read_csv(filename)
    df = addWeeks(df)
    df.to_csv(filename, index=False)