"""Calculate the number of taxis in a time period per week.
ex: week 1 had 56 unique taxis driving."""
import time
import datetime as dt
import pandas as pd


DATATYPES = {
    "Taxi ID": object, "Trip Start Timestamp": object
}


def getwknum(string):
    month, day, year = map(int, [string[:2], string[2:4], string[6:]])
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday // 7


def readWrite(year):
    filename = f"original/Chicago_taxi_trips{year}.csv"
    t0 = time.time()
    df = pd.read_csv(filename,
                     usecols=DATATYPES.keys(),
                     dtype=DATATYPES,
                     nrows=1000)
    print(f"Read in {round(time.time()-t0)} sec.")
    df["weeks"] = df["Trip Start Timestamp"].transform(getwknum)
    print(f"Weeks added in {round(time.time()-t0)} sec.")
    result = df.groupby("week")["Taxi ID"].nunique()
    print(f"Groupby in {round(time.time()-t0)} sec.")

    result.to_csv("Chicago_taxi_trips2014_taxiNum.csv")


if __name__ == "__main__":
    readWrite(2017)
