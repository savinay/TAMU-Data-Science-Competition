import time
import pandas as pd
import datetime as dt

DATATYPES = {
    "Taxi ID": object, "Trip Start Timestamp": object, "Pickup Centroid Location": object
}

def getwknum(df, idx, col):
    string = df[col].loc[idx]
    month, day, year = map(int, [string[:2], string[3:5], string[6:10]])
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday // 7


def readWrite(year):
    filename = f"original/Chicago_taxi_trips{year}.csv"

    t0 = time.time()
    df = pd.read_csv(filename,
                     usecols=["Taxi ID", "Trip Start Timestamp"],
                     dtype=DATATYPES, nrows=1000000).dropna(axis=0, how="any")
    print(f"{filename} read in {round(time.time()-t0)} sec.")

    groups = df.groupby(["Taxi ID", lambda idx: getwknum(
        df, idx, "Trip Start Timestamp")])
    trips = groups.size().unstack(level=-1)
    trips.to_csv(f"{year}_tripcounts.csv", index=False)
    print(f"{year}_tripcounts.csv written in {round(time.time()-t0)} sec.")


if __name__ == "__main__":
    readWrite(2017)
