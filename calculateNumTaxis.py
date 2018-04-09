import pandas as pd
import time
import datetime as dt


def getwknum(string):
    month, day, year = map(int, [string[:2], string[2:4], string[6:]])
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday // 7


def addWeeks(df):
    df['week'] = df['Trip Start Timestamp'].transform(lambda x: getwknum(x))
    return df


filename = f"Chicago_taxi_trips2014.csv"
df = pd.read_csv(filename,
                 usecols=["Taxi ID", "Trip Start Timestamp"],
                 dtype={"Taxi ID": object, "Trip Start Timestamp": object})
print(f"{filename} read.")
result = addWeeks(df).groupby("week")["Taxi ID"].nunique()

result.to_csv("Chicago_taxi_trips2014_taxiNum.csv")
