# pylint: skip-file
import pandas as pd
import time
import datetime as dt

def getwknum(string):
    month, day, year = map(int, string.split()[0].split("/"))
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday // 7


def addWeeks(df):
    df['week'] = df['Trip Start Timestamp'].transform(lambda x: getwknum(x))
    return df


def sumsByTaxiID(column, new_df):
    return new_df.groupby(["Taxi ID", "week"])[column].sum()


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
    print("Start.")
    df = pd.read_csv("Chicago_taxi_trips2015.csv",
                     usecols=["Taxi ID", column, "Trip Start Timestamp"],
                     dtype={
                         "Taxi ID": object,
                         column: object,
                         "Trip Start Timestamp": object
                     })
    t1 = time.time()
    # print(type(df[column]))
    df[column] = df[column].map(lambda x: x if type(x) == float else float(x[1:]))
    print(f"{filename} done in {t1-t0} sec.")
    if dictionary[column] == object:
        df[column] = df[column].map(lambda x: x if type(x) == float else float(x[1:]))
    total_count = df.groupby(["Taxi ID", "week"])[column].sum().to_dict()
    headers = ['Taxi ID', *[f'week{i}' for i in range(1, 54)]]
    result = pd.DataFrame(
        [[key, *val] for key, val in convert(total_count).items()], columns=headers, index=None)
    result.to_csv(f"{filename}_{column}_sums.csv", index=False)
    print(f"{filename}_{column}_sums.csv written.")

if __name__ == "__main__":
    filename = "Chicago_taxi_trips2015.csv"
    getSums(filename, "Trip Total")
