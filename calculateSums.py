# pylint: skip-file
from addWeeks import addWeeks
import pandas as pd


def sumsByTaxiID(column, df):
    weeks = range(1, 54)
    trip_miles = {}
    for week in weeks:
        df_week = df[(df['week'] == week)]
        for _, val in df_week[['Taxi ID', column]].iterrows():
            if val[0] not in trip_miles:
                trip_miles[val[0]] = [0] * 53
            else:
                trip_miles[val[0]][week - 1] += val[1]
    # trip_miles # {taxiId1: [sum-week1, ... , sum-week53], ... , taxiIdN: [sum-week1, ... , sum-week53]}
    headers = ['Taxi ID', *[f'week{i}' for i in range(1, 54)]]
    return pd.DataFrame([[key, *val] for key, val in trip_miles.items()], columns=headers)


if __name__ == "__main__":
    df = pd.read_csv('../data/Chicago_taxi_trips2017.csv', nrows=10000)
    df = addWeeks(df)
    result = sumsByTaxiID("Trip Miles", df)
    result.to_csv("out.csv")
