# pylint: skip-file
import pandas as pd


def addWeeks(df):
	df['datetime_values'] = df['Trip Start Timestamp'].map(
		lambda x: pd.to_datetime(x))
	df['week'] = df['datetime_values'].dt.week
    return df


if __name__ == "__main__":
    df = pd.read_csv('Chicago_taxi_trips2017.csv', nrows=1000000)
    df = addWeeks(df)
